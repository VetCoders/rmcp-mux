use std::io::{stdout, IsTerminal};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Args;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::{Frame, Terminal};

use crate::config::{expand_path, load_config, Config, ServerConfig};

#[derive(Debug, Clone, Args)]
pub struct WizardArgs {
    /// Path to mux config (json/yaml/toml). Default: ~/.codex/mcp-mux.toml
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Service key to edit or create.
    #[arg(long)]
    pub service: Option<String>,
    /// Socket path override.
    #[arg(long)]
    pub socket: Option<PathBuf>,
    /// Command override (e.g. npx).
    #[arg(long)]
    pub cmd: Option<String>,
    /// Args override (space separated).
    #[arg(long)]
    pub args: Vec<String>,
    /// Max clients override.
    #[arg(long)]
    pub max_clients: Option<usize>,
    /// Log level override.
    #[arg(long)]
    pub log_level: Option<String>,
    /// Tray override.
    #[arg(long)]
    pub tray: Option<bool>,
    /// Do not write files; just preview.
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    ConfigPath,
    ServiceName,
    Socket,
    Cmd,
    Args,
    MaxClients,
    LogLevel,
    Tray,
}

#[derive(Debug, Clone)]
struct FormState {
    config_path: String,
    service_name: String,
    socket: String,
    cmd: String,
    args: String,
    max_clients: String,
    log_level: String,
    tray: bool,
    message: String,
    dirty: bool,
}

pub async fn run_wizard(args: WizardArgs) -> Result<()> {
    if !stdout().is_terminal() {
        return Err(anyhow!(
            "wizard requires an interactive TTY; run with --config/--service in non-interactive mode"
        ));
    }

    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| expand_path("~/.codex/mcp-mux.toml"));
    let (initial_config, initial_service) = load_existing(&config_path, args.service.clone())?;

    let form = FormState {
        config_path: config_path.display().to_string(),
        service_name: args
            .service
            .or(initial_service)
            .unwrap_or_else(|| "general-memory".into()),
        socket: args
            .socket
            .map(|p| p.display().to_string())
            .or_else(|| initial_config.socket.clone())
            .unwrap_or_else(|| "~/mcp-sockets/general-memory.sock".into()),
        cmd: args
            .cmd
            .or(initial_config.cmd)
            .unwrap_or_else(|| "npx".into()),
        args: if !args.args.is_empty() {
            args.args.join(" ")
        } else {
            initial_config
                .args
                .unwrap_or_else(|| vec!["@modelcontextprotocol/server-memory".into()])
                .join(" ")
        },
        max_clients: args
            .max_clients
            .or(initial_config.max_active_clients)
            .unwrap_or(5)
            .to_string(),
        log_level: args
            .log_level
            .or(initial_config.log_level)
            .unwrap_or_else(|| "info".into()),
        tray: args.tray.or(initial_config.tray).unwrap_or(false),
        message: "Use ↑/↓ to move, Enter to edit, space to toggle tray, s=save, q=quit".into(),
        dirty: false,
    };

    let mut app = AppState {
        form,
        config_from_disk: load_config(&PathBuf::from(&config_path)).unwrap_or(None),
        current: Field::ServiceName,
        editing: None,
        saved: false,
        dry_run: args.dry_run,
    };

    run_tui(&mut app)?;

    if app.saved && !app.dry_run {
        persist(&app)?;
    }

    Ok(())
}

fn load_existing(
    path: &Path,
    service_override: Option<String>,
) -> Result<(ServerConfig, Option<String>)> {
    let cfg = load_config(path)?;
    let default_server = ServerConfig {
        socket: None,
        cmd: None,
        args: None,
        max_active_clients: None,
        tray: None,
        service_name: None,
        log_level: None,
        lazy_start: None,
        max_request_bytes: None,
        request_timeout_ms: None,
        restart_backoff_ms: None,
        restart_backoff_max_ms: None,
        max_restarts: None,
        status_file: None,
    };
    if let Some(cfg) = cfg {
        if let Some(name) = service_override {
            let entry = cfg.servers.get(&name).cloned().unwrap_or(default_server);
            Ok((entry, Some(name)))
        } else if let Some((name, entry)) = cfg.servers.iter().next() {
            Ok((entry.clone(), Some(name.clone())))
        } else {
            Ok((default_server, None))
        }
    } else {
        Ok((default_server, None))
    }
}

struct AppState {
    form: FormState,
    config_from_disk: Option<Config>,
    current: Field,
    editing: Option<Field>,
    saved: bool,
    dry_run: bool,
}

fn run_tui(app: &mut AppState) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.hide_cursor()?;

    loop {
        terminal.draw(|f| draw_ui(f, app))?;

        if !event::poll(Duration::from_millis(200))? {
            continue;
        }
        let evt = event::read()?;
        if let Event::Key(key) = evt {
            if key.kind == KeyEventKind::Release {
                continue;
            }
            if handle_key(app, key)? {
                break;
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn draw_ui(f: &mut Frame, app: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(
            [
                Constraint::Length(3),
                Constraint::Min(6),
                Constraint::Length(3),
            ]
            .as_ref(),
        )
        .split(f.area());

    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            "rmcp_mux wizard",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(" — interactive config builder"),
    ]));
    f.render_widget(title, chunks[0]);

    let fields = vec![
        (Field::ConfigPath, "Config path", &app.form.config_path),
        (Field::ServiceName, "Service name", &app.form.service_name),
        (Field::Socket, "Socket", &app.form.socket),
        (Field::Cmd, "Command", &app.form.cmd),
        (Field::Args, "Args", &app.form.args),
        (Field::MaxClients, "Max clients", &app.form.max_clients),
        (Field::LogLevel, "Log level", &app.form.log_level),
    ];

    let mut lines: Vec<Line> = fields
        .into_iter()
        .map(|(field, label, value)| {
            let mut spans = vec![Span::styled(
                format!("{label:<14}"),
                Style::default().fg(Color::Cyan),
            )];
            let val_style = if Some(field) == app.editing {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else if field == app.current {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };
            spans.push(Span::styled(value.clone(), val_style));
            Line::from(spans)
        })
        .collect();

    let tray_label = if app.form.tray { "true" } else { "false" };
    let tray_style = if Some(Field::Tray) == app.editing {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else if app.current == Field::Tray {
        Style::default().fg(Color::Green)
    } else {
        Style::default()
    };
    lines.push(Line::from(vec![
        Span::styled("Tray enabled   ", Style::default().fg(Color::Cyan)),
        Span::styled(tray_label, tray_style),
    ]));

    let body = Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("Fields"));
    f.render_widget(body, chunks[1]);

    let mut footer = vec![Span::raw(&app.form.message)];
    if app.dry_run {
        footer.push(Span::raw(" | dry-run: no files will be written"));
    }
    if app.saved {
        footer.push(Span::raw(" | saved"));
    }
    let status = Paragraph::new(Line::from(footer))
        .wrap(Wrap { trim: true })
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(status, chunks[2]);
}

fn handle_key(app: &mut AppState, key: crossterm::event::KeyEvent) -> Result<bool> {
    let ctrl_s = key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('s');
    match key.code {
        KeyCode::Char('q') if app.editing.is_none() => return Ok(true),
        KeyCode::Char('s') | KeyCode::Enter if ctrl_s => {
            try_save(app)?;
            return Ok(app.saved);
        }
        KeyCode::Up => {
            app.current = previous_field(app.current);
        }
        KeyCode::Down => {
            app.current = next_field(app.current);
        }
        KeyCode::Enter => {
            if app.current == Field::Tray {
                app.form.tray = !app.form.tray;
                app.form.dirty = true;
            } else {
                app.editing = Some(app.current);
            }
        }
        KeyCode::Char(' ') => {
            if app.current == Field::Tray {
                app.form.tray = !app.form.tray;
                app.form.dirty = true;
            }
        }
        KeyCode::Esc => {
            app.editing = None;
        }
        KeyCode::Backspace => {
            if let Some(field) = app.editing {
                mutate_field(&mut app.form, field, |s| {
                    s.pop();
                });
            }
        }
        KeyCode::Char(c) => {
            if let Some(field) = app.editing {
                mutate_field(&mut app.form, field, |s| s.push(c));
            }
        }
        _ => {}
    }
    Ok(false)
}

fn mutate_field<F: FnOnce(&mut String)>(form: &mut FormState, field: Field, f: F) {
    let target = match field {
        Field::ConfigPath => &mut form.config_path,
        Field::ServiceName => &mut form.service_name,
        Field::Socket => &mut form.socket,
        Field::Cmd => &mut form.cmd,
        Field::Args => &mut form.args,
        Field::MaxClients => &mut form.max_clients,
        Field::LogLevel => &mut form.log_level,
        Field::Tray => return,
    };
    f(target);
    form.dirty = true;
}

fn previous_field(current: Field) -> Field {
    match current {
        Field::ConfigPath => Field::Tray,
        Field::ServiceName => Field::ConfigPath,
        Field::Socket => Field::ServiceName,
        Field::Cmd => Field::Socket,
        Field::Args => Field::Cmd,
        Field::MaxClients => Field::Args,
        Field::LogLevel => Field::MaxClients,
        Field::Tray => Field::LogLevel,
    }
}

fn next_field(current: Field) -> Field {
    match current {
        Field::ConfigPath => Field::ServiceName,
        Field::ServiceName => Field::Socket,
        Field::Socket => Field::Cmd,
        Field::Cmd => Field::Args,
        Field::Args => Field::MaxClients,
        Field::MaxClients => Field::LogLevel,
        Field::LogLevel => Field::Tray,
        Field::Tray => Field::ConfigPath,
    }
}

fn try_save(app: &mut AppState) -> Result<()> {
    if app.editing.is_some() {
        app.form.message = "Finish editing field before saving (Esc to exit edit mode)".into();
        return Ok(());
    }
    let max_clients: usize = app
        .form
        .max_clients
        .trim()
        .parse()
        .map_err(|_| anyhow!("max clients must be a number"))?;
    if max_clients == 0 {
        return Err(anyhow!("max clients must be at least 1"));
    }

    if app.form.service_name.trim().is_empty() {
        return Err(anyhow!("service name cannot be empty"));
    }
    if app.form.socket.trim().is_empty() {
        return Err(anyhow!("socket path cannot be empty"));
    }
    if app.form.cmd.trim().is_empty() {
        return Err(anyhow!("command cannot be empty"));
    }

    app.saved = true;
    app.form.message =
        "Validated. Press q to quit or keep editing; saved will persist on exit.".into();
    Ok(())
}

fn persist(app: &AppState) -> Result<()> {
    let path = PathBuf::from(app.form.config_path.clone());
    let expanded_path = expand_path(path.to_string_lossy());
    if let Some(parent) = expanded_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut cfg = app.config_from_disk.clone().unwrap_or_default();
    let mut servers = cfg.servers;
    let args_vec = app
        .form
        .args
        .split_whitespace()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    servers.insert(
        app.form.service_name.clone(),
        ServerConfig {
            socket: Some(app.form.socket.clone()),
            cmd: Some(app.form.cmd.clone()),
            args: Some(args_vec),
            max_active_clients: Some(
                app.form
                    .max_clients
                    .trim()
                    .parse()
                    .map_err(|_| anyhow!("invalid max_clients value"))?,
            ),
            tray: Some(app.form.tray),
            service_name: Some(app.form.service_name.clone()),
            log_level: Some(app.form.log_level.clone()),
            lazy_start: Some(false),
            max_request_bytes: Some(1_048_576),
            request_timeout_ms: Some(30_000),
            restart_backoff_ms: Some(1_000),
            restart_backoff_max_ms: Some(30_000),
            max_restarts: Some(5),
            status_file: None,
        },
    );
    cfg.servers = servers;

    let serialized = match expanded_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "json" => serde_json::to_string_pretty(&cfg)?,
        "yaml" | "yml" => serde_yaml::to_string(&cfg)?,
        _ => toml::to_string_pretty(&cfg)?,
    };

    let backup_path = expanded_path.with_extension("bak");
    if expanded_path.exists() {
        std::fs::copy(&expanded_path, &backup_path)
            .with_context(|| format!("failed to create backup at {}", backup_path.display()))?;
    }

    std::fs::write(&expanded_path, serialized)
        .with_context(|| format!("failed to write {}", expanded_path.display()))?;
    Ok(())
}
