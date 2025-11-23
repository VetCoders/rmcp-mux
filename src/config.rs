use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub servers: HashMap<String, ServerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub socket: Option<String>,
    pub cmd: Option<String>,
    pub args: Option<Vec<String>>,
    pub max_active_clients: Option<usize>,
    pub tray: Option<bool>,
    pub service_name: Option<String>,
    pub log_level: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ResolvedParams {
    pub socket: PathBuf,
    pub cmd: String,
    pub args: Vec<String>,
    pub max_clients: usize,
    pub tray_enabled: bool,
    pub log_level: String,
    pub service_name: String,
}

pub fn expand_path(raw: impl AsRef<str>) -> PathBuf {
    let s = raw.as_ref();
    if let Some(stripped) = s.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(stripped);
        }
    }
    PathBuf::from(s)
}

pub fn load_config(path: &Path) -> Result<Option<Config>> {
    if !path.exists() {
        return Ok(None);
    }
    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read config at {}", path.display()))?;

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let cfg: Config = match ext.as_str() {
        "yaml" | "yml" => serde_yaml::from_str(&data)
            .with_context(|| format!("failed to parse yaml config {}", path.display()))?,
        "toml" => toml::from_str(&data)
            .with_context(|| format!("failed to parse toml config {}", path.display()))?,
        _ => serde_json::from_str(&data)
            .with_context(|| format!("failed to parse json config {}", path.display()))?,
    };
    Ok(Some(cfg))
}

pub fn resolve_params(cli: &crate::Cli, config: Option<&Config>) -> Result<ResolvedParams> {
    let service_cfg = if let Some(cfg) = config {
        if let Some(name) = &cli.service {
            let found = cfg
                .servers
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow!("service '{name}' not found in config"))?;
            Some((name.clone(), found))
        } else {
            None
        }
    } else {
        None
    };

    if config.is_some() && cli.service.is_none() {
        return Err(anyhow!("--service is required when using --config"));
    }

    let socket = cli
        .socket
        .clone()
        .or_else(|| {
            service_cfg
                .as_ref()
                .and_then(|(_, c)| c.socket.clone().map(expand_path))
        })
        .ok_or_else(|| anyhow!("socket path not provided (use --socket or config)"))?;

    let cmd = cli
        .cmd
        .clone()
        .or_else(|| service_cfg.as_ref().and_then(|(_, c)| c.cmd.clone()))
        .ok_or_else(|| anyhow!("cmd not provided (use --cmd or config)"))?;

    let args = if !cli.args.is_empty() {
        cli.args.clone()
    } else {
        service_cfg
            .as_ref()
            .and_then(|(_, c)| c.args.clone())
            .unwrap_or_default()
    };

    let max_clients = service_cfg
        .as_ref()
        .and_then(|(_, c)| c.max_active_clients)
        .unwrap_or(cli.max_active_clients);

    let tray_enabled = if cli.tray {
        true
    } else {
        service_cfg
            .as_ref()
            .and_then(|(_, c)| c.tray)
            .unwrap_or(false)
    };

    let log_level = service_cfg
        .as_ref()
        .and_then(|(_, c)| c.log_level.clone())
        .unwrap_or_else(|| cli.log_level.clone());

    let service_name_raw = cli
        .service_name
        .clone()
        .or_else(|| {
            service_cfg
                .as_ref()
                .and_then(|(_, c)| c.service_name.clone())
        })
        .or_else(|| {
            socket
                .file_name()
                .and_then(|n| n.to_string_lossy().split('.').next().map(|s| s.to_string()))
        })
        .unwrap_or_else(|| "mcp_mux".to_string());

    Ok(ResolvedParams {
        socket,
        cmd,
        args,
        max_clients,
        tray_enabled,
        log_level,
        service_name: service_name_raw,
    })
}
