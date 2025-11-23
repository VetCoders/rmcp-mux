use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::UnixStream;

/// Lightweight STDIO↔Unix-socket proxy for mcp_mux.
#[derive(Parser, Debug)]
#[command(author, version, about = "Proxy STDIO to an mcp_mux socket")]
struct ProxyCli {
    /// Path to the Unix socket exposed by mcp_mux (e.g. ~/mcp-sockets/memory.sock).
    #[arg(long)]
    socket: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = ProxyCli::parse();
    let stream = UnixStream::connect(&cli.socket).await?;
    let (mut mux_reader, mut mux_writer) = stream.into_split();

    let mut stdin = io::stdin();
    let mut stdout = io::stdout();

    let to_mux = async {
        io::copy(&mut stdin, &mut mux_writer).await?;
        mux_writer.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    };

    let from_mux = async {
        io::copy(&mut mux_reader, &mut stdout).await?;
        stdout.flush().await?;
        Ok::<(), anyhow::Error>(())
    };

    let _ = tokio::try_join!(to_mux, from_mux)?;
    Ok(())
}
