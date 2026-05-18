use clap::Parser;
use file_pipe::ServerConfig;
use std::path::PathBuf;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(about = "HTTP pipe service — stream uploads to downloads in real-time")]
struct Args {
    /// Address to listen on
    #[arg(short, long, default_value = "0.0.0.0:3000", env = "LISTEN")]
    listen: String,

    /// Directory for temporary files
    #[arg(short, long, default_value_t = std::env::temp_dir().display().to_string(), env = "DATA_DIR")]
    data_dir: String,

    /// Maximum disk usage (e.g. "1G", "500M", "1024K")
    #[arg(short = 'D', long, value_parser = parse_size, env = "MAX_DISK")]
    max_disk: Option<u64>,

    /// Maximum bytes for a single pipe (e.g. "100M", "1G")
    #[arg(short = 'P', long, value_parser = parse_size, env = "MAX_PIPE_SIZE")]
    max_pipe_size: Option<u64>,

    /// Seconds to keep an entry after PUT completes (default: 30)
    #[arg(long, default_value_t = 30, env = "PUT_TTL")]
    put_ttl: u64,

    /// Seconds to keep an entry after first GET completes (default: 5)
    #[arg(long, default_value_t = 5, env = "GET_TTL")]
    get_ttl: u64,

    /// Seconds a GET waits for a missing key before returning 404 (default: 5)
    #[arg(long, default_value_t = 5, env = "GET_WAIT_TIMEOUT")]
    get_wait_timeout: u64,

    /// Allow X-Forward-Url to tee uploads to an external URL.
    /// SSRF risk if the server is exposed to untrusted clients.
    #[cfg(feature = "forward")]
    #[arg(long, env = "ALLOW_FORWARD")]
    allow_forward: bool,

    /// Optional separate listener exposing /health and /metrics.
    /// Bind to localhost (e.g. 127.0.0.1:9090) to keep stats off the data plane.
    #[arg(long, env = "METRICS_ADDR")]
    metrics_addr: Option<String>,
}

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix('G').or(s.strip_suffix('g')) {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('M').or(s.strip_suffix('m')) {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('K').or(s.strip_suffix('k')) {
        (n, 1024)
    } else {
        (s, 1)
    };

    num.trim()
        .parse::<u64>()
        .map_err(|e| format!("invalid size: {e}"))
        .and_then(|n| n.checked_mul(mult).ok_or_else(|| format!("size too large: {s}")))
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = ServerConfig {
        addr: args.listen,
        data_dir: PathBuf::from(args.data_dir),
        max_disk_usage: args.max_disk,
        max_pipe_size: args.max_pipe_size,
        put_ttl: args.put_ttl,
        get_ttl: args.get_ttl,
        get_wait_timeout: args.get_wait_timeout,
        #[cfg(feature = "forward")]
        allow_forward: args.allow_forward,
        metrics_addr: args.metrics_addr,
    };

    if let Some(max) = config.max_disk_usage {
        info!("max disk usage: {}", format_size(max));
    }

    if let Some(max) = config.max_pipe_size {
        info!("max pipe size: {}", format_size(max));
    }

    let handle = file_pipe::start_server(config).await.unwrap_or_else(|e| {
        error!("failed to start server: {e}");
        std::process::exit(1);
    });
    info!("file-pipe listening on {}", handle.addr);

    wait_for_signal().await;
    info!("draining - no new uploads accepted");
    handle.drain();

    wait_for_signal().await;
    info!("cleaning up and exiting");
    handle.cleanup().await;
    std::process::exit(0);
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{}G", bytes / (1024 * 1024 * 1024))
    } else if bytes >= 1024 * 1024 {
        format!("{}M", bytes / (1024 * 1024))
    } else if bytes >= 1024 {
        format!("{}K", bytes / 1024)
    } else {
        format!("{bytes}B")
    }
}

async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();

    tokio::select! {
        _ = sigint.recv() => {}
        _ = sigterm.recv() => {}
    }
}
