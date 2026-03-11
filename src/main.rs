use clap::Parser;
use file_pipe::ServerConfig;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "HTTP pipe service — stream uploads to downloads in real-time")]
struct Args {
    /// Address to listen on
    #[arg(short, long, default_value = "0.0.0.0:3000")]
    listen: String,

    /// Directory for temporary files
    #[arg(short, long, default_value_t = std::env::temp_dir().display().to_string())]
    data_dir: String,

    /// Maximum disk usage (e.g. "1G", "500M", "1024K")
    #[arg(short = 'D', long, value_parser = parse_size)]
    max_disk: Option<u64>,

    /// Maximum memory usage (e.g. "256M", "1G"). When full, new data spills to disk.
    #[arg(short = 'M', long, value_parser = parse_size)]
    max_memory: Option<u64>,

    /// Files smaller than this stay in memory (default: "1M")
    #[arg(short, long, value_parser = parse_size, default_value = "1M")]
    spill_threshold: u64,
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
        .map(|n| n * mult)
        .map_err(|e| format!("invalid size: {e}"))
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config = ServerConfig {
        addr: args.listen,
        data_dir: PathBuf::from(args.data_dir),
        max_disk_usage: args.max_disk,
        max_memory: args.max_memory,
        spill_threshold: args.spill_threshold,
    };

    if let Some(max) = config.max_memory {
        eprintln!("max memory: {}", format_size(max));
    }

    if let Some(max) = config.max_disk_usage {
        eprintln!("max disk usage: {}", format_size(max));
    }

    eprintln!("spill threshold: {}", format_size(config.spill_threshold));

    let handle = file_pipe::start_server(config).await;
    eprintln!("file-pipe listening on {}", handle.addr);

    wait_for_signal().await;
    eprintln!("[SHUTDOWN] draining - no new uploads accepted");
    handle.drain();

    wait_for_signal().await;
    eprintln!("[SHUTDOWN] cleaning up and exiting");
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
