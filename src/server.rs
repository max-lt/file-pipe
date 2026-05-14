use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dashmap::DashMap;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::handler::handle;
use crate::state::AppState;

pub struct ServerConfig {
    pub addr: String,
    pub data_dir: PathBuf,
    pub max_disk_usage: Option<u64>,
    /// Seconds to keep an entry after PUT completes (default: 30s).
    pub put_ttl: u64,
    /// Seconds to keep an entry after first GET completes (default: 5s).
    pub get_ttl: u64,
    /// Seconds a GET waits for a missing key before returning 404 (default: 5s).
    pub get_wait_timeout: u64,
    /// Allow X-Forward-Url to tee uploads to an external URL (default: false).
    /// SSRF risk if the server is exposed to untrusted clients.
    pub allow_forward: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:3000".into(),
            data_dir: std::env::temp_dir(),
            max_disk_usage: None,
            put_ttl: 30,
            get_ttl: 5,
            get_wait_timeout: 5,
            allow_forward: false,
        }
    }
}

pub struct ServerHandle {
    pub addr: SocketAddr,
    state: Arc<AppState>,
}

impl ServerHandle {
    /// Number of pending key waiters (GETs waiting for a non-existent key).
    pub fn key_waiters_count(&self) -> usize {
        self.state.key_waiters.len()
    }

    /// Start draining: reject new PUTs with 503, but let existing transfers finish.
    pub fn drain(&self) {
        self.state.draining.store(true, Ordering::Release);
    }

    /// Remove all temp files for active pipes.
    pub async fn cleanup(&self) {
        let keys: Vec<String> = self
            .state
            .pipes
            .iter()
            .map(|r| r.key().clone())
            .collect();

        for key in &keys {
            if let Some((_, entry)) = self.state.pipes.remove(key) {
                crate::state::free_entry(&self.state, key, &entry).await;
            }
        }
    }
}

pub async fn start_server(config: ServerConfig) -> std::io::Result<ServerHandle> {
    std::fs::create_dir_all(&config.data_dir)?;

    // Clean up orphaned temp files from a prior crash
    if let Ok(entries) = std::fs::read_dir(&config.data_dir) {
        for entry in entries.flatten() {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("pipe-"))
            {
                let path = entry.path();
                eprintln!("[STARTUP] removing orphaned file: {}", path.display());

                if let Err(e) = std::fs::remove_file(&path) {
                    eprintln!("[STARTUP] failed to remove {}: {e}", path.display());
                }
            }
        }
    }

    let state = Arc::new(AppState {
        pipes: DashMap::new(),
        key_waiters: DashMap::new(),
        draining: AtomicBool::new(false),
        data_dir: config.data_dir,
        disk_usage: AtomicU64::new(0),
        max_disk_usage: config.max_disk_usage,
        put_ttl: config.put_ttl,
        get_ttl: config.get_ttl,
        get_wait_timeout: config.get_wait_timeout,
        allow_forward: config.allow_forward,
    });

    let listener = TcpListener::bind(&config.addr).await?;
    let local_addr = listener.local_addr()?;

    let state_clone = state.clone();

    tokio::spawn(async move {
        loop {
            let (stream, remote) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("[ERROR] accept failed: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            let state = state_clone.clone();

            tokio::spawn(async move {
                let io = TokioIo::new(stream);

                let service = service_fn(move |req| {
                    let state = state.clone();
                    handle(req, state)
                });

                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .header_read_timeout(std::time::Duration::from_secs(10))
                    .keep_alive(false)
                    .serve_connection(io, service)
                    .await
                {
                    eprintln!("[ERROR] {remote}: {e}");
                }
            });
        }
    });

    Ok(ServerHandle {
        addr: local_addr,
        state,
    })
}
