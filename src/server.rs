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
    pub max_memory: Option<u64>,
    /// Files smaller than this stay in memory (default: 1MB).
    pub spill_threshold: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:3000".into(),
            data_dir: std::env::temp_dir(),
            max_disk_usage: None,
            max_memory: None,
            spill_threshold: 1024 * 1024,
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

    let state = Arc::new(AppState {
        pipes: DashMap::new(),
        key_waiters: DashMap::new(),
        draining: AtomicBool::new(false),
        data_dir: config.data_dir,
        disk_usage: AtomicU64::new(0),
        max_disk_usage: config.max_disk_usage,
        memory_usage: AtomicU64::new(0),
        max_memory: config.max_memory,
        spill_threshold: config.spill_threshold,
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
