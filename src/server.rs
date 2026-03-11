use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::{Notify, RwLock};

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
    /// Start draining: reject new PUTs with 503, but let existing transfers finish.
    pub fn drain(&self) {
        self.state.draining.store(true, Ordering::Relaxed);
    }

    /// Remove all temp files for active pipes.
    pub async fn cleanup(&self) {
        let mut map = self.state.pipes.write().await;
        let keys: Vec<String> = map.keys().cloned().collect();

        for key in &keys {
            if let Some(entry) = map.remove(key) {
                let written = entry.written.load(Ordering::Relaxed);

                if entry.spilled.load(Ordering::Relaxed) {
                    self.state.disk_usage.fetch_sub(written, Ordering::Relaxed);

                    if let Err(e) = std::fs::remove_file(&entry.path) {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            eprintln!("[CLEANUP] key={key} failed to remove file: {e}");
                        }
                    }
                } else {
                    self.state
                        .memory_usage
                        .fetch_sub(written, Ordering::Relaxed);
                }

                eprintln!("[CLEANUP] key={key} removed ({written} bytes freed)");
            }
        }
    }
}

pub async fn start_server(config: ServerConfig) -> ServerHandle {
    std::fs::create_dir_all(&config.data_dir).expect("failed to create data directory");

    let state = Arc::new(AppState {
        pipes: RwLock::new(HashMap::new()),
        key_added: Notify::new(),
        draining: AtomicBool::new(false),
        data_dir: config.data_dir,
        disk_usage: AtomicU64::new(0),
        max_disk_usage: config.max_disk_usage,
        memory_usage: AtomicU64::new(0),
        max_memory: config.max_memory,
        spill_threshold: config.spill_threshold,
    });

    let listener = TcpListener::bind(&config.addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let state_clone = state.clone();

    tokio::spawn(async move {
        loop {
            let (stream, remote) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("[ERROR] accept failed: {e}");
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
                    .serve_connection(io, service)
                    .await
                {
                    eprintln!("[ERROR] {remote}: {e}");
                }
            });
        }
    });

    ServerHandle {
        addr: local_addr,
        state,
    }
}
