use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use tracing::{Instrument, error, info, warn};

use crate::error::BoxBody;
use crate::handler::handle;
use crate::state::AppState;

static NEXT_REQ_ID: AtomicU64 = AtomicU64::new(0);

pub struct ServerConfig {
    pub addr: String,
    pub data_dir: PathBuf,
    pub max_disk_usage: Option<u64>,
    /// Maximum bytes for a single pipe (default: unlimited).
    pub max_pipe_size: Option<u64>,
    /// Seconds to keep an entry after PUT completes (default: 30s).
    pub put_ttl: u64,
    /// Seconds to keep an entry after first GET completes (default: 5s).
    pub get_ttl: u64,
    /// Seconds a GET waits for a missing key before returning 404 (default: 5s).
    pub get_wait_timeout: u64,
    /// Allow X-Forward-Url to tee uploads to an external URL (default: false).
    /// SSRF risk if the server is exposed to untrusted clients.
    #[cfg(feature = "forward")]
    pub allow_forward: bool,
    /// Timeout in seconds for forward (X-Forward-Url) requests (default: 60s).
    #[cfg(feature = "forward")]
    pub forward_timeout: u64,
    /// Optional separate listener exposing /health and /metrics (default: off).
    pub metrics_addr: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:3000".into(),
            data_dir: std::env::temp_dir(),
            max_disk_usage: None,
            max_pipe_size: None,
            put_ttl: 30,
            get_ttl: 5,
            get_wait_timeout: 5,
            #[cfg(feature = "forward")]
            allow_forward: false,
            #[cfg(feature = "forward")]
            forward_timeout: 60,
            metrics_addr: None,
        }
    }
}

pub struct ServerHandle {
    pub addr: SocketAddr,
    pub metrics_addr: Option<SocketAddr>,
    state: Arc<AppState>,
}

impl ServerHandle {
    /// Number of active pipe entries (uploads in flight or held for the TTL window).
    pub fn pipes_count(&self) -> usize {
        self.state.pipes.len()
    }

    /// Number of transfers currently streaming bytes (uploads + downloads).
    pub fn active_transfers(&self) -> usize {
        self.state.active_uploads.load(Ordering::Relaxed)
            + self.state.active_downloads.load(Ordering::Relaxed)
    }

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
                info!("removing orphaned file: {}", path.display());

                if let Err(e) = std::fs::remove_file(&path) {
                    warn!("failed to remove {}: {e}", path.display());
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
        max_pipe_size: config.max_pipe_size,
        put_ttl: config.put_ttl,
        get_ttl: config.get_ttl,
        get_wait_timeout: config.get_wait_timeout,
        #[cfg(feature = "forward")]
        allow_forward: config.allow_forward,
        #[cfg(feature = "forward")]
        forward_client: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.forward_timeout))
            .build()
            .expect("failed to build reqwest client"),
        active_uploads: std::sync::atomic::AtomicUsize::new(0),
        active_downloads: std::sync::atomic::AtomicUsize::new(0),
    });

    let listener = TcpListener::bind(&config.addr).await?;
    let local_addr = listener.local_addr()?;

    let state_clone = state.clone();

    tokio::spawn(async move {
        loop {
            let (stream, remote) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!("accept failed: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            let state = state_clone.clone();

            tokio::spawn(async move {
                let io = TokioIo::new(stream);

                let service = service_fn(move |req| {
                    let state = state.clone();
                    let id = NEXT_REQ_ID.fetch_add(1, Ordering::Relaxed);
                    let span = tracing::info_span!("req", id);
                    handle(req, state).instrument(span)
                });

                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .header_read_timeout(std::time::Duration::from_secs(10))
                    .keep_alive(false)
                    .serve_connection(io, service)
                    .await
                {
                    warn!("connection error from {remote}: {e}");
                }
            });
        }
    });

    // Optional metrics listener on a separate address
    let metrics_local_addr = if let Some(metrics_addr) = config.metrics_addr {
        let metrics_listener = TcpListener::bind(&metrics_addr).await?;
        let local = metrics_listener.local_addr()?;
        info!("metrics listening on {}", local);

        let state_clone = state.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match metrics_listener.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("metrics accept failed: {e}");
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let state = state_clone.clone();

                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let service = service_fn(move |req| {
                        let state = state.clone();
                        async move { Ok::<_, Infallible>(metrics_handle(req, &state)) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await;
                });
            }
        });
        Some(local)
    } else {
        None
    };

    Ok(ServerHandle {
        addr: local_addr,
        metrics_addr: metrics_local_addr,
        state,
    })
}

fn metrics_handle(req: Request<Incoming>, state: &AppState) -> Response<BoxBody> {
    let (status, body) = match (req.method(), req.uri().path()) {
        (&Method::GET, "/health") => (StatusCode::OK, "ok\n".to_string()),
        (&Method::GET, "/metrics") => {
            let pipes = state.pipes.len();
            let disk_usage = state.disk_usage.load(Ordering::Relaxed);
            let key_waiters = state.key_waiters.len();
            let draining = state.draining.load(Ordering::Relaxed) as u8;
            let active_uploads = state.active_uploads.load(Ordering::Relaxed);
            let active_downloads = state.active_downloads.load(Ordering::Relaxed);
            (
                StatusCode::OK,
                format!(
                    "pipes {pipes}\ndisk_usage {disk_usage}\nkey_waiters {key_waiters}\nactive_uploads {active_uploads}\nactive_downloads {active_downloads}\ndraining {draining}\n"
                ),
            )
        }
        _ => (StatusCode::NOT_FOUND, "not found\n".to_string()),
    };

    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .body(
            Full::new(Bytes::from(body))
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap()
}
