#[tokio::main]
async fn main() {
    let handle = file_pipe::start_server("0.0.0.0:3000").await;
    eprintln!("file-pipe listening on {}", handle.addr);

    wait_for_signal().await;
    eprintln!("[SHUTDOWN] draining - no new uploads accepted");
    handle.drain();

    wait_for_signal().await;
    eprintln!("[SHUTDOWN] cleaning up and exiting");
    handle.cleanup().await;
    std::process::exit(0);
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
