use bytes::Bytes;
use futures::StreamExt;
use reqwest::Client;
use std::time::Instant;
use tokio::time::Duration;

async fn spawn_server() -> file_pipe::ServerHandle {
    file_pipe::start_server("127.0.0.1:0").await
}

fn base_url(handle: &file_pipe::ServerHandle) -> String {
    format!("http://{}", handle.addr)
}

#[tokio::test]
async fn put_then_get() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    client
        .put(format!("{base}/simple"))
        .body("hello world")
        .send()
        .await
        .unwrap();

    let resp = client.get(format!("{base}/simple")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "hello world");
}

#[tokio::test]
async fn get_before_put_waits() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let base2 = base.clone();
    let client = Client::new();

    // GET first - will wait up to 5s
    let get_handle = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base2}/delayed"))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        resp.text().await.unwrap()
    });

    // PUT after 500ms
    tokio::time::sleep(Duration::from_millis(500)).await;

    client
        .put(format!("{base}/delayed"))
        .body("arrived late")
        .send()
        .await
        .unwrap();

    let body = get_handle.await.unwrap();
    assert_eq!(body, "arrived late");
}

#[tokio::test]
async fn get_timeout_returns_404() {
    let srv = spawn_server().await;
    let base = base_url(&srv);

    let start = Instant::now();
    let resp = Client::new()
        .get(format!("{base}/missing"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
    assert!(start.elapsed() >= Duration::from_secs(4));
}

#[tokio::test]
async fn duplicate_put_returns_conflict() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    client
        .put(format!("{base}/dup"))
        .body("first")
        .send()
        .await
        .unwrap();

    let resp = client
        .put(format!("{base}/dup"))
        .body("second")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 409);
}

#[tokio::test]
async fn streaming_pipe() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let base2 = base.clone();

    // Use a channel to simulate a slow upload
    let (body_tx, body_rx) = tokio::sync::mpsc::channel::<Result<Bytes, reqwest::Error>>(16);

    let body_stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);

    // PUT with streaming body
    let put_handle = tokio::spawn(async move {
        let resp = Client::new()
            .put(format!("{base2}/stream"))
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
    });

    // Wait for PUT to register the key
    tokio::time::sleep(Duration::from_millis(100)).await;

    // GET streaming response
    let get_handle = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base}/stream"))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let mut stream = resp.bytes_stream();
        let mut received = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            received.push(chunk);
        }

        received
    });

    // Send chunks with delays
    tokio::time::sleep(Duration::from_millis(100)).await;
    body_tx.send(Ok(Bytes::from("chunk1"))).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    body_tx.send(Ok(Bytes::from("chunk2"))).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    body_tx.send(Ok(Bytes::from("chunk3"))).await.unwrap();

    // Close the upload
    drop(body_tx);

    put_handle.await.unwrap();
    let received = get_handle.await.unwrap();

    // Verify all data was received
    let all: Vec<u8> = received.into_iter().flat_map(|b| b.to_vec()).collect();
    assert_eq!(String::from_utf8(all).unwrap(), "chunk1chunk2chunk3");
}

#[tokio::test]
async fn multiple_readers() {
    let srv = spawn_server().await;
    let base = base_url(&srv);

    // PUT some data
    Client::new()
        .put(format!("{base}/multi"))
        .body("shared data")
        .send()
        .await
        .unwrap();

    // Multiple concurrent GETs
    let mut handles = Vec::new();

    for _ in 0..3 {
        let url = format!("{base}/multi");

        handles.push(tokio::spawn(async move {
            let resp = Client::new().get(&url).send().await.unwrap();
            assert_eq!(resp.status(), 200);
            resp.text().await.unwrap()
        }));
    }

    for handle in handles {
        let body = handle.await.unwrap();
        assert_eq!(body, "shared data");
    }
}

#[tokio::test]
async fn content_length_forwarded() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();
    let data = "known length body";

    client
        .put(format!("{base}/cl"))
        .body(data)
        .send()
        .await
        .unwrap();

    let resp = client.get(format!("{base}/cl")).send().await.unwrap();
    assert_eq!(resp.content_length(), Some(data.len() as u64));
    assert_eq!(resp.text().await.unwrap(), data);
}

#[tokio::test]
async fn empty_key_returns_400() {
    let srv = spawn_server().await;
    let base = base_url(&srv);

    let resp = Client::new().get(format!("{base}/")).send().await.unwrap();

    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn post_returns_405() {
    let srv = spawn_server().await;
    let base = base_url(&srv);

    let resp = Client::new()
        .post(format!("{base}/test"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 405);
}

#[tokio::test]
async fn drain_rejects_new_puts() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    // PUT works before drain
    let resp = client
        .put(format!("{base}/before"))
        .body("ok")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // Drain
    srv.drain();

    // New PUT gets 503
    let resp = client
        .put(format!("{base}/after"))
        .body("nope")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 503);

    // Existing key is still readable
    let resp = client.get(format!("{base}/before")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}
