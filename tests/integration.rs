use bytes::Bytes;
use futures::StreamExt;
use reqwest::Client;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;

async fn spawn_server() -> file_pipe::ServerHandle {
    file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        ..Default::default()
    })
    .await
    .unwrap()
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
async fn second_get_not_stuck_when_first_times_out() {
    // Two GETs wait on the same key. The first times out after 5s and must
    // not break the second GET's notification channel. When the PUT arrives
    // (within the second GET's deadline), the second GET must receive data.
    let srv = spawn_server().await;
    let base = base_url(&srv);

    // GET-A: will timeout (no PUT within 5s)
    let base_a = base.clone();
    let get_a = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base_a}/waiter-race"))
            .send()
            .await
            .unwrap();
        resp.status()
    });

    // GET-B: starts 1s later, so its 5s deadline extends to ~t+6s
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let base_b = base.clone();
    let get_b = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base_b}/waiter-race"))
            .send()
            .await
            .unwrap();
        (resp.status(), resp.text().await.unwrap())
    });

    // PUT arrives at ~t+5.5s — after GET-A's deadline but before GET-B's
    tokio::time::sleep(Duration::from_millis(4600)).await;
    Client::new()
        .put(format!("{base}/waiter-race"))
        .body("hello")
        .send()
        .await
        .unwrap();

    let status_a = get_a.await.unwrap();
    assert_eq!(status_a, 404);

    let (status_b, body_b) = get_b.await.unwrap();
    assert_eq!(status_b, 200);
    assert_eq!(body_b, "hello");
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

#[tokio::test]
async fn disk_quota_enforced() {
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        max_disk_usage: Some(100),
        spill_threshold: 0,
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let client = Client::new();

    // First PUT: 50 bytes — should succeed
    let resp = client
        .put(format!("{base}/small"))
        .body("x".repeat(50))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // Second PUT: 60 bytes — would exceed 100 byte limit
    let resp = client
        .put(format!("{base}/toobig"))
        .body("y".repeat(60))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 503);
    assert!(resp.text().await.unwrap().contains("disk quota"));
}

#[tokio::test]
async fn memory_limit_spills_to_disk() {
    // max_memory=80 bytes, spill_threshold=1M (high), so small files normally stay in memory.
    // But once global memory is full, new uploads should spill to disk instead of failing.
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        max_memory: Some(80),
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let client = Client::new();

    // First PUT: 50 bytes — fits in memory
    let resp = client
        .put(format!("{base}/a"))
        .body("x".repeat(50))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // Second PUT: 50 bytes — would exceed 80 byte memory limit, should spill to disk (not fail)
    let resp = client
        .put(format!("{base}/b"))
        .body("y".repeat(50))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // Both should still be readable
    let resp = client.get(format!("{base}/a")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "x".repeat(50));

    let resp = client.get(format!("{base}/b")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "y".repeat(50));
}

#[tokio::test]
async fn memory_limit_with_disk_quota() {
    // max_memory=80, max_disk=60, spill_threshold=1M.
    // First file (50B) fits in memory. Second file (50B) spills to disk (memory full).
    // Third file (50B) also tries to spill but disk quota is exceeded → 503.
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        max_memory: Some(80),
        max_disk_usage: Some(60),
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let client = Client::new();

    // First: 50B in memory — OK
    let resp = client
        .put(format!("{base}/mem"))
        .body("a".repeat(50))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // Second: 50B spills to disk (memory full) — OK (fits in 60B disk quota)
    let resp = client
        .put(format!("{base}/disk"))
        .body("b".repeat(50))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // Third: 50B tries to spill but disk quota exceeded — 503
    let resp = client
        .put(format!("{base}/fail"))
        .body("c".repeat(50))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 503);
    assert!(resp.text().await.unwrap().contains("disk quota"));
}

#[tokio::test]
async fn spill_threshold_forces_disk() {
    // spill_threshold=0 means everything goes to disk immediately.
    // Verify data is still readable.
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        spill_threshold: 0,
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let client = Client::new();

    let data = "this goes straight to disk";

    client
        .put(format!("{base}/disk"))
        .body(data)
        .send()
        .await
        .unwrap();

    let resp = client.get(format!("{base}/disk")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), data);
}

#[tokio::test]
async fn multipart_upload_preserves_filename_and_mime() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(b"hello multipart".to_vec())
            .file_name("report.pdf")
            .mime_str("application/pdf")
            .unwrap(),
    );

    let resp = client
        .put(format!("{base}/mp"))
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let resp = client.get(format!("{base}/mp")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "application/pdf"
    );
    assert_eq!(
        resp.headers()
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap(),
        "attachment; filename=\"report.pdf\""
    );

    assert_eq!(resp.text().await.unwrap(), "hello multipart");
}

#[tokio::test]
async fn multipart_upload_without_metadata() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    // Part without filename or explicit mime type
    let form = reqwest::multipart::Form::new()
        .part("data", reqwest::multipart::Part::bytes(b"just data".to_vec()));

    let resp = client
        .put(format!("{base}/mp-plain"))
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let resp = client.get(format!("{base}/mp-plain")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // No Content-Disposition since there was no filename
    assert!(resp.headers().get("content-disposition").is_none());
    assert_eq!(resp.text().await.unwrap(), "just data");
}

#[tokio::test]
async fn raw_upload_preserves_content_type() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    let resp = client
        .put(format!("{base}/typed"))
        .header("content-type", "image/png")
        .body("fake png data")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let resp = client.get(format!("{base}/typed")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "image/png"
    );
    assert_eq!(resp.text().await.unwrap(), "fake png data");
}

// --- File path collision: keys with / vs _ must not collide ---

#[tokio::test]
async fn keys_with_slash_vs_underscore_dont_collide() {
    // Keys "a/b" and "a_b" previously mapped to the same temp file.
    // Both should store and return their own data independently.
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        spill_threshold: 0, // force disk so file paths matter
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let client = Client::new();

    client
        .put(format!("{base}/a/b"))
        .body("slash")
        .send()
        .await
        .unwrap();

    client
        .put(format!("{base}/a_b"))
        .body("underscore")
        .send()
        .await
        .unwrap();

    let resp = client.get(format!("{base}/a/b")).send().await.unwrap();
    assert_eq!(resp.text().await.unwrap(), "slash");

    let resp = client.get(format!("{base}/a_b")).send().await.unwrap();
    assert_eq!(resp.text().await.unwrap(), "underscore");
}

// --- Filename sanitization (header injection prevention) ---

#[tokio::test]
async fn multipart_filename_control_chars_are_stripped() {
    // Defense-in-depth: control characters in filenames are stripped from
    // the Content-Disposition response header. We craft a raw multipart body
    // with a null byte in the filename, since \r\n can't survive multipart framing.
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    let boundary = "----testboundary9876";
    let mut body = Vec::new();
    body.extend_from_slice(b"------testboundary9876\r\n");
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"file\"; filename=\"evil\tfile.txt\"\r\n");
    body.extend_from_slice(b"Content-Type: text/plain\r\n");
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(b"payload\r\n");
    body.extend_from_slice(b"------testboundary9876--\r\n");

    let resp = client
        .put(format!("{base}/inject"))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let resp = client.get(format!("{base}/inject")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let cd = resp
        .headers()
        .get("content-disposition")
        .unwrap()
        .to_str()
        .unwrap();

    // No control chars should survive in the header value
    assert!(
        !cd.chars().any(|c| c.is_control()),
        "Content-Disposition contains control chars: {cd:?}"
    );
    assert!(cd.contains("filename="));
}

// --- Spill during active streaming ---

#[tokio::test]
async fn reader_survives_spill_during_streaming() {
    // spill_threshold=64 so the first few chunks stay in memory, then spill to disk.
    // A reader actively streaming should receive all data correctly across the transition.
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        spill_threshold: 64,
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let base2 = base.clone();

    let (body_tx, body_rx) = tokio::sync::mpsc::channel::<Result<Bytes, reqwest::Error>>(16);
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);

    // PUT with streaming body
    let put_handle = tokio::spawn(async move {
        Client::new()
            .put(format!("{base2}/spill-stream"))
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
            .unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // GET starts while upload is still in memory
    let get_handle = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base}/spill-stream"))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let mut stream = resp.bytes_stream();
        let mut received = Vec::new();

        while let Some(chunk) = stream.next().await {
            received.extend_from_slice(&chunk.unwrap());
        }

        received
    });

    // Send small chunks that fit in memory (< 64 bytes)
    tokio::time::sleep(Duration::from_millis(100)).await;
    body_tx.send(Ok(Bytes::from("A".repeat(30)))).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    body_tx.send(Ok(Bytes::from("B".repeat(30)))).await.unwrap();

    // This chunk should trigger the spill (total > 64)
    tokio::time::sleep(Duration::from_millis(100)).await;
    body_tx.send(Ok(Bytes::from("C".repeat(30)))).await.unwrap();

    // More data after spill
    tokio::time::sleep(Duration::from_millis(100)).await;
    body_tx.send(Ok(Bytes::from("D".repeat(30)))).await.unwrap();

    drop(body_tx);

    let resp = put_handle.await.unwrap();
    assert_eq!(resp.status(), 200);

    let received = get_handle.await.unwrap();
    let expected = format!(
        "{}{}{}{}",
        "A".repeat(30),
        "B".repeat(30),
        "C".repeat(30),
        "D".repeat(30)
    );
    assert_eq!(received.len(), expected.len());
    assert_eq!(String::from_utf8(received).unwrap(), expected);
}

// --- Second GET after spill must read complete data from disk ---

#[tokio::test]
async fn second_reader_after_spill_gets_full_data() {
    // GET1 connects while data is in memory. The stream spills to disk.
    // GET2 connects after the spill. Both must receive all the data.
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        spill_threshold: 64,
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let base_get1 = base.clone();
    let base_get2 = base.clone();

    let (body_tx, body_rx) = tokio::sync::mpsc::channel::<Result<Bytes, reqwest::Error>>(16);
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);

    let base_put = base.clone();

    let put_handle = tokio::spawn(async move {
        Client::new()
            .put(format!("{base_put}/spill-two-readers"))
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
            .unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // GET1 connects while still in memory
    let get1_handle = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base_get1}/spill-two-readers"))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let mut stream = resp.bytes_stream();
        let mut received = Vec::new();

        while let Some(chunk) = stream.next().await {
            received.extend_from_slice(&chunk.unwrap());
        }

        received
    });

    // Send data that fits in memory (< 64 bytes)
    body_tx.send(Ok(Bytes::from("A".repeat(30)))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // This chunk triggers the spill (total 90 > 64)
    body_tx.send(Ok(Bytes::from("B".repeat(60)))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // GET2 connects AFTER the spill — must read everything from disk
    let get2_handle = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base_get2}/spill-two-readers"))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let mut stream = resp.bytes_stream();
        let mut received = Vec::new();

        while let Some(chunk) = stream.next().await {
            received.extend_from_slice(&chunk.unwrap());
        }

        received
    });

    // More data after spill
    tokio::time::sleep(Duration::from_millis(100)).await;
    body_tx.send(Ok(Bytes::from("C".repeat(40)))).await.unwrap();
    drop(body_tx);

    put_handle.await.unwrap();

    let expected = format!("{}{}{}", "A".repeat(30), "B".repeat(60), "C".repeat(40));

    let received1 = get1_handle.await.unwrap();
    assert_eq!(
        received1.len(),
        expected.len(),
        "GET1 (started before spill) got wrong length"
    );
    assert_eq!(String::from_utf8(received1).unwrap(), expected);

    let received2 = get2_handle.await.unwrap();
    assert_eq!(
        received2.len(),
        expected.len(),
        "GET2 (started after spill) got wrong length"
    );
    assert_eq!(String::from_utf8(received2).unwrap(), expected);
}

// --- Concurrent readers during live streaming ---

#[tokio::test]
async fn multiple_readers_during_active_streaming() {
    let srv = spawn_server().await;
    let base = Arc::new(base_url(&srv));

    let (body_tx, body_rx) = tokio::sync::mpsc::channel::<Result<Bytes, reqwest::Error>>(16);
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);

    let base_put = base.clone();

    tokio::spawn(async move {
        Client::new()
            .put(format!("{base_put}/live-multi"))
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn 3 concurrent readers
    let mut get_handles = Vec::new();

    for _ in 0..3 {
        let base_get = base.clone();

        get_handles.push(tokio::spawn(async move {
            let resp = Client::new()
                .get(format!("{base_get}/live-multi"))
                .send()
                .await
                .unwrap();

            assert_eq!(resp.status(), 200);

            let mut stream = resp.bytes_stream();
            let mut received = Vec::new();

            while let Some(chunk) = stream.next().await {
                received.extend_from_slice(&chunk.unwrap());
            }

            received
        }));
    }

    // Send data while readers are active
    for i in 0..5 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        body_tx
            .send(Ok(Bytes::from(format!("chunk{i}"))))
            .await
            .unwrap();
    }

    drop(body_tx);

    let expected = "chunk0chunk1chunk2chunk3chunk4";

    for handle in get_handles {
        let received = handle.await.unwrap();
        assert_eq!(String::from_utf8(received).unwrap(), expected);
    }
}

// --- Large file through disk spill with integrity check ---

#[tokio::test]
async fn large_file_through_disk_spill() {
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        spill_threshold: 1024, // 1KB threshold
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let client = Client::new();

    // 100KB of patterned data to verify integrity
    let data: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    let expected = data.clone();

    client
        .put(format!("{base}/bigfile"))
        .body(data)
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base}/bigfile"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let received = resp.bytes().await.unwrap();
    assert_eq!(received.len(), expected.len());
    assert_eq!(received.as_ref(), expected.as_slice());
}

// --- Writer disconnect mid-upload ---

#[tokio::test]
async fn writer_disconnect_mid_upload() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let base2 = base.clone();

    let (body_tx, body_rx) = tokio::sync::mpsc::channel::<Result<Bytes, reqwest::Error>>(16);
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);

    let put_handle = tokio::spawn(async move {
        // This may succeed or fail depending on timing — we don't care about the PUT response
        let _ = Client::new()
            .put(format!("{base2}/disconnect"))
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
    .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start reading
    let get_handle = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base}/disconnect"))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let mut stream = resp.bytes_stream();
        let mut received = Vec::new();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(data) => received.extend_from_slice(&data),
                Err(_) => break,
            }
        }

        received
    });

    // Send some data then drop (disconnect)
    body_tx.send(Ok(Bytes::from("partial"))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(body_tx);

    put_handle.await.unwrap();
    let received = get_handle.await.unwrap();

    // Reader should have received whatever was sent before disconnect
    assert_eq!(String::from_utf8(received).unwrap(), "partial");
}

// --- Key reuse after cleanup ---

#[tokio::test]
async fn key_reusable_after_cleanup() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    // First upload
    client
        .put(format!("{base}/reuse"))
        .body("first")
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base}/reuse"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.text().await.unwrap(), "first");

    // Wait for cleanup (5s after first GET + margin)
    tokio::time::sleep(Duration::from_secs(7)).await;

    // Key should be gone — reuse it
    client
        .put(format!("{base}/reuse"))
        .body("second")
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base}/reuse"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "second");
}

// --- key_waiters must be cleaned up after GET timeout ---

#[tokio::test]
async fn key_waiters_cleaned_up_after_timeout() {
    // Regression test: GETs to non-existent keys create key_waiters entries.
    // After all GETs timeout, the entries must be cleaned up (not leak).
    let srv = spawn_server().await;
    let base = Arc::new(base_url(&srv));

    assert_eq!(srv.key_waiters_count(), 0);

    // Send 50 GETs to unique non-existent keys (all will timeout after 5s)
    let mut handles = Vec::new();

    for i in 0..50 {
        let base = base.clone();

        handles.push(tokio::spawn(async move {
            let resp = Client::new()
                .get(format!("{base}/ghost-{i}"))
                .send()
                .await
                .unwrap();

            assert_eq!(resp.status(), 404);
        }));
    }

    // While GETs are waiting, key_waiters should be populated
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(srv.key_waiters_count() > 0);

    // Wait for all GETs to timeout
    for handle in handles {
        handle.await.unwrap();
    }

    // Give a moment for cleanup
    tokio::time::sleep(Duration::from_millis(200)).await;

    // key_waiters should be empty now
    assert_eq!(srv.key_waiters_count(), 0);
}

// --- First-GET cleanup must not fire while upload is still in progress ---

#[tokio::test]
async fn get_cleanup_waits_for_upload_to_finish() {
    // Regression test: the 5s first-GET cleanup timer must NOT remove the
    // entry while the writer is still uploading. A slow upload that outlasts
    // the 5s window must survive.
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let base2 = base.clone();
    let base3 = base.clone();

    let (body_tx, body_rx) = tokio::sync::mpsc::channel::<Result<Bytes, reqwest::Error>>(16);
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);

    // Start a slow streaming PUT
    let put_handle = tokio::spawn(async move {
        Client::new()
            .put(format!("{base2}/slow-upload"))
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
            .unwrap()
            .status()
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send first chunk
    body_tx.send(Ok(Bytes::from("chunk1-"))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // GET while upload is in progress — triggers 5s cleanup timer
    let get_handle = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base}/slow-upload"))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let mut stream = resp.bytes_stream();
        let mut received = Vec::new();

        while let Some(chunk) = stream.next().await {
            received.extend_from_slice(&chunk.unwrap());
        }

        received
    });

    // Wait for the 5s cleanup timer to fire (+ margin)
    tokio::time::sleep(Duration::from_secs(7)).await;

    // Continue uploading AFTER the timer would have fired
    body_tx.send(Ok(Bytes::from("chunk2-"))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    body_tx.send(Ok(Bytes::from("chunk3"))).await.unwrap();
    drop(body_tx);

    let put_status = put_handle.await.unwrap();
    assert_eq!(put_status, 200);

    let received = get_handle.await.unwrap();
    assert_eq!(
        String::from_utf8(received).unwrap(),
        "chunk1-chunk2-chunk3"
    );

    // A second GET after upload completed should also work
    tokio::time::sleep(Duration::from_millis(200)).await;

    let resp = Client::new()
        .get(format!("{base3}/slow-upload"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "chunk1-chunk2-chunk3");
}

// --- Double cleanup: stale timer must not delete recycled key ---

#[tokio::test]
async fn stale_cleanup_timer_does_not_delete_recycled_key() {
    // Regression test: PUT1 schedules a 30s cleanup. GET triggers a 5s cleanup
    // that removes the entry first. PUT2 recycles the key. The stale 30s timer
    // from PUT1 must NOT remove PUT2's entry.
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    // PUT1: upload "first"
    let resp = client
        .put(format!("{base}/recycle-safe"))
        .body("first")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // GET1: triggers 5s first-GET cleanup timer
    let resp = client
        .get(format!("{base}/recycle-safe"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.text().await.unwrap(), "first");

    // Wait for 5s cleanup to fire (key removed)
    tokio::time::sleep(Duration::from_secs(7)).await;

    // PUT2: recycle the key with new data
    let resp = client
        .put(format!("{base}/recycle-safe"))
        .body("second")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // Wait for PUT1's stale 30s timer to fire (30s from test start)
    tokio::time::sleep(Duration::from_secs(25)).await;

    // GET2: should still return PUT2's data (stale timer must be a no-op)
    let resp = client
        .get(format!("{base}/recycle-safe"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "second");
}

// --- Spill race: reader must not OOB access buffer during spill ---

#[tokio::test]
async fn spill_race_concurrent_readers_stress() {
    // Stress test for the spill race: many readers streaming while the writer
    // triggers a memory→disk spill. If the reader sees the bumped `written`
    // but stale `spilled=false`, it would OOB-index into the in-memory buffer.
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        spill_threshold: 64,
        ..Default::default()
    })
    .await
    .unwrap();
    let base = Arc::new(base_url(&srv));

    for iteration in 0..10 {
        let key = format!("spill-race-{iteration}");
        let (body_tx, body_rx) =
            tokio::sync::mpsc::channel::<Result<Bytes, reqwest::Error>>(1);
        let body_stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);

        let base_put = base.clone();
        let key_put = key.clone();

        tokio::spawn(async move {
            let _ = Client::new()
                .put(format!("{base_put}/{key_put}"))
                .body(reqwest::Body::wrap_stream(body_stream))
                .send()
                .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Spawn several concurrent readers
        let mut get_handles = Vec::new();

        for _ in 0..5 {
            let base_get = base.clone();
            let key_get = key.clone();

            get_handles.push(tokio::spawn(async move {
                let resp = Client::new()
                    .get(format!("{base_get}/{key_get}"))
                    .send()
                    .await
                    .unwrap();

                let mut stream = resp.bytes_stream();
                let mut received = Vec::new();

                while let Some(chunk) = stream.next().await {
                    received.extend_from_slice(&chunk.unwrap());
                }

                received
            }));
        }

        // Small chunk fits in memory (< 64 bytes)
        body_tx.send(Ok(Bytes::from("A".repeat(32)))).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        // This chunk triggers the spill (total 96 > 64)
        body_tx.send(Ok(Bytes::from("B".repeat(64)))).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        // More data after spill
        body_tx.send(Ok(Bytes::from("C".repeat(32)))).await.unwrap();
        drop(body_tx);

        let expected = format!("{}{}{}", "A".repeat(32), "B".repeat(64), "C".repeat(32));

        for handle in get_handles {
            let received = handle.await.unwrap();
            assert_eq!(String::from_utf8(received).unwrap(), expected);
        }
    }
}

// --- Key length limit ---

#[tokio::test]
async fn key_too_long_returns_400() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    let long_key = "x".repeat(513);

    let resp = client
        .put(format!("{base}/{long_key}"))
        .body("data")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    assert!(resp.text().await.unwrap().contains("512"));

    // 512 bytes should be fine
    let ok_key = "y".repeat(512);

    let resp = client
        .put(format!("{base}/{ok_key}"))
        .body("data")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
}

// --- Concurrent PUT race on same key ---

#[tokio::test]
async fn concurrent_put_same_key_one_wins() {
    let srv = spawn_server().await;
    let base = Arc::new(base_url(&srv));

    let mut handles = Vec::new();

    for i in 0..10 {
        let base = base.clone();

        handles.push(tokio::spawn(async move {
            Client::new()
                .put(format!("{base}/race-key"))
                .body(format!("writer-{i}"))
                .send()
                .await
                .unwrap()
                .status()
        }));
    }

    let mut ok_count = 0;
    let mut conflict_count = 0;

    for handle in handles {
        match handle.await.unwrap().as_u16() {
            200 => ok_count += 1,
            409 => conflict_count += 1,
            other => panic!("unexpected status: {other}"),
        }
    }

    // Exactly one writer should win
    assert_eq!(ok_count, 1);
    assert_eq!(conflict_count, 9);
}

// --- Empty body PUT ---

#[tokio::test]
async fn put_empty_body() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    let resp = client
        .put(format!("{base}/empty"))
        .body("")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let resp = client.get(format!("{base}/empty")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "");
}

// --- Keys with special characters ---

#[tokio::test]
async fn key_with_unicode_and_special_chars() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    // Keys with various special characters
    let keys = vec![
        "hello%20world",
        "path/with/slashes",
        "dots...lots",
        "key-with-dashes_and_underscores",
    ];

    for (i, key) in keys.iter().enumerate() {
        let data = format!("data-{i}");

        client
            .put(format!("{base}/{key}"))
            .body(data.clone())
            .send()
            .await
            .unwrap();

        let resp = client.get(format!("{base}/{key}")).send().await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), data);
    }
}

// --- Disk quota hit during streaming upload ---

#[tokio::test]
async fn disk_quota_exceeded_during_streaming() {
    // Quota is 200 bytes. Stream chunks that eventually exceed the limit.
    // The reader should receive whatever was written before the quota hit.
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        spill_threshold: 0,
        max_disk_usage: Some(200),
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let base2 = base.clone();

    let (body_tx, body_rx) = tokio::sync::mpsc::channel::<Result<Bytes, reqwest::Error>>(16);
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);

    let put_handle = tokio::spawn(async move {
        Client::new()
            .put(format!("{base2}/quota-stream"))
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
            .unwrap()
            .status()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let get_handle = tokio::spawn(async move {
        let resp = Client::new()
            .get(format!("{base}/quota-stream"))
            .send()
            .await
            .unwrap();

        let mut stream = resp.bytes_stream();
        let mut received = Vec::new();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(data) => received.extend_from_slice(&data),
                Err(_) => break,
            }
        }

        received
    });

    // First chunks fit in quota
    body_tx
        .send(Ok(Bytes::from("A".repeat(100))))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // This chunk should exceed the 200 byte quota
    body_tx
        .send(Ok(Bytes::from("B".repeat(150))))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    drop(body_tx);

    let put_status = put_handle.await.unwrap();
    assert_eq!(put_status, 503);

    let received = get_handle.await.unwrap();
    // Reader should have received at least the first chunk
    assert!(received.len() >= 100);
}

// --- Reader disconnects mid-stream ---

#[tokio::test]
async fn reader_disconnect_doesnt_break_upload() {
    // A reader disconnecting mid-stream must not affect the upload.
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let base2 = base.clone();
    let base3 = base.clone();

    let (body_tx, body_rx) = tokio::sync::mpsc::channel::<Result<Bytes, reqwest::Error>>(16);
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(body_rx);

    let put_handle = tokio::spawn(async move {
        Client::new()
            .put(format!("{base2}/reader-dc"))
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
            .unwrap()
            .status()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // First reader connects and reads one chunk then disconnects
    {
        let resp = Client::new()
            .get(format!("{base}/reader-dc"))
            .send()
            .await
            .unwrap();

        let mut stream = resp.bytes_stream();

        body_tx
            .send(Ok(Bytes::from("chunk1")))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Read one chunk then drop the stream (disconnect)
        let _chunk = stream.next().await;
    }

    // Continue uploading — should not fail
    body_tx
        .send(Ok(Bytes::from("chunk2")))
        .await
        .unwrap();
    body_tx
        .send(Ok(Bytes::from("chunk3")))
        .await
        .unwrap();
    drop(body_tx);

    let put_status = put_handle.await.unwrap();
    assert_eq!(put_status, 200);

    // Second reader should get all the data
    let resp = Client::new()
        .get(format!("{base3}/reader-dc"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "chunk1chunk2chunk3");
}

// --- Concurrent PUTs to different keys ---

#[tokio::test]
async fn many_concurrent_puts_no_interference() {
    let srv = spawn_server().await;
    let base = Arc::new(base_url(&srv));

    let mut put_handles = Vec::new();

    for i in 0..20 {
        let base = base.clone();

        put_handles.push(tokio::spawn(async move {
            let data: Vec<u8> = (0..1000).map(|j| ((i * 7 + j) % 256) as u8).collect();

            Client::new()
                .put(format!("{base}/concurrent-{i}"))
                .body(data)
                .send()
                .await
                .unwrap();

            i
        }));
    }

    for handle in put_handles {
        handle.await.unwrap();
    }

    // Verify all keys have correct data
    let mut get_handles = Vec::new();

    for i in 0..20u32 {
        let base = base.clone();

        get_handles.push(tokio::spawn(async move {
            let resp = Client::new()
                .get(format!("{base}/concurrent-{i}"))
                .send()
                .await
                .unwrap();

            assert_eq!(resp.status(), 200);
            let received = resp.bytes().await.unwrap();
            let expected: Vec<u8> = (0..1000).map(|j| ((i * 7 + j) % 256) as u8).collect();
            assert_eq!(received.as_ref(), expected.as_slice(), "key concurrent-{i} mismatch");
        }));
    }

    for handle in get_handles {
        handle.await.unwrap();
    }
}

// --- Empty multipart ---

#[tokio::test]
async fn empty_multipart_returns_400() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    // Craft a multipart body with no fields
    let boundary = "----emptyboundary";
    let body = b"------emptyboundary--\r\n";

    let resp = client
        .put(format!("{base}/empty-mp"))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body.to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    assert!(resp.text().await.unwrap().contains("no fields"));
}

// --- HEAD and DELETE return 405 ---

#[tokio::test]
async fn unsupported_methods_return_405() {
    let srv = spawn_server().await;
    let base = base_url(&srv);
    let client = Client::new();

    let resp = client
        .delete(format!("{base}/test"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 405);

    let resp = client
        .patch(format!("{base}/test"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 405);
}

// --- Binary data integrity ---

#[tokio::test]
async fn binary_data_integrity_all_byte_values() {
    // Ensure all 256 byte values survive the roundtrip without corruption.
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        spill_threshold: 0, // force disk path
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let client = Client::new();

    let data: Vec<u8> = (0..=255).collect();
    let expected = data.clone();

    client
        .put(format!("{base}/binary"))
        .body(data)
        .send()
        .await
        .unwrap();

    let resp = client.get(format!("{base}/binary")).send().await.unwrap();
    let received = resp.bytes().await.unwrap();
    assert_eq!(received.as_ref(), expected.as_slice());

    // Also test memory path
    let srv2 = spawn_server().await;
    let base2 = base_url(&srv2);

    let data: Vec<u8> = (0..=255).collect();

    client
        .put(format!("{base2}/binary-mem"))
        .body(data.clone())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base2}/binary-mem"))
        .send()
        .await
        .unwrap();

    let received = resp.bytes().await.unwrap();
    assert_eq!(received.as_ref(), data.as_slice());
}

// --- Multipart large file through disk spill ---

#[tokio::test]
async fn multipart_large_file_spills_to_disk() {
    let srv = file_pipe::start_server(file_pipe::ServerConfig {
        addr: "127.0.0.1:0".into(),
        spill_threshold: 512,
        ..Default::default()
    })
    .await
    .unwrap();
    let base = base_url(&srv);
    let client = Client::new();

    // 10KB patterned data via multipart
    let data: Vec<u8> = (0..10_000).map(|i| (i % 199) as u8).collect();
    let expected = data.clone();

    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(data)
            .file_name("big.bin")
            .mime_str("application/octet-stream")
            .unwrap(),
    );

    let resp = client
        .put(format!("{base}/mp-big"))
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let resp = client
        .get(format!("{base}/mp-big"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "application/octet-stream"
    );
    assert_eq!(
        resp.headers()
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap(),
        "attachment; filename=\"big.bin\""
    );

    let received = resp.bytes().await.unwrap();
    assert_eq!(received.len(), expected.len());
    assert_eq!(received.as_ref(), expected.as_slice());
}
