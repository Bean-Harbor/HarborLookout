use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use harborlookout_contracts::{
    ListRecordingSegmentsRequest, RecordingStatusRequest, RecordingStatusResponse, RegisterCameraRequest,
    RestartRecordingRequest, RestartRecordingResponse, RetentionPreviewRequest, StartRecordingRequest,
    StartRecordingResponse, StopRecordingRequest, StopRecordingResponse, SyncRecordingSegmentsRequest,
    SyncRecordingSegmentsResponse,
};
use harborlookout_control_plane::HttpWorkerClient;
use harborlookout_domain::{
    Camera, CameraId, CameraStream, RtspSource, RtspTransport, StreamProfile, StreamRole, WorkerState,
};
use harborlookout_media_ffmpeg::FfmpegBackend;
use harborlookout_server::{build_app, AppState, ServerControlPlane};
use harborlookout_storage::SqliteCameraStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct FakeWorkerState {
    active: Arc<Mutex<HashMap<String, FakeSession>>>,
}

#[derive(Clone)]
struct FakeSession {
    pid: u32,
    output_hint: String,
}

#[tokio::test]
async fn smoke_recording_flow_persists_segments() -> Result<()> {
    let temp_root = std::env::temp_dir().join(format!(
        "harborlookout-smoke-{}-{}",
        std::process::id(),
        chrono_like_now()
    ));
    let _ = fs::remove_dir_all(&temp_root);
    fs::create_dir_all(&temp_root)?;

    let fake_worker = FakeWorkerState::default();
    let worker_listener = TcpListener::bind("127.0.0.1:0").await?;
    let worker_addr = worker_listener.local_addr()?;
    let worker_app = build_fake_worker_app(fake_worker.clone());
    let worker_task = tokio::spawn(async move {
        axum::serve(worker_listener, worker_app).await.unwrap();
    });

    let database_path = temp_root.join("harborlookout.db");
    let store = SqliteCameraStore::open(&database_path)?;
    let control_plane = Arc::new(ServerControlPlane::new(
        FfmpegBackend,
        store,
        HttpWorkerClient::new(format!("http://{}", worker_addr)),
    ));
    let server_listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = server_listener.local_addr()?;
    let server_app = build_app(AppState { control_plane });
    let server_task = tokio::spawn(async move {
        axum::serve(server_listener, server_app).await.unwrap();
    });

    std::thread::sleep(Duration::from_millis(50));

    let output_directory = temp_root.join("segments").join("cam-smoke");
    fs::create_dir_all(&output_directory)?;
    let camera = sample_camera();

    let register_response: serde_json::Value = post_json(
        server_addr,
        "/api/cameras",
        &RegisterCameraRequest {
            camera: camera.clone(),
        },
    )
    .await?;
    assert_eq!(register_response["camera_id"], "cam-smoke");

    let start_response: StartRecordingResponse = post_json(
        server_addr,
        "/api/recordings/start",
        &StartRecordingRequest {
            camera: camera.clone(),
            output_directory: output_directory.to_string_lossy().to_string(),
        },
    )
    .await?;
    assert_eq!(start_response.pid, 7001);

    let status_response: RecordingStatusResponse = post_json(
        server_addr,
        "/api/recordings/status",
        &RecordingStatusRequest {
            camera_id: camera.id.clone(),
        },
    )
    .await?;
    assert_eq!(status_response.state, WorkerState::Running);

    let sync_response: SyncRecordingSegmentsResponse = post_json(
        server_addr,
        "/api/recordings/segments/sync",
        &SyncRecordingSegmentsRequest {
            camera_id: camera.id.clone(),
        },
    )
    .await?;
    assert_eq!(sync_response.synced_segments, 2);

    let query_response: serde_json::Value = post_json(
        server_addr,
        "/api/recordings/segments/query",
        &ListRecordingSegmentsRequest {
            camera_id: camera.id.clone(),
            started_after_unix_ms: None,
            started_before_unix_ms: None,
        },
    )
    .await?;
    assert_eq!(query_response["segments"].as_array().unwrap().len(), 2);

    let summary_response: serde_json::Value = get_json(server_addr, "/api/storage/summary").await?;
    assert_eq!(summary_response["total_segments"], 2);
    assert_eq!(summary_response["active_recordings"], 1);

    let preview_response: serde_json::Value = post_json(
        server_addr,
        "/api/storage/retention-preview",
        &RetentionPreviewRequest {
            retain_after_unix_ms: u64::MAX,
        },
    )
    .await?;
    assert_eq!(preview_response["reclaimable_segments"], 2);

    let stop_response: StopRecordingResponse = post_json(
        server_addr,
        "/api/recordings/stop",
        &StopRecordingRequest {
            camera_id: CameraId("cam-smoke".into()),
        },
    )
    .await?;
    assert!(stop_response.stopped);

    server_task.abort();
    worker_task.abort();
    let _ = fs::remove_dir_all(&temp_root);
    Ok(())
}

fn build_fake_worker_app(state: FakeWorkerState) -> Router {
    Router::new()
        .route("/api/recordings/start", post(fake_start_recording))
        .route("/api/recordings/stop", post(fake_stop_recording))
        .route("/api/recordings/restart", post(fake_restart_recording))
        .route("/api/recordings/status", post(fake_recording_status))
        .with_state(state)
}

async fn fake_start_recording(
    State(state): State<FakeWorkerState>,
    Json(request): Json<StartRecordingRequest>,
) -> Json<StartRecordingResponse> {
    let output_hint = format!(
        "{}/{}-%06d.mp4",
        request.output_directory.replace('\\', "/"),
        request.camera.id.0
    );
    let first_segment = format!("{}/{}-000001.mp4", request.output_directory.replace('\\', "/"), request.camera.id.0);
    let second_segment = format!("{}/{}-000002.mp4", request.output_directory.replace('\\', "/"), request.camera.id.0);
    fs::create_dir_all(&request.output_directory).unwrap();
    fs::write(first_segment, b"segment-one").unwrap();
    fs::write(second_segment, b"segment-two").unwrap();

    state.active.lock().await.insert(
        request.camera.id.0.clone(),
        FakeSession {
            pid: 7001,
            output_hint: output_hint.clone(),
        },
    );

    Json(StartRecordingResponse {
        backend: "fake-worker".into(),
        program: "fake-ffmpeg".into(),
        args: vec![request.camera.id.0.clone()],
        output_hint,
        pid: 7001,
    })
}

async fn fake_stop_recording(
    State(state): State<FakeWorkerState>,
    Json(request): Json<StopRecordingRequest>,
) -> Json<StopRecordingResponse> {
    let removed = state.active.lock().await.remove(&request.camera_id.0).is_some();
    Json(StopRecordingResponse {
        camera_id: request.camera_id.0,
        stopped: removed,
    })
}

async fn fake_restart_recording(
    State(state): State<FakeWorkerState>,
    Json(request): Json<RestartRecordingRequest>,
) -> Json<RestartRecordingResponse> {
    let start = fake_start_recording(
        State(state),
        Json(StartRecordingRequest {
            camera: request.camera.clone(),
            output_directory: request.output_directory.clone(),
        }),
    )
    .await
    .0;

    Json(RestartRecordingResponse {
        camera_id: request.camera.id.0,
        restarted: true,
        backend: start.backend,
        program: start.program,
        args: start.args,
        output_hint: start.output_hint,
        pid: start.pid,
    })
}

async fn fake_recording_status(
    State(state): State<FakeWorkerState>,
    Json(request): Json<RecordingStatusRequest>,
) -> Json<RecordingStatusResponse> {
    let active = state.active.lock().await;
    if let Some(session) = active.get(&request.camera_id.0) {
        Json(RecordingStatusResponse {
            camera_id: request.camera_id.0,
            state: WorkerState::Running,
            pid: Some(session.pid),
            output_hint: Some(session.output_hint.clone()),
        })
    } else {
        Json(RecordingStatusResponse {
            camera_id: request.camera_id.0,
            state: WorkerState::Stopped,
            pid: None,
            output_hint: None,
        })
    }
}

async fn post_json<TRequest, TResponse>(
    address: std::net::SocketAddr,
    path: &str,
    request: &TRequest,
) -> Result<TResponse>
where
    TRequest: serde::Serialize + ?Sized,
    TResponse: serde::de::DeserializeOwned,
{
    let body = serde_json::to_string(request)?;
    let mut stream = TcpStream::connect(address).await?;
    let request_text = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body,
        host = address.ip(),
        port = address.port(),
    );
    stream.write_all(request_text.as_bytes()).await?;

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).await?;
    let response = String::from_utf8(response_bytes)?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .or_else(|| response.split_once("\n\n"))
        .ok_or_else(|| anyhow::anyhow!("invalid response: {response}"))?;
    let status_line = head.lines().next().ok_or_else(|| anyhow::anyhow!("missing status line"))?;
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("missing status code"))?
        .parse()?;
    if status_code >= 400 {
        return Err(anyhow::anyhow!("request failed: {status_code} {body}"));
    }
    Ok(serde_json::from_str(body)?)
}

async fn get_json<TResponse>(address: std::net::SocketAddr, path: &str) -> Result<TResponse>
where
    TResponse: serde::de::DeserializeOwned,
{
    let mut stream = TcpStream::connect(address).await?;
    let request_text = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n",
        host = address.ip(),
        port = address.port(),
    );
    stream.write_all(request_text.as_bytes()).await?;

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).await?;
    let response = String::from_utf8(response_bytes)?;
    let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| anyhow::anyhow!("invalid response: {response}"))?;
    let status_code: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| anyhow::anyhow!("missing status code"))?
        .parse()?;
    if status_code >= 400 {
        return Err(anyhow::anyhow!("request failed: {status_code} {body}"));
    }
    Ok(serde_json::from_str(body)?)
}

fn sample_camera() -> Camera {
    Camera {
        id: CameraId("cam-smoke".into()),
        name: "Smoke".into(),
        streams: vec![CameraStream {
            role: StreamRole::Record,
            source: RtspSource {
                url: "rtsp://camera.local/smoke".into(),
                transport: RtspTransport::Tcp,
            },
            stream_profile: StreamProfile {
                width: 1280,
                height: 720,
                fps: 15,
                segment_seconds: 10,
            },
        }],
        source: RtspSource {
            url: "rtsp://camera.local/smoke".into(),
            transport: RtspTransport::Tcp,
        },
        stream_profile: StreamProfile {
            width: 1280,
            height: 720,
            fps: 15,
            segment_seconds: 10,
        },
        enabled: true,
    }
}

fn chrono_like_now() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_millis()
}