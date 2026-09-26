use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use harborlookout_contracts::{
    AnalysisStatusRequest, LocalDetectorConfig, PullAnalysisEventsResponse, StartAnalysisRequest,
    StartAnalysisResponse,
    CreateSurveillanceEventRequest,
    ListEventArtifactsRequest,
    ListRecordingSegmentsRequest, RecordingStatusRequest, RecordingStatusResponse, RegisterCameraRequest,
    RegisterCameraRecordingRequest,
    ListSurveillanceEventsRequest,
    StopAnalysisRequest, StopAnalysisResponse, SyncAnalysisEventsRequest, SyncAnalysisEventsResponse,
    RestartRecordingRequest, RestartRecordingResponse, RetentionCleanupRequest, RetentionPreviewRequest,
    StartRecordingRequest, StartRecordingResponse, StopRecordingRequest, StopRecordingResponse,
    SyncRecordingSegmentsRequest, SyncRecordingSegmentsResponse,
};
use harborlookout_control_plane::HttpWorkerClient;
use harborlookout_domain::{
    ArtifactId, ArtifactKind, Camera, CameraId, CameraStream, EventArtifact, EventId, EventKind,
    EventSeverity, RtspSource, RtspTransport, StreamProfile, StreamRole, SurveillanceEvent,
    WorkerState,
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
    analyses: Arc<Mutex<HashMap<String, FakeAnalysisSession>>>,
}

#[derive(Clone)]
struct FakeSession {
    pid: u32,
    output_hint: String,
}

#[derive(Clone)]
struct FakeAnalysisSession {
    detector_program: String,
    event_kind: EventKind,
    event_severity: EventSeverity,
    message: String,
    artifacts_directory: Option<String>,
    emitted_events: usize,
    last_event_at_unix_ms: Option<u64>,
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
            start_recording: None,
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
            external_recording_lease: None,
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

    let cleanup_response: serde_json::Value = post_json(
        server_addr,
        "/api/storage/retention-cleanup",
        &RetentionCleanupRequest {
            retain_after_unix_ms: u64::MAX,
        },
    )
    .await?;
    assert_eq!(cleanup_response["deleted_segments"], 2);

    let summary_after_cleanup: serde_json::Value = get_json(server_addr, "/api/storage/summary").await?;
    assert_eq!(summary_after_cleanup["total_segments"], 0);

    server_task.abort();
    worker_task.abort();
    let _ = fs::remove_dir_all(&temp_root);
    Ok(())
}

#[tokio::test]
async fn register_camera_can_optionally_start_recording() -> Result<()> {
    let temp_root = std::env::temp_dir().join(format!(
        "harborlookout-register-start-{}-{}",
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
    let camera = sample_camera();
    let register_response: serde_json::Value = post_json(
        server_addr,
        "/api/cameras",
        &RegisterCameraRequest {
            camera: camera.clone(),
            start_recording: Some(RegisterCameraRecordingRequest {
                output_directory: output_directory.to_string_lossy().to_string(),
                external_recording_lease: None,
            }),
        },
    )
    .await?;

    assert_eq!(register_response["camera_id"], "cam-smoke");
    assert_eq!(register_response["recording"]["pid"], 7001);

    let status_response: RecordingStatusResponse = post_json(
        server_addr,
        "/api/recordings/status",
        &RecordingStatusRequest {
            camera_id: camera.id.clone(),
        },
    )
    .await?;
    assert_eq!(status_response.state, WorkerState::Running);

    server_task.abort();
    worker_task.abort();
    let _ = fs::remove_dir_all(&temp_root);
    Ok(())
}

#[tokio::test]
async fn create_and_query_surveillance_events() -> Result<()> {
    let temp_root = std::env::temp_dir().join(format!(
        "harborlookout-events-{}-{}",
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

    let create_response: serde_json::Value = post_json(
        server_addr,
        "/api/events",
        &CreateSurveillanceEventRequest {
            event: SurveillanceEvent {
                id: EventId("event-garage-1".into()),
                camera_id: CameraId("cam-garage".into()),
                kind: EventKind::PackageDetected,
                severity: EventSeverity::Warning,
                occurred_at_unix_ms: 1_500,
                message: "package detected near garage door".into(),
            },
            artifacts: vec![EventArtifact {
                id: ArtifactId("artifact-garage-1".into()),
                event_id: EventId("event-garage-1".into()),
                camera_id: CameraId("cam-garage".into()),
                kind: ArtifactKind::Keyframe,
                path: "artifacts/event-garage-1/frame.jpg".into(),
                mime_type: Some("image/jpeg".into()),
                created_at_unix_ms: 1_510,
                started_at_unix_ms: Some(1_500),
                ended_at_unix_ms: Some(1_510),
                size_bytes: 2_048,
            }],
        },
    )
    .await?;
    assert_eq!(create_response["event_id"], "event-garage-1");
    assert_eq!(create_response["stored_artifacts"], 1);

    let events_response: serde_json::Value = post_json(
        server_addr,
        "/api/events/query",
        &ListSurveillanceEventsRequest {
            camera_id: Some(CameraId("cam-garage".into())),
            occurred_after_unix_ms: Some(1_000),
            occurred_before_unix_ms: Some(2_000),
        },
    )
    .await?;
    assert_eq!(events_response["events"].as_array().unwrap().len(), 1);
    assert_eq!(events_response["events"][0]["message"], "package detected near garage door");

    let artifacts_response: serde_json::Value = post_json(
        server_addr,
        "/api/events/artifacts/query",
        &ListEventArtifactsRequest {
            event_id: EventId("event-garage-1".into()),
        },
    )
    .await?;
    assert_eq!(artifacts_response["artifacts"].as_array().unwrap().len(), 1);
    assert_eq!(artifacts_response["artifacts"][0]["kind"], "Keyframe");

    server_task.abort();
    worker_task.abort();
    let _ = fs::remove_dir_all(&temp_root);
    Ok(())
}

#[tokio::test]
async fn analysis_sync_persists_mock_worker_events() -> Result<()> {
    let temp_root = std::env::temp_dir().join(format!(
        "harborlookout-analysis-sync-{}-{}",
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

    let artifacts_directory = temp_root.join("artifacts");
    let camera = sample_camera();
    let start_response: StartAnalysisResponse = post_json(
        server_addr,
        "/api/analysis/start",
        &StartAnalysisRequest {
            camera: camera.clone(),
            interval_ms: 1_000,
            event_kind: EventKind::PackageDetected,
            event_severity: EventSeverity::Warning,
            message: Some("package detected by mock worker".into()),
            artifacts_directory: Some(artifacts_directory.to_string_lossy().to_string()),
            detector: LocalDetectorConfig {
                program: "fake-detector".into(),
                args: vec!["{frame_path}".into()],
            },
        },
    )
    .await?;
    assert_eq!(start_response.camera_id, "cam-smoke");

    let status_response: serde_json::Value = post_json(
        server_addr,
        "/api/analysis/status",
        &AnalysisStatusRequest {
            camera_id: camera.id.clone(),
        },
    )
    .await?;
    assert_eq!(status_response["state"], "Running");

    let sync_response: SyncAnalysisEventsResponse = post_json(
        server_addr,
        "/api/analysis/events/sync",
        &SyncAnalysisEventsRequest {
            camera_id: camera.id.clone(),
            max_events: Some(2),
        },
    )
    .await?;
    assert_eq!(sync_response.stored_events, 1);
    assert_eq!(sync_response.stored_artifacts, 1);

    let events_response: serde_json::Value = post_json(
        server_addr,
        "/api/events/query",
        &ListSurveillanceEventsRequest {
            camera_id: Some(camera.id.clone()),
            occurred_after_unix_ms: None,
            occurred_before_unix_ms: None,
        },
    )
    .await?;
    assert_eq!(events_response["events"].as_array().unwrap().len(), 1);
    assert_eq!(events_response["events"][0]["kind"], "PackageDetected");

    let event_id = events_response["events"][0]["id"]
        .as_str()
        .expect("event id should be present")
        .to_string();
    let artifacts_response: serde_json::Value = post_json(
        server_addr,
        "/api/events/artifacts/query",
        &ListEventArtifactsRequest {
            event_id: EventId(event_id),
        },
    )
    .await?;
    assert_eq!(artifacts_response["artifacts"].as_array().unwrap().len(), 1);

    let stop_response: StopAnalysisResponse = post_json(
        server_addr,
        "/api/analysis/stop",
        &StopAnalysisRequest {
            camera_id: camera.id.clone(),
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
        .route("/api/analysis/start", post(fake_start_analysis))
        .route("/api/analysis/stop", post(fake_stop_analysis))
        .route("/api/analysis/status", post(fake_analysis_status))
        .route("/api/analysis/events/pull", post(fake_pull_analysis_events))
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
            external_recording_lease: request.external_recording_lease.clone(),
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
            exit_code: None,
            last_error: None,
        })
    } else {
        Json(RecordingStatusResponse {
            camera_id: request.camera_id.0,
            state: WorkerState::Stopped,
            pid: None,
            output_hint: None,
            exit_code: None,
            last_error: None,
        })
    }
}

async fn fake_start_analysis(
    State(state): State<FakeWorkerState>,
    Json(request): Json<StartAnalysisRequest>,
) -> Json<StartAnalysisResponse> {
    state.analyses.lock().await.insert(
        request.camera.id.0.clone(),
        FakeAnalysisSession {
            detector_program: request.detector.program.clone(),
            event_kind: request.event_kind.clone(),
            event_severity: request.event_severity.clone(),
            message: request
                .message
                .unwrap_or_else(|| "mock worker emitted analysis event".into()),
            artifacts_directory: request.artifacts_directory.clone(),
            emitted_events: 0,
            last_event_at_unix_ms: None,
        },
    );

    Json(StartAnalysisResponse {
        camera_id: request.camera.id.0,
        worker_name: "fake-worker".into(),
        state: WorkerState::Running,
        interval_ms: request.interval_ms,
        event_kind: request.event_kind,
        event_severity: request.event_severity,
        artifacts_directory: request.artifacts_directory,
        detector_program: request.detector.program,
    })
}

async fn fake_stop_analysis(
    State(state): State<FakeWorkerState>,
    Json(request): Json<StopAnalysisRequest>,
) -> Json<StopAnalysisResponse> {
    let stopped = state.analyses.lock().await.remove(&request.camera_id.0).is_some();
    Json(StopAnalysisResponse {
        camera_id: request.camera_id.0,
        stopped,
    })
}

async fn fake_analysis_status(
    State(state): State<FakeWorkerState>,
    Json(request): Json<AnalysisStatusRequest>,
) -> Json<harborlookout_contracts::AnalysisStatusResponse> {
    let analyses = state.analyses.lock().await;
    if let Some(session) = analyses.get(&request.camera_id.0) {
        Json(harborlookout_contracts::AnalysisStatusResponse {
            camera_id: request.camera_id.0,
            state: WorkerState::Running,
            event_kind: Some(session.event_kind.clone()),
            event_severity: Some(session.event_severity.clone()),
            interval_ms: Some(1_000),
            detector_program: Some(session.detector_program.clone()),
            emitted_events: session.emitted_events,
            pending_events: 0,
            last_event_at_unix_ms: session.last_event_at_unix_ms,
            last_error: None,
        })
    } else {
        Json(harborlookout_contracts::AnalysisStatusResponse {
            camera_id: request.camera_id.0,
            state: WorkerState::Stopped,
            event_kind: None,
            event_severity: None,
            interval_ms: None,
            detector_program: None,
            emitted_events: 0,
            pending_events: 0,
            last_event_at_unix_ms: None,
            last_error: None,
        })
    }
}

async fn fake_pull_analysis_events(
    State(state): State<FakeWorkerState>,
    Json(request): Json<SyncAnalysisEventsRequest>,
) -> Json<PullAnalysisEventsResponse> {
    let mut analyses = state.analyses.lock().await;
    let Some(session) = analyses.get_mut(&request.camera_id.0) else {
        return Json(PullAnalysisEventsResponse {
            camera_id: request.camera_id.0,
            events: Vec::new(),
            last_event_at_unix_ms: None,
        });
    };

    session.emitted_events += 1;
    let occurred_at_unix_ms = 10_000 + session.emitted_events as u64;
    session.last_event_at_unix_ms = Some(occurred_at_unix_ms);
    let event_id = format!("analysis-{}-{:06}", request.camera_id.0, session.emitted_events);
    let artifact = session.artifacts_directory.as_ref().map(|directory| EventArtifact {
        id: ArtifactId(format!("artifact-{event_id}")),
        event_id: EventId(event_id.clone()),
        camera_id: request.camera_id.clone(),
        kind: ArtifactKind::Keyframe,
        path: format!("{}/{}/{}-keyframe.jpg", directory.replace('\\', "/"), request.camera_id.0, event_id),
        mime_type: Some("image/jpeg".into()),
        created_at_unix_ms: occurred_at_unix_ms,
        started_at_unix_ms: Some(occurred_at_unix_ms),
        ended_at_unix_ms: Some(occurred_at_unix_ms),
        size_bytes: 128,
    });

    Json(PullAnalysisEventsResponse {
        camera_id: request.camera_id.0.clone(),
        events: vec![harborlookout_contracts::PendingAnalysisEvent {
            event: SurveillanceEvent {
                id: EventId(event_id.clone()),
                camera_id: request.camera_id.clone(),
                kind: session.event_kind.clone(),
                severity: session.event_severity.clone(),
                occurred_at_unix_ms,
                message: session.message.clone(),
            },
            artifacts: artifact.into_iter().collect(),
        }],
        last_event_at_unix_ms: session.last_event_at_unix_ms,
    })
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
