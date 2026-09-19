use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use harborlookout_contracts::{
    AnalysisStatusRequest, AnalysisStatusResponse, BuildRecordingPlanRequest, BuildRecordingPlanResponse,
    LocalDetectorConfig, PendingAnalysisEvent, PullAnalysisEventsResponse, RecordingStatusRequest,
    RecordingStatusResponse, RestartRecordingRequest, RestartRecordingResponse, StartAnalysisRequest,
    StartAnalysisResponse, StartRecordingRequest, StartRecordingResponse, StopAnalysisRequest,
    StopAnalysisResponse, StopRecordingRequest, StopRecordingResponse, SyncAnalysisEventsRequest,
    RecordingWriterSegmentCompleteRequest, RecordingWriterSegmentStartRequest,
    RecordingWriterSegmentWriteRequest,
};
use harborlookout_domain::{
    ArtifactId, ArtifactKind, EventId, SurveillanceEvent, WorkerState as RecordingWorkerState,
};
use harborlookout_media_ffmpeg::FfmpegBackend;
use harborlookout_media_core::MediaBackend;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, oneshot};
use tracing::info;

mod recording_writer;

struct ManagedRecording {
    pid: u32,
    output_hint: String,
    child: Child,
    last_exit_code: Option<i32>,
    last_error: Option<String>,
}

struct ManagedExternalRecording {
    pid: u32,
    output_hint: String,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<()>>,
}

struct ManagedAnalysis {
    camera: harborlookout_domain::Camera,
    interval_ms: u64,
    event_kind: harborlookout_domain::EventKind,
    event_severity: harborlookout_domain::EventSeverity,
    message: String,
    detector: LocalDetectorConfig,
    artifacts_directory: Option<String>,
    emitted_events: usize,
    pending_events: Vec<PendingAnalysisEvent>,
    last_generated_at_unix_ms: u64,
    last_event_at_unix_ms: Option<u64>,
    last_error: Option<String>,
}

#[derive(Clone)]
struct WorkerAppState {
    backend: FfmpegBackend,
    sessions: Arc<Mutex<HashMap<String, ManagedRecording>>>,
    external_sessions: Arc<Mutex<HashMap<String, ManagedExternalRecording>>>,
    analyses: Arc<Mutex<HashMap<String, ManagedAnalysis>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let state = WorkerAppState {
        backend: FfmpegBackend,
        sessions: Arc::new(Mutex::new(HashMap::new())),
        external_sessions: Arc::new(Mutex::new(HashMap::new())),
        analyses: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/recordings/plan", post(build_recording_plan))
        .route("/api/recordings/start", post(start_recording))
        .route("/api/recordings/stop", post(stop_recording))
        .route("/api/recordings/restart", post(restart_recording))
        .route("/api/recordings/status", post(recording_status))
        .route("/api/analysis/start", post(start_analysis))
        .route("/api/analysis/stop", post(stop_analysis))
        .route("/api/analysis/status", post(analysis_status))
        .route("/api/analysis/events/pull", post(pull_analysis_events))
        .with_state(state);

    let bind_address = std::env::var("HARBORLOOKOUT_WORKER_BIND")
        .unwrap_or_else(|_| "127.0.0.1:5050".to_string());
    let listener = TcpListener::bind(&bind_address).await?;
    info!(address = %listener.local_addr()?, "HarborLookout worker listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<WorkerAppState>) -> Json<serde_json::Value> {
    let active_sessions = state.sessions.lock().await.len();
    let active_analyses = state.analyses.lock().await.len();
    Json(serde_json::json!({
        "status": "ok",
        "backend": state.backend.name(),
        "active_sessions": active_sessions,
        "active_analyses": active_analyses,
    }))
}

async fn build_recording_plan(
    State(state): State<WorkerAppState>,
    Json(request): Json<BuildRecordingPlanRequest>,
) -> Result<Json<BuildRecordingPlanResponse>, (StatusCode, String)> {
    let plan = state
        .backend
        .build_recording_plan(&request.camera, &request.output_directory)
        .map_err(bad_request)?;

    Ok(Json(BuildRecordingPlanResponse {
        backend: state.backend.name().to_string(),
        program: plan.program_string(),
        args: plan.args,
        output_hint: plan.output_hint,
    }))
}

async fn start_external_recording(
    State(state): State<WorkerAppState>,
    camera: harborlookout_domain::Camera,
    lease: harborlookout_domain::ExternalRecordingLease,
) -> Result<Json<StartRecordingResponse>, (StatusCode, String)> {
    lease
        .validate(now_unix_ms())
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let camera_id = camera.id.0.clone();
    ensure_not_running(&state, &camera_id).await?;

    let socket_path = std::env::var("HARBOROS_RECORDING_WRITER_SOCKET")
        .unwrap_or_else(|_| "/run/harboros/recording-writer.sock".to_string());
    let client = recording_writer::RecordingWriterClient::new(socket_path);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (stop_tx, stop_rx) = oneshot::channel();
    let backend = state.backend.clone();
    let task_camera = camera.clone();
    let task_lease = lease.clone();
    let task = tokio::spawn(async move {
        run_external_recording(&backend, &task_camera, &task_lease, client, stop_rx, ready_tx).await
    });
    let (program, args, output_hint, pid) = ready_rx.await.map_err(|_| {
        (
            StatusCode::BAD_GATEWAY,
            "external recording failed before the first segment started".to_string(),
        )
    })?;
    state.external_sessions.lock().await.insert(
        camera_id.clone(),
        ManagedExternalRecording {
            pid,
            output_hint: output_hint.clone(),
            stop: Some(stop_tx),
            task,
        },
    );
    Ok(Json(StartRecordingResponse {
        backend: "ffmpeg".into(),
        program,
        args,
        output_hint,
        pid,
    }))
}

async fn run_external_recording(
    backend: &FfmpegBackend,
    camera: &harborlookout_domain::Camera,
    lease: &harborlookout_domain::ExternalRecordingLease,
    client: recording_writer::RecordingWriterClient,
    mut stop_rx: oneshot::Receiver<()>,
    ready_tx: oneshot::Sender<(String, Vec<String>, String, u32)>,
) -> Result<()> {
    client
        .resolve(format!("lookout-resolve-{}", now_unix_ms()), lease.source_token.clone())
        .await?;
    let mut ready_tx = Some(ready_tx);
    let safe_camera_id = camera
        .id
        .0
        .chars()
        .map(|character| if character.is_ascii_alphanumeric() || character == '-' || character == '_' { character } else { '-' })
        .collect::<String>();
    for sequence in 0u64.. {
        client
            .renew(format!("lookout-renew-{sequence}-{}", now_unix_ms()), lease.source_token.clone())
            .await?;
        let plan = backend.build_external_segment_plan(camera)?;
        let mut command = Command::new(&plan.program);
        command
            .args(&plan.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command.spawn()?;
        let pid = child.id().ok_or_else(|| anyhow::anyhow!("external ffmpeg process has no pid"))?;
        let mut stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("external ffmpeg stdout is unavailable"))?;
        let segment_id = format!("external-{safe_camera_id}-{sequence}");
        if let Err(error) = client
            .start(
                format!("lookout-start-{sequence}-{}", now_unix_ms()),
                RecordingWriterSegmentStartRequest {
                    lease_ref: lease.source_token.clone(),
                    segment_id: segment_id.clone(),
                    sequence,
                    started_at: None,
                },
            )
            .await
        {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(error);
        }
        if let Some(sender) = ready_tx.take() {
            let _ = sender.send((plan.program.clone(), plan.args.clone(), plan.output_hint.clone(), pid));
        }
        let mut digest = Sha256::new();
        let mut bytes_done = 0u64;
        let mut buffer = vec![0u8; 768 * 1024];
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    let _ = client.complete(
                        format!("lookout-interrupted-{sequence}-{}", now_unix_ms()),
                        RecordingWriterSegmentCompleteRequest {
                            lease_ref: lease.source_token.clone(),
                            segment_id: segment_id.clone(),
                            state: "interrupted".into(),
                            bytes_done: Some(bytes_done),
                            hash_sha256: Some(digest_hex(&digest)),
                            error_code: Some("RECORDING_STOPPED".into()),
                        },
                    ).await;
                    return Ok(());
                }
                read = stdout.read(&mut buffer) => {
                    let count = read?;
                    if count == 0 {
                        break;
                    }
                    digest.update(&buffer[..count]);
                    bytes_done = bytes_done.saturating_add(count as u64);
                    client.write(
                        format!("lookout-write-{sequence}-{bytes_done}"),
                        RecordingWriterSegmentWriteRequest {
                            lease_ref: lease.source_token.clone(),
                            segment_id: segment_id.clone(),
                            chunk_base64: BASE64.encode(&buffer[..count]),
                        },
                    ).await?;
                }
            }
        }
        let status = child.wait().await?;
        if !status.success() {
            let _ = client.complete(
                format!("lookout-failed-{sequence}-{}", now_unix_ms()),
                RecordingWriterSegmentCompleteRequest {
                    lease_ref: lease.source_token.clone(),
                    segment_id,
                    state: "failed".into(),
                    bytes_done: Some(bytes_done),
                    hash_sha256: Some(digest_hex(&digest)),
                    error_code: Some("FFMPEG_EXIT_FAILED".into()),
                },
            ).await;
            bail!("external ffmpeg exited unsuccessfully");
        }
        client
            .complete(
                format!("lookout-complete-{sequence}-{}", now_unix_ms()),
                RecordingWriterSegmentCompleteRequest {
                    lease_ref: lease.source_token.clone(),
                    segment_id,
                    state: "sealed".into(),
                    bytes_done: Some(bytes_done),
                    hash_sha256: Some(digest_hex(&digest)),
                    error_code: None,
                },
            )
            .await?;
    }
    unreachable!()
}

async fn start_recording(
    State(state): State<WorkerAppState>,
    Json(request): Json<StartRecordingRequest>,
) -> Result<Json<StartRecordingResponse>, (StatusCode, String)> {
    if let Some(lease) = request.external_recording_lease.clone() {
        return start_external_recording(State(state), request.camera, lease).await;
    }
    reject_external_recording_lease(request.external_recording_lease.as_ref())?;
    let camera_id = request.camera.id.0.clone();
    ensure_not_running(&state, &camera_id).await?;

    let plan = state
        .backend
        .build_recording_plan(&request.camera, &request.output_directory)
        .map_err(bad_request)?;

    fs::create_dir_all(&request.output_directory).map_err(internal_error)?;

    let program = plan.program_string();
    let args = plan.args.clone();
    let output_hint = plan.output_hint.clone();

    let mut command = Command::new(plan.program);
    command.args(&plan.args);
    command.stdin(Stdio::null());
    command.stdout(Stdio::null());
    command.stderr(Stdio::null());

    let child = command.spawn().map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed to spawn recording process `{program}` for camera {camera_id}: {error}"
            ),
        )
    })?;
    let pid = child.id().ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "spawned ffmpeg process has no pid".to_string()))?;

    state.sessions.lock().await.insert(
        camera_id,
        ManagedRecording {
            pid,
            output_hint: output_hint.clone(),
            child,
            last_exit_code: None,
            last_error: None,
        },
    );

    Ok(Json(StartRecordingResponse {
        backend: state.backend.name().to_string(),
        program,
        args,
        output_hint,
        pid,
    }))
}

async fn stop_recording(
    State(state): State<WorkerAppState>,
    Json(request): Json<StopRecordingRequest>,
) -> Result<Json<StopRecordingResponse>, (StatusCode, String)> {
    if let Some(mut managed) = state.external_sessions.lock().await.remove(&request.camera_id.0) {
        if let Some(stop) = managed.stop.take() {
            let _ = stop.send(());
        }
        let _ = managed.task.await;
        return Ok(Json(StopRecordingResponse {
            camera_id: request.camera_id.0,
            stopped: true,
        }));
    }
    let mut sessions = state.sessions.lock().await;
    let Some(mut managed) = sessions.remove(&request.camera_id.0) else {
        return Ok(Json(StopRecordingResponse {
            camera_id: request.camera_id.0,
            stopped: false,
        }));
    };
    drop(sessions);

    if managed.child.try_wait().map_err(internal_error)?.is_none() {
        managed.child.start_kill().map_err(internal_error)?;
        let _ = managed.child.wait().await.map_err(internal_error)?;
    }

    Ok(Json(StopRecordingResponse {
        camera_id: request.camera_id.0,
        stopped: true,
    }))
}

async fn restart_recording(
    State(state): State<WorkerAppState>,
    Json(request): Json<RestartRecordingRequest>,
) -> Result<Json<RestartRecordingResponse>, (StatusCode, String)> {
    if let Some(lease) = request.external_recording_lease.clone() {
        let stop_request = StopRecordingRequest { camera_id: request.camera.id.clone() };
        let _ = stop_recording(State(state.clone()), Json(stop_request)).await?;
        let start_response = start_external_recording(State(state.clone()), request.camera.clone(), lease).await?.0;
        return Ok(Json(RestartRecordingResponse {
            camera_id: request.camera.id.0,
            restarted: true,
            backend: start_response.backend,
            program: start_response.program,
            args: start_response.args,
            output_hint: start_response.output_hint,
            pid: start_response.pid,
        }));
    }
    reject_external_recording_lease(request.external_recording_lease.as_ref())?;
    let stop_request = StopRecordingRequest {
        camera_id: request.camera.id.clone(),
    };
    let _ = stop_recording(State(state.clone()), Json(stop_request)).await?;

    let start_response = start_recording(
        State(state.clone()),
        Json(StartRecordingRequest {
            camera: request.camera.clone(),
            output_directory: request.output_directory.clone(),
            external_recording_lease: request.external_recording_lease.clone(),
        }),
    )
    .await?
    .0;

    Ok(Json(RestartRecordingResponse {
        camera_id: request.camera.id.0,
        restarted: true,
        backend: start_response.backend,
        program: start_response.program,
        args: start_response.args,
        output_hint: start_response.output_hint,
        pid: start_response.pid,
    }))
}

async fn recording_status(
    State(state): State<WorkerAppState>,
    Json(request): Json<RecordingStatusRequest>,
) -> Result<Json<RecordingStatusResponse>, (StatusCode, String)> {
    let mut external_sessions = state.external_sessions.lock().await;
    if external_sessions.contains_key(&request.camera_id.0) {
        let finished = external_sessions
            .get(&request.camera_id.0)
            .map(|managed| managed.task.is_finished())
            .unwrap_or(false);
        if finished {
            let managed = external_sessions.remove(&request.camera_id.0).expect("external session exists");
            let result = managed.task.await;
            let (state_value, last_error) = match result {
                Ok(Ok(())) => (RecordingWorkerState::Stopped, None),
                Ok(Err(error)) => (RecordingWorkerState::Failed, Some(error.to_string())),
                Err(error) => (RecordingWorkerState::Failed, Some(error.to_string())),
            };
            return Ok(Json(RecordingStatusResponse {
                camera_id: request.camera_id.0,
                state: state_value,
                pid: Some(managed.pid),
                output_hint: Some(managed.output_hint),
                exit_code: None,
                last_error,
            }));
        }
        let managed = external_sessions.get(&request.camera_id.0).expect("external session exists");
        return Ok(Json(RecordingStatusResponse {
            camera_id: request.camera_id.0,
            state: RecordingWorkerState::Running,
            pid: Some(managed.pid),
            output_hint: Some(managed.output_hint.clone()),
            exit_code: None,
            last_error: None,
        }));
    }
    drop(external_sessions);
    let mut sessions = state.sessions.lock().await;
    let Some(managed) = sessions.get_mut(&request.camera_id.0) else {
        return Ok(Json(RecordingStatusResponse {
            camera_id: request.camera_id.0,
            state: RecordingWorkerState::Stopped,
            pid: None,
            output_hint: None,
            exit_code: None,
            last_error: None,
        }));
    };

    let mut final_state = RecordingWorkerState::Running;
    let pid = Some(managed.pid);
    let output_hint = Some(managed.output_hint.clone());
    let mut exit_code = managed.last_exit_code;
    let mut last_error = managed.last_error.clone();
    let remove = match managed.child.try_wait().map_err(internal_error)? {
        Some(exit_status) => {
            exit_code = exit_status.code();
            final_state = if exit_status.success() {
                RecordingWorkerState::Stopped
            } else {
                last_error = Some(describe_failed_exit(&request.camera_id.0, exit_status.code()));
                RecordingWorkerState::Failed
            };
            managed.last_exit_code = exit_code;
            managed.last_error = last_error.clone();
            true
        }
        None => false,
    };

    if remove {
        sessions.remove(&request.camera_id.0);
    }

    Ok(Json(RecordingStatusResponse {
        camera_id: request.camera_id.0,
        state: final_state,
        pid,
        output_hint,
        exit_code,
        last_error,
    }))
}

async fn start_analysis(
    State(state): State<WorkerAppState>,
    Json(request): Json<StartAnalysisRequest>,
) -> Result<Json<StartAnalysisResponse>, (StatusCode, String)> {
    if request.interval_ms == 0 {
        return Err((StatusCode::BAD_REQUEST, "analysis interval must be greater than zero".to_string()));
    }
    if request.detector.program.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "analysis detector program cannot be empty".to_string()));
    }

    let camera_id = request.camera.id.0.clone();
    let mut analyses = state.analyses.lock().await;
    if analyses.contains_key(&camera_id) {
        return Err((
            StatusCode::CONFLICT,
            format!("analysis for camera {camera_id} is already running"),
        ));
    }

    let now = now_unix_ms();
    analyses.insert(
        camera_id.clone(),
        ManagedAnalysis {
            camera: request.camera,
            interval_ms: request.interval_ms,
            event_kind: request.event_kind.clone(),
            event_severity: request.event_severity.clone(),
            message: request.message.clone().unwrap_or_else(|| format!("{:?} detected by local detector", request.event_kind)),
            detector: request.detector.clone(),
            artifacts_directory: request.artifacts_directory.clone(),
            emitted_events: 0,
            pending_events: Vec::new(),
            last_generated_at_unix_ms: now.saturating_sub(request.interval_ms),
            last_event_at_unix_ms: None,
            last_error: None,
        },
    );

    Ok(Json(StartAnalysisResponse {
        camera_id,
        worker_name: "harborlookout-worker".into(),
        state: RecordingWorkerState::Running,
        interval_ms: request.interval_ms,
        event_kind: request.event_kind,
        event_severity: request.event_severity,
        artifacts_directory: request.artifacts_directory,
        detector_program: request.detector.program,
    }))
}

async fn stop_analysis(
    State(state): State<WorkerAppState>,
    Json(request): Json<StopAnalysisRequest>,
) -> Result<Json<StopAnalysisResponse>, (StatusCode, String)> {
    let removed = state.analyses.lock().await.remove(&request.camera_id.0).is_some();
    Ok(Json(StopAnalysisResponse {
        camera_id: request.camera_id.0,
        stopped: removed,
    }))
}

async fn analysis_status(
    State(state): State<WorkerAppState>,
    Json(request): Json<AnalysisStatusRequest>,
) -> Result<Json<AnalysisStatusResponse>, (StatusCode, String)> {
    let analyses = state.analyses.lock().await;
    let Some(analysis) = analyses.get(&request.camera_id.0) else {
        return Ok(Json(AnalysisStatusResponse {
            camera_id: request.camera_id.0,
            state: RecordingWorkerState::Stopped,
            event_kind: None,
            event_severity: None,
            interval_ms: None,
            detector_program: None,
            emitted_events: 0,
            pending_events: 0,
            last_event_at_unix_ms: None,
            last_error: None,
        }));
    };

    Ok(Json(AnalysisStatusResponse {
        camera_id: request.camera_id.0,
        state: RecordingWorkerState::Running,
        event_kind: Some(analysis.event_kind.clone()),
        event_severity: Some(analysis.event_severity.clone()),
        interval_ms: Some(analysis.interval_ms),
        detector_program: Some(analysis.detector.program.clone()),
        emitted_events: analysis.emitted_events,
        pending_events: analysis.pending_events.len(),
        last_event_at_unix_ms: analysis.last_event_at_unix_ms,
        last_error: analysis.last_error.clone(),
    }))
}

async fn pull_analysis_events(
    State(state): State<WorkerAppState>,
    Json(request): Json<SyncAnalysisEventsRequest>,
) -> Result<Json<PullAnalysisEventsResponse>, (StatusCode, String)> {
    let max_events = request.max_events.unwrap_or(16);
    let mut analyses = state.analyses.lock().await;
    let Some(analysis) = analyses.get_mut(&request.camera_id.0) else {
        return Ok(Json(PullAnalysisEventsResponse {
            camera_id: request.camera_id.0,
            events: Vec::new(),
            last_event_at_unix_ms: None,
        }));
    };

    generate_due_analysis_events(&state.backend, analysis, max_events)
        .await
        .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;
    let events = std::mem::take(&mut analysis.pending_events);

    Ok(Json(PullAnalysisEventsResponse {
        camera_id: request.camera_id.0,
        events,
        last_event_at_unix_ms: analysis.last_event_at_unix_ms,
    }))
}

async fn ensure_not_running(state: &WorkerAppState, camera_id: &str) -> Result<(), (StatusCode, String)> {
    if state.external_sessions.lock().await.contains_key(camera_id) {
        return Err((
            StatusCode::CONFLICT,
            format!("camera {camera_id} is already recording"),
        ));
    }
    let mut sessions = state.sessions.lock().await;
    let mut remove_stale = false;

    if let Some(managed) = sessions.get_mut(camera_id) {
        if managed.child.try_wait().map_err(internal_error)?.is_none() {
            return Err((
                StatusCode::CONFLICT,
                format!("camera {camera_id} is already recording"),
            ));
        }
        remove_stale = true;
    }

    if remove_stale {
        sessions.remove(camera_id);
    }

    Ok(())
}

fn bad_request(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, error.to_string())
}

fn reject_external_recording_lease(
    lease: Option<&harborlookout_domain::ExternalRecordingLease>,
) -> Result<(), (StatusCode, String)> {
    if let Some(lease) = lease {
        lease
            .validate(now_unix_ms())
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            "external recording lease requires the HarborOS-owned recording writer".to_string(),
        ));
    }
    Ok(())
}

fn internal_error(error: std::io::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn describe_failed_exit(camera_id: &str, exit_code: Option<i32>) -> String {
    match exit_code {
        Some(code) => format!("recording process for camera {camera_id} exited with code {code}"),
        None => format!("recording process for camera {camera_id} exited without an exit code"),
    }
}

async fn generate_due_analysis_events(
    backend: &FfmpegBackend,
    analysis: &mut ManagedAnalysis,
    max_events: usize,
) -> Result<()> {
    if max_events == 0 {
        return Ok(());
    }

    let now = now_unix_ms();
    while analysis.pending_events.len() < max_events
        && now.saturating_sub(analysis.last_generated_at_unix_ms) >= analysis.interval_ms
    {
        let occurred_at_unix_ms = analysis.last_generated_at_unix_ms.saturating_add(analysis.interval_ms);
        analysis.last_generated_at_unix_ms = occurred_at_unix_ms;
        analysis.emitted_events += 1;
        analysis.last_event_at_unix_ms = Some(occurred_at_unix_ms);

        let event_id = EventId(format!("analysis-{}-{:06}", analysis.camera.id.0, analysis.emitted_events));
        let sample_path = build_sample_path(
            &analysis.camera.id.0,
            analysis.emitted_events,
            analysis.artifacts_directory.as_deref(),
        );

        capture_snapshot(backend, &analysis.camera, &sample_path).await?;
        let detection = run_local_detector(
            &analysis.detector,
            &sample_path,
            &analysis.camera.id.0,
            &analysis.event_kind,
            &analysis.event_severity,
        )
        .await?;

        if !detection.detected {
            let _ = fs::remove_file(&sample_path);
            continue;
        }

        let effective_kind = detection.event_kind.unwrap_or_else(|| analysis.event_kind.clone());
        let effective_severity = detection.event_severity.unwrap_or_else(|| analysis.event_severity.clone());
        let effective_message = detection.message.unwrap_or_else(|| analysis.message.clone());
        let mut artifacts = Vec::new();
        if let Some(artifact) = build_analysis_artifact(
            &analysis.camera.id.0,
            &event_id.0,
            &sample_path,
            occurred_at_unix_ms,
        )? {
            artifacts.push(artifact);
        } else {
            let _ = fs::remove_file(&sample_path);
        }

        analysis.pending_events.push(PendingAnalysisEvent {
            event: SurveillanceEvent {
                id: event_id,
                camera_id: analysis.camera.id.clone(),
                kind: effective_kind,
                severity: effective_severity,
                occurred_at_unix_ms,
                message: effective_message,
            },
            artifacts,
        });
    }

    Ok(())
}

fn build_analysis_artifact(
    camera_id: &str,
    event_id: &str,
    sample_path: &Path,
    created_at_unix_ms: u64,
) -> Result<Option<harborlookout_domain::EventArtifact>> {
    if !sample_path.exists() {
        return Ok(None);
    }

    let size_bytes = fs::metadata(sample_path)?.len();
    Ok(Some(harborlookout_domain::EventArtifact {
        id: ArtifactId(format!("artifact-{event_id}")),
        event_id: EventId(event_id.to_string()),
        camera_id: harborlookout_domain::CameraId(camera_id.to_string()),
        kind: ArtifactKind::Keyframe,
        path: sample_path.to_string_lossy().replace('\\', "/"),
        mime_type: Some("image/jpeg".into()),
        created_at_unix_ms,
        started_at_unix_ms: Some(created_at_unix_ms),
        ended_at_unix_ms: Some(created_at_unix_ms),
        size_bytes,
    }))
}

fn build_sample_path(camera_id: &str, sequence: usize, artifacts_directory: Option<&str>) -> PathBuf {
    let base_directory = artifacts_directory
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("harborlookout-analysis-samples"));
    base_directory
        .join(camera_id)
        .join(format!("sample-{:06}.jpg", sequence))
}

async fn capture_snapshot(backend: &FfmpegBackend, camera: &harborlookout_domain::Camera, sample_path: &Path) -> Result<()> {
    if let Some(parent) = sample_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let plan = backend.build_snapshot_plan(camera, &sample_path.to_string_lossy())?;
    let program = plan.program_string();
    let output = Command::new(&program)
        .args(&plan.args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "failed to capture sampled frame for camera {} with `{}`: {}",
            camera.id.0,
            program,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    Ok(())
}

async fn run_local_detector(
    detector: &LocalDetectorConfig,
    sample_path: &Path,
    camera_id: &str,
    event_kind: &harborlookout_domain::EventKind,
    event_severity: &harborlookout_domain::EventSeverity,
) -> Result<DetectorResult> {
    let resolved_args = detector
        .args
        .iter()
        .map(|arg| {
            arg.replace("{frame_path}", &sample_path.to_string_lossy())
                .replace("{camera_id}", camera_id)
                .replace("{event_kind}", &format!("{:?}", event_kind))
                .replace("{event_severity}", &format!("{:?}", event_severity))
        })
        .collect::<Vec<_>>();

    let output = Command::new(&detector.program)
        .args(&resolved_args)
        .env("HARBORLOOKOUT_FRAME_PATH", sample_path)
        .env("HARBORLOOKOUT_CAMERA_ID", camera_id)
        .env("HARBORLOOKOUT_EVENT_KIND", format!("{:?}", event_kind))
        .env("HARBORLOOKOUT_EVENT_SEVERITY", format!("{:?}", event_severity))
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "detector `{}` failed: {}",
            detector.program,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let stdout = String::from_utf8(output.stdout)?;
    let parsed: DetectorResult = serde_json::from_str(stdout.trim()).map_err(|error| {
        anyhow::anyhow!("detector `{}` returned invalid JSON: {error}", detector.program)
    })?;
    Ok(parsed)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis() as u64
}

#[derive(Debug, Deserialize)]
struct DetectorResult {
    detected: bool,
    message: Option<String>,
    event_kind: Option<harborlookout_domain::EventKind>,
    event_severity: Option<harborlookout_domain::EventSeverity>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use harborlookout_contracts::StartRecordingRequest;
    use harborlookout_domain::{Camera, CameraId, ExternalRecordingLease, RtspSource, RtspTransport, StreamProfile};

    #[test]
    fn describes_failed_exit_with_numeric_code() {
        let message = describe_failed_exit("cam-lobby", Some(7));
        assert_eq!(message, "recording process for camera cam-lobby exited with code 7");
    }

    #[test]
    fn describes_failed_exit_without_code() {
        let message = describe_failed_exit("cam-lobby", None);
        assert_eq!(message, "recording process for camera cam-lobby exited without an exit code");
    }

    #[test]
    fn builds_analysis_artifact_from_sample_file() {
        let temp_root = std::env::temp_dir().join(format!(
            "harborlookout-worker-analysis-{}-{}",
            std::process::id(),
            now_unix_ms()
        ));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("cam-analysis")).unwrap();
        let sample_path = temp_root.join("cam-analysis").join("sample-000001.jpg");
        fs::write(&sample_path, b"sample-data").unwrap();

        let artifact = build_analysis_artifact(
            "cam-analysis",
            "analysis-cam-analysis-000001",
            &sample_path,
            1_000,
        )
        .unwrap()
        .expect("artifact should be created");

        assert_eq!(artifact.kind, ArtifactKind::Keyframe);
        assert!(Path::new(&artifact.path).exists());

        let _ = fs::remove_dir_all(&temp_root);
    }

    #[test]
    fn builds_sample_path_under_camera_directory() {
        let path = build_sample_path("cam-analysis", 3, Some("data/artifacts"));
        assert!(path.to_string_lossy().contains("cam-analysis"));
        assert!(path.to_string_lossy().ends_with("sample-000003.jpg"));
    }

    #[test]
    fn parses_detector_result_json() {
        let parsed: DetectorResult = serde_json::from_str(
            r#"{"detected":true,"message":"package detected","event_kind":"PackageDetected","event_severity":"Warning"}"#,
        )
        .unwrap();

        assert!(parsed.detected);
        assert_eq!(parsed.message.as_deref(), Some("package detected"));
    }

    #[tokio::test]
    async fn external_recording_fails_closed_when_writer_is_unavailable() {
        let state = WorkerAppState {
            backend: FfmpegBackend,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            external_sessions: Arc::new(Mutex::new(HashMap::new())),
            analyses: Arc::new(Mutex::new(HashMap::new())),
        };
        let request = StartRecordingRequest {
            camera: Camera {
                id: CameraId("cam-external".into()),
                name: "External target".into(),
                streams: Vec::new(),
                source: RtspSource {
                    url: "rtsp://camera.local/external".into(),
                    transport: RtspTransport::Tcp,
                },
                stream_profile: StreamProfile {
                    width: 1280,
                    height: 720,
                    fps: 15,
                    segment_seconds: 10,
                },
                enabled: true,
            },
            output_directory: "should-never-be-used".into(),
            external_recording_lease: Some(ExternalRecordingLease {
                lease_id: "lease-tf-1".into(),
                slot_id: "TF-1".into(),
                source_token: "opaque-source-token".into(),
                expires_at_unix_ms: now_unix_ms() + 60_000,
            }),
        };

        let result = start_recording(State(state.clone()), Json(request)).await;
        let error = result.unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_GATEWAY);
        assert!(error.1.contains("external recording failed"));
        assert!(state.sessions.lock().await.is_empty());
    }
}

fn digest_hex(digest: &Sha256) -> String {
    format!("{:x}", digest.clone().finalize())
}
