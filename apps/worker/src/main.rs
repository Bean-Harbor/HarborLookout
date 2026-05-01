use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
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
};
use harborlookout_domain::{
    ArtifactId, ArtifactKind, EventId, SurveillanceEvent, WorkerState as RecordingWorkerState,
};
use harborlookout_media_ffmpeg::FfmpegBackend;
use harborlookout_media_core::MediaBackend;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::info;

struct ManagedRecording {
    pid: u32,
    output_hint: String,
    child: Child,
    last_exit_code: Option<i32>,
    last_error: Option<String>,
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
    analyses: Arc<Mutex<HashMap<String, ManagedAnalysis>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let state = WorkerAppState {
        backend: FfmpegBackend,
        sessions: Arc::new(Mutex::new(HashMap::new())),
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

async fn start_recording(
    State(state): State<WorkerAppState>,
    Json(request): Json<StartRecordingRequest>,
) -> Result<Json<StartRecordingResponse>, (StatusCode, String)> {
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
    let stop_request = StopRecordingRequest {
        camera_id: request.camera.id.clone(),
    };
    let _ = stop_recording(State(state.clone()), Json(stop_request)).await?;

    let start_response = start_recording(
        State(state.clone()),
        Json(StartRecordingRequest {
            camera: request.camera.clone(),
            output_directory: request.output_directory.clone(),
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
}