use std::collections::HashMap;
use std::fs;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use harborlookout_contracts::{
    BuildRecordingPlanRequest, BuildRecordingPlanResponse, RecordingStatusRequest, RecordingStatusResponse,
    RestartRecordingRequest, RestartRecordingResponse, StartRecordingRequest, StartRecordingResponse,
    StopRecordingRequest, StopRecordingResponse,
};
use harborlookout_domain::WorkerState as RecordingWorkerState;
use harborlookout_media_ffmpeg::FfmpegBackend;
use harborlookout_media_core::MediaBackend;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::info;

struct ManagedRecording {
    pid: u32,
    output_hint: String,
    child: Child,
}

#[derive(Clone)]
struct WorkerAppState {
    backend: FfmpegBackend,
    sessions: Arc<Mutex<HashMap<String, ManagedRecording>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let state = WorkerAppState {
        backend: FfmpegBackend,
        sessions: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/recordings/plan", post(build_recording_plan))
        .route("/api/recordings/start", post(start_recording))
        .route("/api/recordings/stop", post(stop_recording))
        .route("/api/recordings/restart", post(restart_recording))
        .route("/api/recordings/status", post(recording_status))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:5050").await?;
    info!(address = %listener.local_addr()?, "HarborLookout worker listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<WorkerAppState>) -> Json<serde_json::Value> {
    let active_sessions = state.sessions.lock().await.len();
    Json(serde_json::json!({
        "status": "ok",
        "backend": state.backend.name(),
        "active_sessions": active_sessions,
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

    let child = command.spawn().map_err(internal_error)?;
    let pid = child.id().ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "spawned ffmpeg process has no pid".to_string()))?;

    state.sessions.lock().await.insert(
        camera_id,
        ManagedRecording {
            pid,
            output_hint: output_hint.clone(),
            child,
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
        }));
    };

    let mut final_state = RecordingWorkerState::Running;
    let pid = Some(managed.pid);
    let output_hint = Some(managed.output_hint.clone());
    let remove = match managed.child.try_wait().map_err(internal_error)? {
        Some(exit_status) => {
            final_state = if exit_status.success() {
                RecordingWorkerState::Stopped
            } else {
                RecordingWorkerState::Failed
            };
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