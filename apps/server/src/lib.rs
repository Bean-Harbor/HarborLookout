use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use harborlookout_contracts::{
    HealthResponse, ListCamerasResponse, ListRecordingSegmentsRequest, ListRecordingSegmentsResponse,
    ProbeCameraRequest, ProbeCameraResponse, RecordingStatusRequest, RecordingStatusResponse,
    RetentionPreviewRequest, RetentionPreviewResponse,
    RegisterCameraRequest, RegisterCameraResponse, RestartRecordingRequest, RestartRecordingResponse,
    StorageSummaryResponse,
    StartRecordingRequest, StartRecordingResponse, StopRecordingRequest, StopRecordingResponse,
    SyncRecordingSegmentsRequest, SyncRecordingSegmentsResponse,
};
use harborlookout_control_plane::{ControlPlane, HttpWorkerClient};
use harborlookout_media_ffmpeg::FfmpegBackend;
use harborlookout_storage::SqliteCameraStore;

pub type ServerControlPlane = ControlPlane<FfmpegBackend, SqliteCameraStore, HttpWorkerClient>;

#[derive(Clone)]
pub struct AppState {
    pub control_plane: Arc<ServerControlPlane>,
}

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/cameras/probe", axum::routing::post(probe_camera))
        .route("/api/cameras", get(list_cameras).post(register_camera))
        .route("/api/recordings/start", axum::routing::post(start_recording))
        .route("/api/recordings/stop", axum::routing::post(stop_recording))
        .route("/api/recordings/restart", axum::routing::post(restart_recording))
        .route("/api/recordings/status", axum::routing::post(recording_status))
        .route("/api/recordings/segments/query", axum::routing::post(list_recording_segments))
        .route("/api/recordings/segments/sync", axum::routing::post(sync_recording_segments))
        .route("/api/storage/summary", get(storage_summary))
        .route("/api/storage/retention-preview", axum::routing::post(retention_preview))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, (StatusCode, String)> {
    state.control_plane.health().map(Json).map_err(internal_error)
}

async fn list_cameras(State(state): State<AppState>) -> Result<Json<ListCamerasResponse>, (StatusCode, String)> {
    state.control_plane.list_cameras().map(Json).map_err(internal_error)
}

async fn register_camera(
    State(state): State<AppState>,
    Json(request): Json<RegisterCameraRequest>,
) -> Result<Json<RegisterCameraResponse>, (StatusCode, String)> {
    state
        .control_plane
        .register_camera(&request)
        .map(Json)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))
}

async fn probe_camera(
    State(state): State<AppState>,
    Json(request): Json<ProbeCameraRequest>,
) -> Result<Json<ProbeCameraResponse>, (StatusCode, String)> {
    state
        .control_plane
        .probe_camera(&request)
        .map(Json)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))
}

async fn start_recording(
    State(state): State<AppState>,
    Json(request): Json<StartRecordingRequest>,
) -> Result<Json<StartRecordingResponse>, (StatusCode, String)> {
    state
        .control_plane
        .start_recording(&request)
        .await
        .map(Json)
        .map_err(map_worker_or_request_error)
}

async fn stop_recording(
    State(state): State<AppState>,
    Json(request): Json<StopRecordingRequest>,
) -> Result<Json<StopRecordingResponse>, (StatusCode, String)> {
    state
        .control_plane
        .stop_recording(&request)
        .await
        .map(Json)
        .map_err(map_worker_or_request_error)
}

async fn restart_recording(
    State(state): State<AppState>,
    Json(request): Json<RestartRecordingRequest>,
) -> Result<Json<RestartRecordingResponse>, (StatusCode, String)> {
    state
        .control_plane
        .restart_recording(&request)
        .await
        .map(Json)
        .map_err(map_worker_or_request_error)
}

async fn recording_status(
    State(state): State<AppState>,
    Json(request): Json<RecordingStatusRequest>,
) -> Result<Json<RecordingStatusResponse>, (StatusCode, String)> {
    state
        .control_plane
        .recording_status(&request)
        .await
        .map(Json)
        .map_err(map_worker_or_request_error)
}

async fn list_recording_segments(
    State(state): State<AppState>,
    Json(request): Json<ListRecordingSegmentsRequest>,
) -> Result<Json<ListRecordingSegmentsResponse>, (StatusCode, String)> {
    state
        .control_plane
        .list_recording_segments(&request)
        .map(Json)
        .map_err(internal_error)
}

async fn sync_recording_segments(
    State(state): State<AppState>,
    Json(request): Json<SyncRecordingSegmentsRequest>,
) -> Result<Json<SyncRecordingSegmentsResponse>, (StatusCode, String)> {
    state
        .control_plane
        .sync_recording_segments(&request)
        .map(Json)
        .map_err(internal_error)
}

async fn storage_summary(State(state): State<AppState>) -> Result<Json<StorageSummaryResponse>, (StatusCode, String)> {
    state.control_plane.storage_summary().map(Json).map_err(internal_error)
}

async fn retention_preview(
    State(state): State<AppState>,
    Json(request): Json<RetentionPreviewRequest>,
) -> Result<Json<RetentionPreviewResponse>, (StatusCode, String)> {
    state
        .control_plane
        .retention_preview(&request)
        .map(Json)
        .map_err(internal_error)
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn map_worker_or_request_error(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    if message.contains("409 Conflict") {
        (StatusCode::CONFLICT, message)
    } else if message.contains("400 Bad Request") {
        (StatusCode::BAD_REQUEST, message)
    } else {
        (StatusCode::BAD_GATEWAY, message)
    }
}