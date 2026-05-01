use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use harborlookout_contracts::{
    AnalysisStatusRequest, AnalysisStatusResponse, ArtifactRetentionCleanupRequest,
    ArtifactRetentionCleanupResponse, ArtifactRetentionPreviewRequest, ArtifactRetentionPreviewResponse,
    CreateSurveillanceEventRequest, CreateSurveillanceEventResponse, HealthResponse, ListCamerasResponse,
    ListEventArtifactsRequest, ListEventArtifactsResponse, ListRecordingSegmentsRequest,
    ListRecordingSegmentsResponse, ListSurveillanceEventsRequest, ListSurveillanceEventsResponse,
    ProbeCameraRequest, ProbeCameraResponse, RecordingStatusRequest, RecordingStatusResponse,
    RetentionCleanupRequest, RetentionCleanupResponse, RetentionPreviewRequest,
    RetentionPreviewResponse, RegisterCameraRequest, RegisterCameraResponse, RestartRecordingRequest,
    RestartRecordingResponse, StartAnalysisRequest, StartAnalysisResponse, StartRecordingRequest,
    StartRecordingResponse, StopAnalysisRequest, StopAnalysisResponse, StopRecordingRequest,
    StopRecordingResponse, StorageSummaryResponse, SyncAnalysisEventsRequest,
    SyncAnalysisEventsResponse, SyncRecordingSegmentsRequest, SyncRecordingSegmentsResponse,
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
        .route("/api/analysis/start", axum::routing::post(start_analysis))
        .route("/api/analysis/stop", axum::routing::post(stop_analysis))
        .route("/api/analysis/status", axum::routing::post(analysis_status))
        .route("/api/analysis/events/sync", axum::routing::post(sync_analysis_events))
        .route("/api/events", axum::routing::post(create_surveillance_event))
        .route("/api/events/query", axum::routing::post(list_surveillance_events))
        .route("/api/events/artifacts/query", axum::routing::post(list_event_artifacts))
        .route("/api/storage/summary", get(storage_summary))
        .route("/api/storage/retention-preview", axum::routing::post(retention_preview))
        .route("/api/storage/retention-cleanup", axum::routing::post(retention_cleanup))
        .route(
            "/api/storage/artifacts/retention-preview",
            axum::routing::post(artifact_retention_preview),
        )
        .route(
            "/api/storage/artifacts/retention-cleanup",
            axum::routing::post(artifact_retention_cleanup),
        )
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
        .await
        .map(Json)
        .map_err(map_worker_or_request_error)
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

async fn start_analysis(
    State(state): State<AppState>,
    Json(request): Json<StartAnalysisRequest>,
) -> Result<Json<StartAnalysisResponse>, (StatusCode, String)> {
    state
        .control_plane
        .start_analysis(&request)
        .await
        .map(Json)
        .map_err(map_worker_or_request_error)
}

async fn stop_analysis(
    State(state): State<AppState>,
    Json(request): Json<StopAnalysisRequest>,
) -> Result<Json<StopAnalysisResponse>, (StatusCode, String)> {
    state
        .control_plane
        .stop_analysis(&request)
        .await
        .map(Json)
        .map_err(map_worker_or_request_error)
}

async fn analysis_status(
    State(state): State<AppState>,
    Json(request): Json<AnalysisStatusRequest>,
) -> Result<Json<AnalysisStatusResponse>, (StatusCode, String)> {
    state
        .control_plane
        .analysis_status(&request)
        .await
        .map(Json)
        .map_err(map_worker_or_request_error)
}

async fn sync_analysis_events(
    State(state): State<AppState>,
    Json(request): Json<SyncAnalysisEventsRequest>,
) -> Result<Json<SyncAnalysisEventsResponse>, (StatusCode, String)> {
    state
        .control_plane
        .sync_analysis_events(&request)
        .await
        .map(Json)
        .map_err(map_worker_or_request_error)
}

async fn create_surveillance_event(
    State(state): State<AppState>,
    Json(request): Json<CreateSurveillanceEventRequest>,
) -> Result<Json<CreateSurveillanceEventResponse>, (StatusCode, String)> {
    state
        .control_plane
        .create_surveillance_event(&request)
        .map(Json)
        .map_err(internal_error)
}

async fn list_surveillance_events(
    State(state): State<AppState>,
    Json(request): Json<ListSurveillanceEventsRequest>,
) -> Result<Json<ListSurveillanceEventsResponse>, (StatusCode, String)> {
    state
        .control_plane
        .list_surveillance_events(&request)
        .map(Json)
        .map_err(internal_error)
}

async fn list_event_artifacts(
    State(state): State<AppState>,
    Json(request): Json<ListEventArtifactsRequest>,
) -> Result<Json<ListEventArtifactsResponse>, (StatusCode, String)> {
    state
        .control_plane
        .list_event_artifacts(&request)
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

async fn retention_cleanup(
    State(state): State<AppState>,
    Json(request): Json<RetentionCleanupRequest>,
) -> Result<Json<RetentionCleanupResponse>, (StatusCode, String)> {
    state
        .control_plane
        .retention_cleanup(&request)
        .map(Json)
        .map_err(internal_error)
}

async fn artifact_retention_preview(
    State(state): State<AppState>,
    Json(request): Json<ArtifactRetentionPreviewRequest>,
) -> Result<Json<ArtifactRetentionPreviewResponse>, (StatusCode, String)> {
    state
        .control_plane
        .artifact_retention_preview(&request)
        .map(Json)
        .map_err(internal_error)
}

async fn artifact_retention_cleanup(
    State(state): State<AppState>,
    Json(request): Json<ArtifactRetentionCleanupRequest>,
) -> Result<Json<ArtifactRetentionCleanupResponse>, (StatusCode, String)> {
    state
        .control_plane
        .artifact_retention_cleanup(&request)
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