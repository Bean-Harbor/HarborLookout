use harborlookout_domain::{Camera, CameraId, RecordingSegment, WorkerHealth, WorkerState};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterCameraRequest {
    pub camera: Camera,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterCameraResponse {
    pub camera_id: String,
    pub backend: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeCameraRequest {
    pub camera: Camera,
    pub output_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeCameraResponse {
    pub backend: String,
    pub transport: String,
    pub recording_program: String,
    pub recording_args: Vec<String>,
    pub output_hint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListCamerasResponse {
    pub cameras: Vec<Camera>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub backend: String,
    pub registered_cameras: usize,
    pub active_recordings: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHealthEvent {
    pub health: WorkerHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartRecordingRequest {
    pub camera: Camera,
    pub output_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartRecordingResponse {
    pub backend: String,
    pub program: String,
    pub args: Vec<String>,
    pub output_hint: String,
    pub pid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildRecordingPlanRequest {
    pub camera: Camera,
    pub output_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildRecordingPlanResponse {
    pub backend: String,
    pub program: String,
    pub args: Vec<String>,
    pub output_hint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopRecordingRequest {
    pub camera_id: CameraId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopRecordingResponse {
    pub camera_id: String,
    pub stopped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartRecordingRequest {
    pub camera: Camera,
    pub output_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartRecordingResponse {
    pub camera_id: String,
    pub restarted: bool,
    pub backend: String,
    pub program: String,
    pub args: Vec<String>,
    pub output_hint: String,
    pub pid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingStatusRequest {
    pub camera_id: CameraId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingStatusResponse {
    pub camera_id: String,
    pub state: WorkerState,
    pub pid: Option<u32>,
    pub output_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRecordingSegmentsRequest {
    pub camera_id: CameraId,
    pub started_after_unix_ms: Option<u64>,
    pub started_before_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRecordingSegmentsResponse {
    pub segments: Vec<RecordingSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRecordingSegmentsRequest {
    pub camera_id: CameraId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRecordingSegmentsResponse {
    pub camera_id: String,
    pub synced_segments: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCameraSummary {
    pub camera_id: String,
    pub segment_count: usize,
    pub total_bytes: u64,
    pub oldest_started_at_unix_ms: Option<u64>,
    pub newest_started_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSummaryResponse {
    pub total_segments: usize,
    pub total_bytes: u64,
    pub active_recordings: usize,
    pub cameras: Vec<StorageCameraSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPreviewRequest {
    pub retain_after_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPreviewCamera {
    pub camera_id: String,
    pub reclaimable_segments: usize,
    pub reclaimable_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPreviewResponse {
    pub retain_after_unix_ms: u64,
    pub reclaimable_segments: usize,
    pub reclaimable_bytes: u64,
    pub cameras: Vec<RetentionPreviewCamera>,
}