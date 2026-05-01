use harborlookout_domain::{
    ArtifactKind, Camera, CameraId, EventArtifact, EventId, EventKind, EventSeverity,
    RecordingSegment, SurveillanceEvent, WorkerHealth, WorkerState,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterCameraRecordingRequest {
    pub output_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterCameraRequest {
    pub camera: Camera,
    #[serde(default)]
    pub start_recording: Option<RegisterCameraRecordingRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterCameraResponse {
    pub camera_id: String,
    pub backend: String,
    pub recording: Option<StartRecordingResponse>,
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
    pub exit_code: Option<i32>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalDetectorConfig {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartAnalysisRequest {
    pub camera: Camera,
    pub interval_ms: u64,
    pub event_kind: EventKind,
    pub event_severity: EventSeverity,
    pub message: Option<String>,
    pub artifacts_directory: Option<String>,
    pub detector: LocalDetectorConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartAnalysisResponse {
    pub camera_id: String,
    pub worker_name: String,
    pub state: WorkerState,
    pub interval_ms: u64,
    pub event_kind: EventKind,
    pub event_severity: EventSeverity,
    pub artifacts_directory: Option<String>,
    pub detector_program: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopAnalysisRequest {
    pub camera_id: CameraId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopAnalysisResponse {
    pub camera_id: String,
    pub stopped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisStatusRequest {
    pub camera_id: CameraId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisStatusResponse {
    pub camera_id: String,
    pub state: WorkerState,
    pub event_kind: Option<EventKind>,
    pub event_severity: Option<EventSeverity>,
    pub interval_ms: Option<u64>,
    pub detector_program: Option<String>,
    pub emitted_events: usize,
    pub pending_events: usize,
    pub last_event_at_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncAnalysisEventsRequest {
    pub camera_id: CameraId,
    pub max_events: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingAnalysisEvent {
    pub event: SurveillanceEvent,
    #[serde(default)]
    pub artifacts: Vec<EventArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullAnalysisEventsResponse {
    pub camera_id: String,
    pub events: Vec<PendingAnalysisEvent>,
    pub last_event_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncAnalysisEventsResponse {
    pub camera_id: String,
    pub stored_events: usize,
    pub stored_artifacts: usize,
    pub last_event_at_unix_ms: Option<u64>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionCleanupRequest {
    pub retain_after_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionCleanupCamera {
    pub camera_id: String,
    pub deleted_segments: usize,
    pub deleted_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionCleanupResponse {
    pub retain_after_unix_ms: u64,
    pub attempted_segments: usize,
    pub deleted_segments: usize,
    pub deleted_bytes: u64,
    pub skipped_segments: usize,
    pub cameras: Vec<RetentionCleanupCamera>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRetentionPreviewRequest {
    pub retain_after_unix_ms: u64,
    #[serde(default)]
    pub retain_after_by_kind: Vec<ArtifactKindRetentionRule>,
    pub protect_events_occurred_after_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactKindRetentionRule {
    pub kind: ArtifactKind,
    pub retain_after_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRetentionPreviewCamera {
    pub camera_id: String,
    pub reclaimable_artifacts: usize,
    pub reclaimable_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRetentionPreviewResponse {
    pub retain_after_unix_ms: u64,
    pub reclaimable_artifacts: usize,
    pub reclaimable_bytes: u64,
    pub protected_artifacts: usize,
    pub cameras: Vec<ArtifactRetentionPreviewCamera>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRetentionCleanupRequest {
    pub retain_after_unix_ms: u64,
    #[serde(default)]
    pub retain_after_by_kind: Vec<ArtifactKindRetentionRule>,
    pub protect_events_occurred_after_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRetentionCleanupCamera {
    pub camera_id: String,
    pub deleted_artifacts: usize,
    pub deleted_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRetentionCleanupResponse {
    pub retain_after_unix_ms: u64,
    pub attempted_artifacts: usize,
    pub deleted_artifacts: usize,
    pub deleted_bytes: u64,
    pub protected_artifacts: usize,
    pub skipped_artifacts: usize,
    pub cameras: Vec<ArtifactRetentionCleanupCamera>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSurveillanceEventRequest {
    pub event: SurveillanceEvent,
    #[serde(default)]
    pub artifacts: Vec<EventArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSurveillanceEventResponse {
    pub event_id: String,
    pub stored_artifacts: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSurveillanceEventsRequest {
    pub camera_id: Option<CameraId>,
    pub occurred_after_unix_ms: Option<u64>,
    pub occurred_before_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSurveillanceEventsResponse {
    pub events: Vec<SurveillanceEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListEventArtifactsRequest {
    pub event_id: EventId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListEventArtifactsResponse {
    pub artifacts: Vec<EventArtifact>,
}