use harborlookout_domain::{
    ArtifactKind, Camera, CameraId, EventArtifact, EventId, EventKind, EventSeverity,
    ExternalRecordingLease, RecordingSegment, SurveillanceEvent, WorkerHealth, WorkerState,
};
use serde::{Deserialize, Serialize};

/// Versioned HarborOS writer seam. The worker must not translate these
/// requests into an output directory or a device path.
pub const HARBOROS_RECORDING_WRITER_SCHEMA: &str = "harboros.recording-writer.v1";
pub const HARBOROS_RECORDING_WRITER_MAX_CHUNK_BYTES: usize = 1024 * 1024;

/// HarborOS writer requests intentionally contain only opaque identifiers.
/// The media service owns the mount and destination path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordingWriterLeaseResolveRequest {
    pub lease_ref: String,
}

impl RecordingWriterLeaseResolveRequest {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if !valid_opaque_id(&self.lease_ref) {
            return Err("lease_ref must be an opaque identifier");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingWriterLeaseResponse {
    pub lease_ref: String,
    pub job_id: String,
    pub slot_id: String,
    pub recording_expires_at: String,
    pub recording_state: String,
    pub capability_profile: String,
    pub active_segment_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordingWriterSegmentStartRequest {
    pub lease_ref: String,
    pub segment_id: String,
    pub sequence: u64,
    #[serde(default)]
    pub started_at: Option<String>,
}

impl RecordingWriterSegmentStartRequest {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if !valid_opaque_id(&self.lease_ref) || !valid_opaque_id(&self.segment_id) {
            return Err("lease_ref and segment_id must be opaque identifiers");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordingWriterSegmentWriteRequest {
    pub lease_ref: String,
    pub segment_id: String,
    pub chunk_base64: String,
}

impl RecordingWriterSegmentWriteRequest {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if !valid_opaque_id(&self.lease_ref) || !valid_opaque_id(&self.segment_id) {
            return Err("lease_ref and segment_id must be opaque identifiers");
        }
        if self.chunk_base64.is_empty()
            || !self.chunk_base64.is_ascii()
            || self
                .chunk_base64
                .bytes()
                .any(|byte| byte.is_ascii_whitespace())
            || self.chunk_base64.bytes().any(|byte| {
                !matches!(
                    byte,
                    b'A'..=b'Z'
                        | b'a'..=b'z'
                        | b'0'..=b'9'
                        | b'+'
                        | b'/'
                        | b'='
                )
            })
            || self.chunk_base64.len() % 4 != 0
        {
            return Err("chunk_base64 must be padded ASCII base64");
        }
        let max_encoded = 4 * HARBOROS_RECORDING_WRITER_MAX_CHUNK_BYTES.div_ceil(3);
        if self.chunk_base64.len() > max_encoded {
            return Err("chunk_base64 exceeds the bounded writer chunk size");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordingWriterSegmentCompleteRequest {
    pub lease_ref: String,
    pub segment_id: String,
    pub state: String,
    pub bytes_done: Option<u64>,
    pub hash_sha256: Option<String>,
    pub error_code: Option<String>,
}

impl RecordingWriterSegmentCompleteRequest {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if !valid_opaque_id(&self.lease_ref) || !valid_opaque_id(&self.segment_id) {
            return Err("lease_ref and segment_id must be opaque identifiers");
        }
        if !matches!(self.state.as_str(), "sealed" | "failed" | "interrupted") {
            return Err("state must be sealed, failed, or interrupted");
        }
        if let Some(hash) = self.hash_sha256.as_deref()
            && (hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err("hash_sha256 must be a SHA-256 digest");
        }
        Ok(())
    }
}

/// Projection returned by HarborOS for all segment lifecycle operations.
/// A path is deliberately not part of this type; unknown fields are rejected
/// when decoding a response so a path-bearing response fails closed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingWriterSegmentResponse {
    pub segment_id: String,
    pub lease_ref: String,
    pub sequence: u64,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub bytes_done: u64,
    pub hash_sha256: Option<String>,
    pub state: String,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterCameraRecordingRequest {
    pub output_directory: String,
    /// External recording is accepted at the wire boundary so callers cannot
    /// accidentally fall back to internal storage. The current worker rejects
    /// this explicitly until the HarborOS-owned writer is integrated.
    #[serde(default)]
    pub external_recording_lease: Option<ExternalRecordingLease>,
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
    /// Opaque TF-1 lease. No worker may interpret this as a local path.
    #[serde(default)]
    pub external_recording_lease: Option<ExternalRecordingLease>,
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
    /// Opaque TF-1 lease. No worker may interpret this as a local path.
    #[serde(default)]
    pub external_recording_lease: Option<ExternalRecordingLease>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_requests_reject_paths_and_output_directories() {
        let error = serde_json::from_str::<RecordingWriterSegmentStartRequest>(
            r#"{"lease_ref":"lease-1","segment_id":"segment-1","sequence":1,"output_directory":"/data"}"#,
        )
        .expect_err("writer request must reject output_directory");
        assert!(error.to_string().contains("unknown field"));

        let request = RecordingWriterSegmentStartRequest {
            lease_ref: "lease-1".into(),
            segment_id: "segment-1".into(),
            sequence: 1,
            started_at: None,
        };
        assert!(request.validate_shape().is_ok());
    }

    #[test]
    fn writer_chunk_shape_is_bounded_and_opaque() {
        let request = RecordingWriterSegmentWriteRequest {
            lease_ref: "lease-1".into(),
            segment_id: "segment-1".into(),
            chunk_base64: "AQID".into(),
        };
        assert!(request.validate_shape().is_ok());

        let mut path_request = request.clone();
        path_request.segment_id = "../segment".into();
        assert!(path_request.validate_shape().is_err());

        let mut invalid = request;
        invalid.chunk_base64 = "not base64".into();
        assert!(invalid.validate_shape().is_err());
    }

    #[test]
    fn writer_complete_shape_only_allows_terminal_states_and_sha256() {
        let request = RecordingWriterSegmentCompleteRequest {
            lease_ref: "lease-1".into(),
            segment_id: "segment-1".into(),
            state: "sealed".into(),
            bytes_done: Some(3),
            hash_sha256: Some("a".repeat(64)),
            error_code: None,
        };
        assert!(request.validate_shape().is_ok());

        let mut invalid = request;
        invalid.state = "writing".into();
        assert!(invalid.validate_shape().is_err());
    }
}

fn valid_opaque_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.chars().any(|character| {
            character.is_control()
                || character.is_whitespace()
                || matches!(character, '/' | '\\' | '?' | '#')
        })
}
