use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CameraId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EventId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ArtifactId(pub String);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RtspTransport {
    Tcp,
    Udp,
    Auto,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RtspSource {
    pub url: String,
    pub transport: RtspTransport,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StreamRole {
    Detect,
    Record,
    LiveView,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamProfile {
    pub width: u32,
    pub height: u32,
    pub fps: u16,
    pub segment_seconds: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CameraStream {
    pub role: StreamRole,
    pub source: RtspSource,
    pub stream_profile: StreamProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Camera {
    pub id: CameraId,
    pub name: String,
    #[serde(default)]
    pub streams: Vec<CameraStream>,
    pub source: RtspSource,
    pub stream_profile: StreamProfile,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordingSegment {
    pub camera_id: CameraId,
    pub sequence: u64,
    pub path: String,
    pub started_at_unix_ms: u64,
    pub ended_at_unix_ms: Option<u64>,
    pub duration_ms: u64,
    pub size_bytes: u64,
    pub state: RecordingState,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecordingState {
    Pending,
    Running,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordingSession {
    pub camera_id: CameraId,
    pub worker_name: String,
    pub output_directory: String,
    pub output_hint: Option<String>,
    pub state: RecordingState,
    pub pid: Option<u32>,
    pub started_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EventKind {
    MotionDetected,
    MotionInZone,
    PersonDetected,
    VehicleDetected,
    PetDetected,
    PackageDetected,
    DrinkContainerDetected,
    ObjectRemoved,
    SpillCandidateDetected,
    RecordingStarted,
    RecordingStopped,
    CameraOffline,
    CameraOnline,
}

impl EventKind {
    pub fn is_analysis_event(&self) -> bool {
        matches!(
            self,
            Self::MotionDetected
                | Self::MotionInZone
                | Self::PersonDetected
                | Self::VehicleDetected
                | Self::PetDetected
                | Self::PackageDetected
                | Self::DrinkContainerDetected
                | Self::ObjectRemoved
                | Self::SpillCandidateDetected
        )
    }

    pub fn is_system_event(&self) -> bool {
        !self.is_analysis_event()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EventSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ArtifactKind {
    Keyframe,
    Clip,
    Thumbnail,
    Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SurveillanceEvent {
    pub id: EventId,
    pub camera_id: CameraId,
    pub kind: EventKind,
    pub severity: EventSeverity,
    pub occurred_at_unix_ms: u64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventArtifact {
    pub id: ArtifactId,
    pub event_id: EventId,
    pub camera_id: CameraId,
    pub kind: ArtifactKind,
    pub path: String,
    pub mime_type: Option<String>,
    pub created_at_unix_ms: u64,
    pub started_at_unix_ms: Option<u64>,
    pub ended_at_unix_ms: Option<u64>,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkerState {
    Starting,
    Running,
    Healthy,
    Degraded,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerHealth {
    pub worker_name: String,
    pub state: WorkerState,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("camera id cannot be empty")]
    EmptyCameraId,
    #[error("camera name cannot be empty")]
    EmptyCameraName,
    #[error("rtsp url must start with rtsp:// or rtsps://")]
    InvalidRtspUrl,
    #[error("stream width and height must be non-zero")]
    InvalidResolution,
    #[error("stream fps must be non-zero")]
    InvalidFps,
    #[error("segment duration must be non-zero")]
    InvalidSegmentDuration,
    #[error("duplicate stream role configured: {0:?}")]
    DuplicateStreamRole(StreamRole),
}

impl Camera {
    pub fn legacy_stream(&self) -> CameraStream {
        CameraStream {
            role: StreamRole::Record,
            source: self.source.clone(),
            stream_profile: self.stream_profile.clone(),
        }
    }

    pub fn effective_streams(&self) -> Vec<CameraStream> {
        if self.streams.is_empty() {
            vec![self.legacy_stream()]
        } else {
            self.streams.clone()
        }
    }

    pub fn preferred_stream(&self, role: StreamRole) -> CameraStream {
        self.streams
            .iter()
            .find(|stream| stream.role == role)
            .cloned()
            .or_else(|| self.streams.first().cloned())
            .unwrap_or_else(|| self.legacy_stream())
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.id.0.trim().is_empty() {
            return Err(DomainError::EmptyCameraId);
        }

        if self.name.trim().is_empty() {
            return Err(DomainError::EmptyCameraName);
        }

        let mut seen_roles = HashSet::new();
        for stream in self.effective_streams() {
            if !seen_roles.insert(stream.role) {
                return Err(DomainError::DuplicateStreamRole(stream.role));
            }

            validate_stream(&stream.source, &stream.stream_profile)?;
        }

        Ok(())
    }
}

fn validate_stream(source: &RtspSource, stream_profile: &StreamProfile) -> Result<(), DomainError> {
    let url = source.url.trim().to_ascii_lowercase();
    if !(url.starts_with("rtsp://") || url.starts_with("rtsps://")) {
        return Err(DomainError::InvalidRtspUrl);
    }

    if stream_profile.width == 0 || stream_profile.height == 0 {
        return Err(DomainError::InvalidResolution);
    }

    if stream_profile.fps == 0 {
        return Err(DomainError::InvalidFps);
    }

    if stream_profile.segment_seconds == 0 {
        return Err(DomainError::InvalidSegmentDuration);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_camera() -> Camera {
        Camera {
            id: CameraId("cam-front-door".into()),
            name: "Front Door".into(),
            streams: vec![],
            source: RtspSource {
                url: "rtsp://camera.local/stream".into(),
                transport: RtspTransport::Tcp,
            },
            stream_profile: StreamProfile {
                width: 1920,
                height: 1080,
                fps: 15,
                segment_seconds: 10,
            },
            enabled: true,
        }
    }

    #[test]
    fn validates_camera() {
        assert!(valid_camera().validate().is_ok());
    }

    #[test]
    fn rejects_invalid_rtsp_url() {
        let mut camera = valid_camera();
        camera.source.url = "http://camera.local/stream".into();
        assert!(matches!(camera.validate(), Err(DomainError::InvalidRtspUrl)));
    }

    #[test]
    fn supports_multi_stream_cameras() {
        let mut camera = valid_camera();
        camera.streams = vec![
            CameraStream {
                role: StreamRole::Detect,
                source: RtspSource {
                    url: "rtsp://camera.local/detect".into(),
                    transport: RtspTransport::Tcp,
                },
                stream_profile: StreamProfile {
                    width: 640,
                    height: 360,
                    fps: 5,
                    segment_seconds: 10,
                },
            },
            CameraStream {
                role: StreamRole::Record,
                source: RtspSource {
                    url: "rtsp://camera.local/record".into(),
                    transport: RtspTransport::Udp,
                },
                stream_profile: StreamProfile {
                    width: 1920,
                    height: 1080,
                    fps: 15,
                    segment_seconds: 10,
                },
            },
        ];

        assert!(camera.validate().is_ok());
        assert_eq!(camera.preferred_stream(StreamRole::Record).source.url, "rtsp://camera.local/record");
        assert_eq!(camera.preferred_stream(StreamRole::LiveView).source.url, "rtsp://camera.local/detect");
    }

    #[test]
    fn rejects_duplicate_stream_roles() {
        let mut camera = valid_camera();
        camera.streams = vec![
            CameraStream {
                role: StreamRole::Record,
                source: RtspSource {
                    url: "rtsp://camera.local/record-1".into(),
                    transport: RtspTransport::Tcp,
                },
                stream_profile: StreamProfile {
                    width: 1280,
                    height: 720,
                    fps: 10,
                    segment_seconds: 10,
                },
            },
            CameraStream {
                role: StreamRole::Record,
                source: RtspSource {
                    url: "rtsp://camera.local/record-2".into(),
                    transport: RtspTransport::Tcp,
                },
                stream_profile: StreamProfile {
                    width: 1920,
                    height: 1080,
                    fps: 15,
                    segment_seconds: 10,
                },
            },
        ];

        assert!(matches!(camera.validate(), Err(DomainError::DuplicateStreamRole(StreamRole::Record))));
    }

    #[test]
    fn recording_segment_supports_runtime_state() {
        let segment = RecordingSegment {
            camera_id: CameraId("cam-front-door".into()),
            sequence: 42,
            path: "segments/cam-front-door-000042.mp4".into(),
            started_at_unix_ms: 1_744_000_000_000,
            ended_at_unix_ms: Some(1_744_000_010_000),
            duration_ms: 10_000,
            size_bytes: 4_096,
            state: RecordingState::Completed,
        };

        assert_eq!(segment.state, RecordingState::Completed);
        assert_eq!(segment.ended_at_unix_ms, Some(1_744_000_010_000));
    }

    #[test]
    fn event_artifact_supports_optional_time_bounds() {
        let artifact = EventArtifact {
            id: ArtifactId("artifact-1".into()),
            event_id: EventId("event-1".into()),
            camera_id: CameraId("cam-front-door".into()),
            kind: ArtifactKind::Clip,
            path: "artifacts/event-1/clip.mp4".into(),
            mime_type: Some("video/mp4".into()),
            created_at_unix_ms: 1_000,
            started_at_unix_ms: Some(900),
            ended_at_unix_ms: Some(1_100),
            size_bytes: 2_048,
        };

        assert_eq!(artifact.kind, ArtifactKind::Clip);
        assert_eq!(artifact.started_at_unix_ms, Some(900));
        assert_eq!(artifact.ended_at_unix_ms, Some(1_100));
    }

    #[test]
    fn distinguishes_analysis_and_system_event_kinds() {
        assert!(EventKind::PackageDetected.is_analysis_event());
        assert!(EventKind::MotionInZone.is_analysis_event());
        assert!(!EventKind::RecordingStarted.is_analysis_event());
        assert!(EventKind::RecordingStarted.is_system_event());
        assert!(!EventKind::PackageDetected.is_system_event());
    }
}