use std::time::{SystemTime, UNIX_EPOCH};
use std::{future::Future, pin::Pin};
use std::{fs, path::PathBuf};

use anyhow::Result;
use harborlookout_contracts::{
    HealthResponse, ListCamerasResponse, ListRecordingSegmentsRequest, ListRecordingSegmentsResponse,
    ProbeCameraRequest, ProbeCameraResponse, RecordingStatusRequest,
    RecordingStatusResponse, RegisterCameraRequest, RegisterCameraResponse, RestartRecordingRequest,
    RestartRecordingResponse, StartRecordingRequest, StartRecordingResponse, StopRecordingRequest,
    StopRecordingResponse, SyncRecordingSegmentsRequest, SyncRecordingSegmentsResponse,
    RetentionPreviewRequest, RetentionPreviewResponse, StorageSummaryResponse,
};
use harborlookout_domain::{Camera, RecordingSegment, RecordingSession, RecordingState, StreamRole, WorkerState};
use harborlookout_media_core::MediaBackend;
use harborlookout_storage::{CameraStore, RecordingSegmentStore, RecordingSessionStore};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub trait WorkerClient: Send + Sync {
    fn name(&self) -> &str;

    fn start_recording<'a>(
        &'a self,
        request: &'a StartRecordingRequest,
    ) -> Pin<Box<dyn Future<Output = Result<StartRecordingResponse>> + Send + 'a>>;

    fn stop_recording<'a>(
        &'a self,
        request: &'a StopRecordingRequest,
    ) -> Pin<Box<dyn Future<Output = Result<StopRecordingResponse>> + Send + 'a>>;

    fn restart_recording<'a>(
        &'a self,
        request: &'a RestartRecordingRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RestartRecordingResponse>> + Send + 'a>>;

    fn recording_status<'a>(
        &'a self,
        request: &'a RecordingStatusRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RecordingStatusResponse>> + Send + 'a>>;
}

#[derive(Clone)]
pub struct HttpWorkerClient {
    base_url: String,
}

impl HttpWorkerClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    async fn post_json<TRequest, TResponse>(&self, path: &str, request: &TRequest) -> Result<TResponse>
    where
        TRequest: Serialize + ?Sized,
        TResponse: DeserializeOwned,
    {
        let (host, port) = parse_base_url(&self.base_url)?;
        let body = serde_json::to_string(request)?;
        let mut stream = TcpStream::connect((host.as_str(), port)).await?;
        let request_text = format!(
            "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body,
        );

        stream.write_all(request_text.as_bytes()).await?;

        let mut response_bytes = Vec::new();
        stream.read_to_end(&mut response_bytes).await?;
        parse_json_response(&response_bytes)
    }
}

impl WorkerClient for HttpWorkerClient {
    fn name(&self) -> &str {
        "worker-http"
    }

    fn start_recording<'a>(
        &'a self,
        request: &'a StartRecordingRequest,
    ) -> Pin<Box<dyn Future<Output = Result<StartRecordingResponse>> + Send + 'a>> {
        Box::pin(async move { self.post_json("/api/recordings/start", request).await })
    }

    fn stop_recording<'a>(
        &'a self,
        request: &'a StopRecordingRequest,
    ) -> Pin<Box<dyn Future<Output = Result<StopRecordingResponse>> + Send + 'a>> {
        Box::pin(async move { self.post_json("/api/recordings/stop", request).await })
    }

    fn restart_recording<'a>(
        &'a self,
        request: &'a RestartRecordingRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RestartRecordingResponse>> + Send + 'a>> {
        Box::pin(async move { self.post_json("/api/recordings/restart", request).await })
    }

    fn recording_status<'a>(
        &'a self,
        request: &'a RecordingStatusRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RecordingStatusResponse>> + Send + 'a>> {
        Box::pin(async move { self.post_json("/api/recordings/status", request).await })
    }
}

pub struct ControlPlane<B: MediaBackend, S: CameraStore + RecordingSessionStore + RecordingSegmentStore, W: WorkerClient> {
    backend: B,
    store: S,
    worker: W,
}

impl<B: MediaBackend, S: CameraStore + RecordingSessionStore + RecordingSegmentStore, W: WorkerClient> ControlPlane<B, S, W> {
    pub fn new(backend: B, store: S, worker: W) -> Self {
        Self { backend, store, worker }
    }

    pub fn register_camera(&self, request: &RegisterCameraRequest) -> Result<RegisterCameraResponse> {
        self.backend.probe_camera(&request.camera)?;
        self.store.upsert_camera(&request.camera)?;

        Ok(RegisterCameraResponse {
            camera_id: request.camera.id.0.clone(),
            backend: self.backend.name().to_string(),
        })
    }

    pub fn probe_camera(&self, request: &ProbeCameraRequest) -> Result<ProbeCameraResponse> {
        let summary = self.backend.probe_camera(&request.camera)?;
        let plan = self
            .backend
            .build_recording_plan(&request.camera, &request.output_directory)?;

        Ok(ProbeCameraResponse {
            backend: summary.backend.to_string(),
            transport: format!("{:?}", summary.transport),
            recording_program: plan.program_string(),
            recording_args: plan.args,
            output_hint: plan.output_hint,
        })
    }

    pub async fn start_recording(&self, request: &StartRecordingRequest) -> Result<StartRecordingResponse> {
        self.backend.probe_camera(&request.camera)?;
        self.store.upsert_camera(&request.camera)?;

        let response = self.worker.start_recording(request).await?;
        self.store.upsert_session(&RecordingSession {
            camera_id: request.camera.id.clone(),
            worker_name: self.worker.name().to_string(),
            output_directory: request.output_directory.clone(),
            output_hint: Some(response.output_hint.clone()),
            state: RecordingState::Running,
            pid: Some(response.pid),
            started_at_unix_ms: now_unix_ms(),
            updated_at_unix_ms: now_unix_ms(),
            last_error: None,
        })?;
        Ok(response)
    }

    pub async fn stop_recording(&self, request: &StopRecordingRequest) -> Result<StopRecordingResponse> {
        let session = self.store.get_session(&request.camera_id)?;
        let camera = self.store.get_camera(&request.camera_id)?;
        let response = self.worker.stop_recording(request).await?;
        if response.stopped {
            if let (Some(camera), Some(session)) = (camera.as_ref(), session.as_ref()) {
                self.sync_segments_for_session(camera, session, RecordingState::Interrupted)?;
            }
            self.store.delete_session(&request.camera_id)?;
        }
        Ok(response)
    }

    pub async fn restart_recording(&self, request: &RestartRecordingRequest) -> Result<RestartRecordingResponse> {
        self.backend.probe_camera(&request.camera)?;
        self.store.upsert_camera(&request.camera)?;

        let response = self.worker.restart_recording(request).await?;
        self.store.upsert_session(&RecordingSession {
            camera_id: request.camera.id.clone(),
            worker_name: self.worker.name().to_string(),
            output_directory: request.output_directory.clone(),
            output_hint: Some(response.output_hint.clone()),
            state: RecordingState::Running,
            pid: Some(response.pid),
            started_at_unix_ms: now_unix_ms(),
            updated_at_unix_ms: now_unix_ms(),
            last_error: None,
        })?;
        Ok(response)
    }

    pub async fn recording_status(&self, request: &RecordingStatusRequest) -> Result<RecordingStatusResponse> {
        let response = self.worker.recording_status(request).await?;
        let session = self.store.get_session(&request.camera_id)?;
        let camera = self.store.get_camera(&request.camera_id)?;
        match response.state {
            WorkerState::Running | WorkerState::Starting | WorkerState::Healthy | WorkerState::Degraded => {
                if let Some(mut session) = self.store.get_session(&request.camera_id)? {
                    session.state = RecordingState::Running;
                    session.pid = response.pid;
                    session.output_hint = response.output_hint.clone();
                    session.updated_at_unix_ms = now_unix_ms();
                    self.store.upsert_session(&session)?;
                    if let Some(camera) = camera.as_ref() {
                        self.sync_segments_for_session(camera, &session, RecordingState::Running)?;
                    }
                }
            }
            WorkerState::Failed | WorkerState::Stopped => {
                if let (Some(camera), Some(session)) = (camera.as_ref(), session.as_ref()) {
                    let tail_state = if matches!(response.state, WorkerState::Stopped) {
                        RecordingState::Completed
                    } else {
                        RecordingState::Failed
                    };
                    self.sync_segments_for_session(camera, session, tail_state)?;
                }
                self.store.delete_session(&request.camera_id)?;
            }
        }
        Ok(response)
    }

    pub fn sync_recording_segments(&self, request: &SyncRecordingSegmentsRequest) -> Result<SyncRecordingSegmentsResponse> {
        let camera = self
            .store
            .get_camera(&request.camera_id)?
            .ok_or_else(|| anyhow::anyhow!("camera not found: {}", request.camera_id.0))?;
        let session = self
            .store
            .get_session(&request.camera_id)?
            .ok_or_else(|| anyhow::anyhow!("active session not found for camera: {}", request.camera_id.0))?;
        let synced_segments = self.sync_segments_for_session(&camera, &session, RecordingState::Running)?;
        Ok(SyncRecordingSegmentsResponse {
            camera_id: request.camera_id.0.clone(),
            synced_segments,
        })
    }

    pub fn list_recording_segments(&self, request: &ListRecordingSegmentsRequest) -> Result<ListRecordingSegmentsResponse> {
        Ok(ListRecordingSegmentsResponse {
            segments: self.store.list_segments(
                &request.camera_id,
                request.started_after_unix_ms,
                request.started_before_unix_ms,
            )?,
        })
    }

    pub fn storage_summary(&self) -> Result<StorageSummaryResponse> {
        let active_recordings = self.store.list_sessions()?.len();
        self.store.storage_summary(active_recordings)
    }

    pub fn retention_preview(&self, request: &RetentionPreviewRequest) -> Result<RetentionPreviewResponse> {
        self.store.retention_preview(request.retain_after_unix_ms)
    }

    pub fn list_cameras(&self) -> Result<ListCamerasResponse> {
        Ok(ListCamerasResponse {
            cameras: self.store.list_cameras()?,
        })
    }

    pub fn health(&self) -> Result<HealthResponse> {
        let cameras = self.store.list_cameras()?;
        let sessions = self.store.list_sessions()?;
        Ok(HealthResponse {
            status: "ok",
            backend: self.backend.name().to_string(),
            registered_cameras: cameras.len(),
            active_recordings: sessions.len(),
        })
    }
}

impl<B: MediaBackend, S: CameraStore + RecordingSessionStore + RecordingSegmentStore, W: WorkerClient> ControlPlane<B, S, W> {
    fn sync_segments_for_session(
        &self,
        camera: &Camera,
        session: &RecordingSession,
        tail_state: RecordingState,
    ) -> Result<usize> {
        let Some(output_hint) = session.output_hint.as_deref() else {
            return Ok(0);
        };
        let discovered = discover_recording_segments(camera, output_hint, tail_state)?;
        let count = discovered.len();
        for segment in discovered {
            self.store.upsert_segment(&segment)?;
        }
        Ok(count)
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis() as u64
}

fn parse_base_url(base_url: &str) -> Result<(String, u16)> {
    let trimmed = base_url.trim();
    let without_scheme = trimmed
        .strip_prefix("http://")
        .ok_or_else(|| anyhow::anyhow!("unsupported worker url: {trimmed}"))?;
    let authority = without_scheme.trim_end_matches('/');
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("worker url must include host and port: {trimmed}"))?;
    Ok((host.to_string(), port.parse()?))
}

fn parse_json_response<T: DeserializeOwned>(response_bytes: &[u8]) -> Result<T> {
    let response = String::from_utf8(response_bytes.to_vec())?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("invalid worker response"))?;
    let status_line = head
        .lines()
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing worker status line"))?;
    let mut parts = status_line.split_whitespace();
    let _http = parts.next();
    let status_code: u16 = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing worker status code"))?
        .parse()?;

    if status_code >= 400 {
        return Err(anyhow::anyhow!("worker request failed: {status_code} {body}"));
    }

    Ok(serde_json::from_str(body)?)
}

fn discover_recording_segments(
    camera: &Camera,
    output_hint: &str,
    tail_state: RecordingState,
) -> Result<Vec<RecordingSegment>> {
    let (directory, prefix, suffix) = parse_output_pattern(output_hint)?;
    let mut discovered = Vec::new();

    if !directory.exists() {
        return Ok(discovered);
    }

    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !file_name.starts_with(&prefix) || !file_name.ends_with(&suffix) {
            continue;
        }

        let sequence_str = &file_name[prefix.len()..file_name.len() - suffix.len()];
        let Ok(sequence) = sequence_str.parse::<u64>() else {
            continue;
        };

        let metadata = entry.metadata()?;
        let modified = metadata.modified().ok();
        let ended_at = modified.and_then(system_time_to_unix_ms);
        let segment_seconds = camera.preferred_stream(StreamRole::Record).stream_profile.segment_seconds as u64;
        let duration_ms = segment_seconds * 1000;
        let started_at_unix_ms = ended_at
            .map(|value| value.saturating_sub(duration_ms))
            .unwrap_or(0);

        discovered.push(RecordingSegment {
            camera_id: camera.id.clone(),
            sequence,
            path: entry.path().to_string_lossy().replace('\\', "/"),
            started_at_unix_ms,
            ended_at_unix_ms: ended_at,
            duration_ms,
            size_bytes: metadata.len(),
            state: RecordingState::Completed,
        });
    }

    discovered.sort_by_key(|segment| segment.sequence);
    if let Some(last) = discovered.last_mut() {
        last.state = tail_state;
        if matches!(tail_state, RecordingState::Running) {
            last.ended_at_unix_ms = None;
        }
    }

    Ok(discovered)
}

fn parse_output_pattern(output_hint: &str) -> Result<(PathBuf, String, String)> {
    let normalized = output_hint.replace('\\', "/");
    let path = PathBuf::from(normalized);
    let directory = path.parent().ok_or_else(|| anyhow::anyhow!("invalid output hint: {output_hint}"))?.to_path_buf();
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid output file name: {output_hint}"))?;
    let placeholder = "%06d";
    let position = file_name
        .find(placeholder)
        .ok_or_else(|| anyhow::anyhow!("output hint missing segment placeholder: {output_hint}"))?;

    Ok((
        directory,
        file_name[..position].to_string(),
        file_name[position + placeholder.len()..].to_string(),
    ))
}

fn system_time_to_unix_ms(value: SystemTime) -> Option<u64> {
    value.duration_since(UNIX_EPOCH).ok().map(|duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use harborlookout_domain::{
        Camera, CameraId, CameraStream, RtspSource, RtspTransport, StreamProfile, StreamRole, WorkerState,
    };
    use harborlookout_media_core::{MediaBackend, ProbeSummary, RecordingPlan};
    use harborlookout_storage::SqliteCameraStore;

    #[derive(Debug, Clone, Default)]
    struct TestBackend;

    #[derive(Debug, Clone, Default)]
    struct TestWorker;

    impl WorkerClient for TestWorker {
        fn name(&self) -> &str {
            "test-worker"
        }

        fn start_recording<'a>(
            &'a self,
            request: &'a StartRecordingRequest,
        ) -> Pin<Box<dyn Future<Output = Result<StartRecordingResponse>> + Send + 'a>> {
            Box::pin(async move {
                Ok(StartRecordingResponse {
                    backend: "test".into(),
                    program: "test-backend".into(),
                    args: vec![request.camera.id.0.clone()],
                    output_hint: format!("{}/{}-%06d.mp4", request.output_directory, request.camera.id.0),
                    pid: 4242,
                })
            })
        }

        fn stop_recording<'a>(
            &'a self,
            request: &'a StopRecordingRequest,
        ) -> Pin<Box<dyn Future<Output = Result<StopRecordingResponse>> + Send + 'a>> {
            Box::pin(async move {
                Ok(StopRecordingResponse {
                    camera_id: request.camera_id.0.clone(),
                    stopped: true,
                })
            })
        }

        fn restart_recording<'a>(
            &'a self,
            request: &'a RestartRecordingRequest,
        ) -> Pin<Box<dyn Future<Output = Result<RestartRecordingResponse>> + Send + 'a>> {
            Box::pin(async move {
                Ok(RestartRecordingResponse {
                    camera_id: request.camera.id.0.clone(),
                    restarted: true,
                    backend: "test".into(),
                    program: "test-backend".into(),
                    args: vec![request.camera.id.0.clone(), request.output_directory.clone()],
                    output_hint: format!("{}/{}-%06d.mp4", request.output_directory, request.camera.id.0),
                    pid: 5252,
                })
            })
        }

        fn recording_status<'a>(
            &'a self,
            request: &'a RecordingStatusRequest,
        ) -> Pin<Box<dyn Future<Output = Result<RecordingStatusResponse>> + Send + 'a>> {
            Box::pin(async move {
                Ok(RecordingStatusResponse {
                    camera_id: request.camera_id.0.clone(),
                    state: WorkerState::Running,
                    pid: Some(4242),
                    output_hint: Some(format!("recordings/{}-%06d.mp4", request.camera_id.0)),
                })
            })
        }
    }

    impl MediaBackend for TestBackend {
        fn name(&self) -> &'static str {
            "test"
        }

        fn probe_camera(&self, camera: &Camera) -> Result<ProbeSummary> {
            camera.validate()?;
            Ok(ProbeSummary {
                backend: self.name(),
                transport: camera.source.transport.clone(),
            })
        }

        fn build_recording_plan(&self, camera: &Camera, output_directory: &str) -> Result<RecordingPlan> {
            Ok(RecordingPlan {
                program: "test-backend",
                args: vec![camera.id.0.clone(), output_directory.to_string()],
                output_hint: output_directory.to_string(),
            })
        }
    }

    #[test]
    fn registers_camera() {
        let control_plane = ControlPlane::new(TestBackend, SqliteCameraStore::open(":memory:").unwrap(), TestWorker);
        let request = RegisterCameraRequest {
            camera: Camera {
                id: CameraId("cam-lobby".into()),
                name: "Lobby".into(),
                streams: vec![CameraStream {
                    role: StreamRole::Record,
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
                }],
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
            },
        };

        let response = control_plane.register_camera(&request).unwrap();
        assert_eq!(response.camera_id, "cam-lobby");
        assert_eq!(control_plane.health().unwrap().registered_cameras, 1);
        assert_eq!(control_plane.health().unwrap().active_recordings, 0);
        assert_eq!(control_plane.list_cameras().unwrap().cameras.len(), 1);
    }

    #[test]
    fn probes_camera() {
        let control_plane = ControlPlane::new(TestBackend, SqliteCameraStore::open(":memory:").unwrap(), TestWorker);
        let request = ProbeCameraRequest {
            camera: Camera {
                id: CameraId("cam-probe".into()),
                name: "Probe".into(),
                streams: vec![
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
                            segment_seconds: 6,
                        },
                    },
                    CameraStream {
                        role: StreamRole::Record,
                        source: RtspSource {
                            url: "rtsp://camera.local/probe".into(),
                            transport: RtspTransport::Udp,
                        },
                        stream_profile: StreamProfile {
                            width: 1280,
                            height: 720,
                            fps: 20,
                            segment_seconds: 6,
                        },
                    },
                ],
                source: RtspSource {
                    url: "rtsp://camera.local/probe".into(),
                    transport: RtspTransport::Udp,
                },
                stream_profile: StreamProfile {
                    width: 1280,
                    height: 720,
                    fps: 20,
                    segment_seconds: 6,
                },
                enabled: true,
            },
            output_directory: "recordings".into(),
        };

        let response = control_plane.probe_camera(&request).unwrap();
        assert_eq!(response.backend, "test");
        assert_eq!(response.recording_program, "test-backend");
    }

    #[tokio::test]
    async fn starts_recording_and_persists_session() {
        let store = SqliteCameraStore::open(":memory:").unwrap();
        let control_plane = ControlPlane::new(TestBackend, store, TestWorker);
        let request = StartRecordingRequest {
            camera: Camera {
                id: CameraId("cam-start".into()),
                name: "Start".into(),
                streams: vec![CameraStream {
                    role: StreamRole::Record,
                    source: RtspSource {
                        url: "rtsp://camera.local/start".into(),
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
                    url: "rtsp://camera.local/start".into(),
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
            output_directory: "recordings".into(),
        };

        let response = control_plane.start_recording(&request).await.unwrap();
        assert_eq!(response.pid, 4242);
        assert_eq!(control_plane.health().unwrap().active_recordings, 1);
    }

    #[test]
    fn reports_storage_summary() {
        let store = SqliteCameraStore::open(":memory:").unwrap();
        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-summary".into()),
                sequence: 1,
                path: "segments/cam-summary-000001.mp4".into(),
                started_at_unix_ms: 10,
                ended_at_unix_ms: Some(20),
                duration_ms: 10,
                size_bytes: 64,
                state: RecordingState::Completed,
            })
            .unwrap();
        let control_plane = ControlPlane::new(TestBackend, store, TestWorker);

        let summary = control_plane.storage_summary().unwrap();
        assert_eq!(summary.total_segments, 1);
        assert_eq!(summary.total_bytes, 64);

        let preview = control_plane
            .retention_preview(&RetentionPreviewRequest {
                retain_after_unix_ms: 100,
            })
            .unwrap();
        assert_eq!(preview.reclaimable_segments, 1);
        assert_eq!(preview.reclaimable_bytes, 64);
    }

    #[test]
    fn discovers_segments_from_output_pattern() {
        let temp_dir = std::env::temp_dir().join(format!("harborlookout-segments-{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();
        fs::write(temp_dir.join("cam-sync-000001.mp4"), b"one").unwrap();
        fs::write(temp_dir.join("cam-sync-000002.mp4"), b"two").unwrap();

        let camera = Camera {
            id: CameraId("cam-sync".into()),
            name: "Sync".into(),
            streams: vec![CameraStream {
                role: StreamRole::Record,
                source: RtspSource {
                    url: "rtsp://camera.local/sync".into(),
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
                url: "rtsp://camera.local/sync".into(),
                transport: RtspTransport::Tcp,
            },
            stream_profile: StreamProfile {
                width: 1280,
                height: 720,
                fps: 15,
                segment_seconds: 10,
            },
            enabled: true,
        };

        let output_hint = temp_dir.join("cam-sync-%06d.mp4");
        let segments = discover_recording_segments(
            &camera,
            &output_hint.to_string_lossy(),
            RecordingState::Running,
        )
        .unwrap();

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].sequence, 1);
        assert_eq!(segments[0].state, RecordingState::Completed);
        assert_eq!(segments[1].sequence, 2);
        assert_eq!(segments[1].state, RecordingState::Running);

        let _ = fs::remove_dir_all(&temp_dir);
    }
}