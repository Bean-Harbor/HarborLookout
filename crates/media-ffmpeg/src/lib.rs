use anyhow::Result;
use harborlookout_domain::{Camera, RtspTransport, StreamRole};
use harborlookout_media_core::{MediaBackend, ProbeSummary, RecordingPlan, SnapshotPlan};

#[derive(Debug, Clone, Default)]
pub struct FfmpegBackend;

impl MediaBackend for FfmpegBackend {
    fn name(&self) -> &'static str {
        "ffmpeg"
    }

    fn probe_camera(&self, camera: &Camera) -> Result<ProbeSummary> {
        camera.validate()?;
        let stream = camera.preferred_stream(StreamRole::Record);

        Ok(ProbeSummary {
            backend: self.name(),
            transport: stream.source.transport,
        })
    }

    fn build_recording_plan(&self, camera: &Camera, output_directory: &str) -> Result<RecordingPlan> {
        camera.validate()?;

        let stream = camera.preferred_stream(StreamRole::Record);
        let transport = match stream.source.transport {
            RtspTransport::Tcp => "tcp",
            RtspTransport::Udp => "udp",
            RtspTransport::Auto => "prefer_tcp",
        };

        let output_pattern = format!(
            "{}/{}-%06d.mp4",
            output_directory.trim_end_matches('/').trim_end_matches('\\'),
            camera.id.0
        );

        Ok(RecordingPlan {
            program: ffmpeg_program(),
            args: vec![
                "-hide_banner".into(),
                "-loglevel".into(),
                "warning".into(),
                "-rtsp_transport".into(),
                transport.into(),
                "-i".into(),
                stream.source.url,
                "-map".into(),
                "0:v:0".into(),
                "-c:v".into(),
                "copy".into(),
                "-f".into(),
                "segment".into(),
                "-segment_time".into(),
                stream.stream_profile.segment_seconds.to_string(),
                "-reset_timestamps".into(),
                "1".into(),
                output_pattern.clone(),
            ],
            output_hint: output_pattern,
        })
    }

    fn build_snapshot_plan(&self, camera: &Camera, output_path: &str) -> Result<SnapshotPlan> {
        camera.validate()?;

        let stream = camera.preferred_stream(StreamRole::Detect);
        let transport = match stream.source.transport {
            RtspTransport::Tcp => "tcp",
            RtspTransport::Udp => "udp",
            RtspTransport::Auto => "prefer_tcp",
        };

        Ok(SnapshotPlan {
            program: ffmpeg_program(),
            args: vec![
                "-hide_banner".into(),
                "-loglevel".into(),
                "warning".into(),
                "-y".into(),
                "-rtsp_transport".into(),
                transport.into(),
                "-i".into(),
                stream.source.url,
                "-frames:v".into(),
                "1".into(),
                "-q:v".into(),
                "2".into(),
                output_path.to_string(),
            ],
            output_path: output_path.to_string(),
        })
    }
}

fn ffmpeg_program() -> String {
    std::env::var("HARBORLOOKOUT_FFMPEG_PROGRAM").unwrap_or_else(|_| "ffmpeg".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use harborlookout_domain::{CameraId, CameraStream, RtspSource, StreamProfile};
    use std::sync::{Mutex, OnceLock};

    fn env_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn builds_recording_plan() {
        let _lock = env_test_lock().lock().unwrap();
        let _guard = EnvVarGuard::remove("HARBORLOOKOUT_FFMPEG_PROGRAM");
        let backend = FfmpegBackend;
        let camera = Camera {
            id: CameraId("cam-yard".into()),
            name: "Yard".into(),
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
                        segment_seconds: 8,
                    },
                },
                CameraStream {
                    role: StreamRole::Record,
                    source: RtspSource {
                        url: "rtsp://camera.local/live".into(),
                        transport: RtspTransport::Tcp,
                    },
                    stream_profile: StreamProfile {
                        width: 1280,
                        height: 720,
                        fps: 15,
                        segment_seconds: 8,
                    },
                },
            ],
            source: RtspSource {
                url: "rtsp://camera.local/live".into(),
                transport: RtspTransport::Tcp,
            },
            stream_profile: StreamProfile {
                width: 1280,
                height: 720,
                fps: 15,
                segment_seconds: 8,
            },
            enabled: true,
        };

        let plan = backend.build_recording_plan(&camera, "recordings").unwrap();
        assert_eq!(plan.program, "ffmpeg");
        assert!(plan.output_hint.contains("cam-yard"));
        assert!(plan.args.contains(&"rtsp://camera.local/live".to_string()));
    }

    #[test]
    fn builds_snapshot_plan_from_detect_stream() {
        let _lock = env_test_lock().lock().unwrap();
        let _guard = EnvVarGuard::remove("HARBORLOOKOUT_FFMPEG_PROGRAM");
        let backend = FfmpegBackend;
        let camera = Camera {
            id: CameraId("cam-yard".into()),
            name: "Yard".into(),
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
                        segment_seconds: 8,
                    },
                },
                CameraStream {
                    role: StreamRole::Record,
                    source: RtspSource {
                        url: "rtsp://camera.local/live".into(),
                        transport: RtspTransport::Udp,
                    },
                    stream_profile: StreamProfile {
                        width: 1280,
                        height: 720,
                        fps: 15,
                        segment_seconds: 8,
                    },
                },
            ],
            source: RtspSource {
                url: "rtsp://camera.local/live".into(),
                transport: RtspTransport::Udp,
            },
            stream_profile: StreamProfile {
                width: 1280,
                height: 720,
                fps: 15,
                segment_seconds: 8,
            },
            enabled: true,
        };

        let plan = backend.build_snapshot_plan(&camera, "samples/cam-yard.jpg").unwrap();
        assert_eq!(plan.program, "ffmpeg");
        assert!(plan.args.contains(&"rtsp://camera.local/detect".to_string()));
        assert_eq!(plan.output_path, "samples/cam-yard.jpg");
    }

    #[test]
    fn honors_ffmpeg_program_override() {
        let _lock = env_test_lock().lock().unwrap();
        let _guard = EnvVarGuard::set("HARBORLOOKOUT_FFMPEG_PROGRAM", "fake-ffmpeg.cmd");
        let backend = FfmpegBackend;
        let camera = Camera {
            id: CameraId("cam-yard".into()),
            name: "Yard".into(),
            streams: vec![],
            source: RtspSource {
                url: "rtsp://camera.local/live".into(),
                transport: RtspTransport::Tcp,
            },
            stream_profile: StreamProfile {
                width: 1280,
                height: 720,
                fps: 15,
                segment_seconds: 8,
            },
            enabled: true,
        };

        let plan = backend.build_snapshot_plan(&camera, "samples/cam-yard.jpg").unwrap();
        assert_eq!(plan.program, "fake-ffmpeg.cmd");
    }

    struct EnvVarGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, original }
        }

        fn remove(key: &'static str) -> Self {
            let original = std::env::var(key).ok();
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }
}