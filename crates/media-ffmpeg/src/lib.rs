use anyhow::Result;
use harborlookout_domain::{Camera, RtspTransport, StreamRole};
use harborlookout_media_core::{MediaBackend, ProbeSummary, RecordingPlan};

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
            program: "ffmpeg",
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use harborlookout_domain::{CameraId, CameraStream, RtspSource, StreamProfile};

    #[test]
    fn builds_recording_plan() {
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
}