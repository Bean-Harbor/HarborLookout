use std::env;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail};
use harborlookout_domain::EventKind;
use serde::{Deserialize, Serialize};

const YOLO_INLINE_SCRIPT: &str = r#"
import json
import sys

from ultralytics import YOLO

image_path = sys.argv[1]
model_path = sys.argv[2]
confidence = float(sys.argv[3])

model = YOLO(model_path)
results = model.predict(source=image_path, conf=confidence, verbose=False)
detections = []
for result in results:
    names = result.names
    boxes = getattr(result, 'boxes', None)
    if boxes is None:
        continue
    for cls, conf in zip(boxes.cls.tolist(), boxes.conf.tolist()):
        class_name = names.get(int(cls), str(int(cls)))
        detections.append({"class_name": class_name, "confidence": conf})

print(json.dumps({"detections": detections}))
"#;

#[derive(Debug, Clone, PartialEq)]
struct Config {
    image_path: String,
    model_path: String,
    python_program: String,
    min_confidence: f32,
    fallback_kind: Option<EventKind>,
    message: Option<String>,
    mock_output_file: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct RunnerOutput {
    detected: bool,
    message: Option<String>,
    event_kind: Option<EventKind>,
}

#[derive(Debug, Deserialize)]
struct RawDetectionBatch {
    detections: Vec<RawDetection>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
struct RawDetection {
    class_name: String,
    confidence: f32,
}

fn main() -> Result<()> {
    let config = parse_args(env::args().skip(1).collect())?;
    let detections = run_yolo(&config)?;
    let output = normalize_detections(&config, &detections);
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

fn parse_args(args: Vec<String>) -> Result<Config> {
    if matches!(args.first().map(String::as_str), Some("--help") | Some("-h")) {
        bail!(usage())
    }

    let mut remaining = args;
    let image_path = take_option(&mut remaining, "--image")?
        .or_else(|| env::var("HARBORLOOKOUT_FRAME_PATH").ok())
        .or_else(|| remaining.first().cloned())
        .ok_or_else(|| anyhow!("missing image path; pass --image or set HARBORLOOKOUT_FRAME_PATH"))?;
    if !remaining.is_empty() && !remaining[0].starts_with("--") {
        remaining.remove(0);
    }

    let model_path = take_option(&mut remaining, "--model")?
        .or_else(|| env::var("HARBORLOOKOUT_YOLO_MODEL").ok())
        .ok_or_else(|| anyhow!("missing model path; pass --model or set HARBORLOOKOUT_YOLO_MODEL"))?;
    let python_program = take_option(&mut remaining, "--python-program")?
        .or_else(|| env::var("HARBORLOOKOUT_PYTHON").ok())
        .unwrap_or_else(|| "python".into());
    let min_confidence = take_option(&mut remaining, "--min-confidence")?
        .map(|value| {
            value
                .parse::<f32>()
                .with_context(|| format!("invalid float for --min-confidence: {value}"))
        })
        .transpose()?
        .unwrap_or(0.25);
    let fallback_kind = take_option(&mut remaining, "--fallback-kind")?
        .or_else(|| env::var("HARBORLOOKOUT_EVENT_KIND").ok())
        .map(|value| parse_event_kind(&value))
        .transpose()?;
    let message = take_option(&mut remaining, "--message")?;
    let mock_output_file = take_option(&mut remaining, "--mock-output-file")?;

    if !remaining.is_empty() {
        bail!("unexpected arguments: {}", remaining.join(" "))
    }

    Ok(Config {
        image_path,
        model_path,
        python_program,
        min_confidence,
        fallback_kind,
        message,
        mock_output_file,
    })
}

fn run_yolo(config: &Config) -> Result<Vec<RawDetection>> {
    if let Some(mock_output_file) = &config.mock_output_file {
        let content = std::fs::read_to_string(mock_output_file)
            .with_context(|| format!("failed to read mock output file: {mock_output_file}"))?;
        let batch: RawDetectionBatch = serde_json::from_str(&content)
            .context("mock YOLO output file contained invalid JSON")?;
        return Ok(batch.detections);
    }

    let output = Command::new(&config.python_program)
        .args([
            "-c",
            YOLO_INLINE_SCRIPT,
            &config.image_path,
            &config.model_path,
            &config.min_confidence.to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to launch python program `{}`", config.python_program))?;

    if !output.status.success() {
        bail!(
            "YOLO runner failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
    }

    let batch: RawDetectionBatch = serde_json::from_slice(&output.stdout)
        .context("YOLO runner returned invalid JSON")?;
    Ok(batch.detections)
}

fn normalize_detections(config: &Config, detections: &[RawDetection]) -> RunnerOutput {
    let best_supported = detections
        .iter()
        .filter_map(|detection| {
            map_class_name_to_event_kind(&detection.class_name).map(|kind| (detection, kind))
        })
        .max_by(|left, right| left.0.confidence.total_cmp(&right.0.confidence));

    if let Some((best, kind)) = best_supported {
        return RunnerOutput {
            detected: true,
            message: Some(
                config
                    .message
                    .clone()
                    .unwrap_or_else(|| format!("{} detected by YOLO runner", best.class_name)),
            ),
            event_kind: Some(kind),
        };
    }

    if !detections.is_empty() {
        if let Some(kind) = config.fallback_kind.clone() {
            return RunnerOutput {
                detected: true,
                message: Some(
                    config
                        .message
                        .clone()
                        .unwrap_or_else(|| format!("{} detected by YOLO runner", event_kind_name(&kind))),
                ),
                event_kind: Some(kind),
            };
        }
    }

    RunnerOutput {
        detected: false,
        message: None,
        event_kind: None,
    }
}

fn map_class_name_to_event_kind(class_name: &str) -> Option<EventKind> {
    match class_name.trim().to_ascii_lowercase().as_str() {
        "person" => Some(EventKind::PersonDetected),
        "car" | "truck" | "bus" | "motorcycle" | "bicycle" => Some(EventKind::VehicleDetected),
        "dog" | "cat" | "bird" => Some(EventKind::PetDetected),
        "package" | "parcel" | "box" => Some(EventKind::PackageDetected),
        "bottle" | "cup" | "can" | "wine glass" => Some(EventKind::DrinkContainerDetected),
        _ => None,
    }
}

fn take_option(args: &mut Vec<String>, flag: &str) -> Result<Option<String>> {
    let Some(index) = args.iter().position(|arg| arg == flag) else {
        return Ok(None);
    };
    if index + 1 >= args.len() {
        bail!("missing value for option: {flag}")
    }

    let value = args.remove(index + 1);
    args.remove(index);
    Ok(Some(value))
}

fn parse_event_kind(value: &str) -> Result<EventKind> {
    match value {
        "MotionDetected" => Ok(EventKind::MotionDetected),
        "MotionInZone" => Ok(EventKind::MotionInZone),
        "PersonDetected" => Ok(EventKind::PersonDetected),
        "VehicleDetected" => Ok(EventKind::VehicleDetected),
        "PetDetected" => Ok(EventKind::PetDetected),
        "PackageDetected" => Ok(EventKind::PackageDetected),
        "DrinkContainerDetected" => Ok(EventKind::DrinkContainerDetected),
        "ObjectRemoved" => Ok(EventKind::ObjectRemoved),
        "SpillCandidateDetected" => Ok(EventKind::SpillCandidateDetected),
        "RecordingStarted" => Ok(EventKind::RecordingStarted),
        "RecordingStopped" => Ok(EventKind::RecordingStopped),
        "CameraOffline" => Ok(EventKind::CameraOffline),
        "CameraOnline" => Ok(EventKind::CameraOnline),
        other => bail!("unknown event kind: {other}"),
    }
}

fn event_kind_name(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::MotionDetected => "motion",
        EventKind::MotionInZone => "motion in zone",
        EventKind::PersonDetected => "person",
        EventKind::VehicleDetected => "vehicle",
        EventKind::PetDetected => "pet",
        EventKind::PackageDetected => "package",
        EventKind::DrinkContainerDetected => "drink container",
        EventKind::ObjectRemoved => "object removed",
        EventKind::SpillCandidateDetected => "spill candidate",
        EventKind::RecordingStarted => "recording started",
        EventKind::RecordingStopped => "recording stopped",
        EventKind::CameraOffline => "camera offline",
        EventKind::CameraOnline => "camera online",
    }
}

fn usage() -> &'static str {
    "HarborLookout YOLO model runner\n\nUsage:\n  harborlookout-model-runner-yolo --image sample.jpg --model yolov8n.pt [--python-program python] [--min-confidence 0.25] [--fallback-kind PackageDetected] [--message text]\n  harborlookout-model-runner-yolo --image sample.jpg --model yolov8n.pt --mock-output-file detections.json\n\nPrerequisites:\n  python with `ultralytics` installed\n\nEnvironment:\n  HARBORLOOKOUT_FRAME_PATH\n  HARBORLOOKOUT_YOLO_MODEL\n  HARBORLOOKOUT_PYTHON\n  HARBORLOOKOUT_EVENT_KIND"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_missing_model() {
        let error = parse_args(vec!["--image".into(), "frame.jpg".into()])
            .expect_err("missing model should fail");
        assert!(error.to_string().contains("missing model path"));
    }

    #[test]
    fn maps_supported_labels() {
        assert_eq!(map_class_name_to_event_kind("person"), Some(EventKind::PersonDetected));
        assert_eq!(map_class_name_to_event_kind("truck"), Some(EventKind::VehicleDetected));
        assert_eq!(map_class_name_to_event_kind("box"), Some(EventKind::PackageDetected));
    }

    #[test]
    fn normalizes_best_supported_detection() {
        let output = normalize_detections(
            &Config {
                image_path: "frame.jpg".into(),
                model_path: "yolov8n.pt".into(),
                python_program: "python".into(),
                min_confidence: 0.25,
                fallback_kind: Some(EventKind::PackageDetected),
                message: None,
                mock_output_file: None,
            },
            &[
                RawDetection {
                    class_name: "box".into(),
                    confidence: 0.62,
                },
                RawDetection {
                    class_name: "person".into(),
                    confidence: 0.91,
                },
            ],
        );

        assert!(output.detected);
        assert_eq!(output.event_kind, Some(EventKind::PersonDetected));
    }

    #[test]
    fn falls_back_to_requested_kind_for_unknown_labels() {
        let output = normalize_detections(
            &Config {
                image_path: "frame.jpg".into(),
                model_path: "yolov8n.pt".into(),
                python_program: "python".into(),
                min_confidence: 0.25,
                fallback_kind: Some(EventKind::PackageDetected),
                message: None,
                mock_output_file: None,
            },
            &[RawDetection {
                class_name: "chair".into(),
                confidence: 0.88,
            }],
        );

        assert!(output.detected);
        assert_eq!(output.event_kind, Some(EventKind::PackageDetected));
    }

    #[test]
    fn returns_negative_when_no_detections() {
        let output = normalize_detections(
            &Config {
                image_path: "frame.jpg".into(),
                model_path: "yolov8n.pt".into(),
                python_program: "python".into(),
                min_confidence: 0.25,
                fallback_kind: Some(EventKind::PackageDetected),
                message: None,
                mock_output_file: None,
            },
            &[],
        );

        assert!(!output.detected);
        assert!(output.event_kind.is_none());
    }

    #[test]
    fn reads_mock_output_file() {
        let temp_root = std::env::temp_dir().join(format!(
            "harborlookout-model-runner-yolo-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&temp_root);
        std::fs::create_dir_all(&temp_root).unwrap();
        let mock_file = temp_root.join("detections.json");
        std::fs::write(&mock_file, r#"{"detections":[{"class_name":"person","confidence":0.99}]}"#).unwrap();

        let detections = run_yolo(&Config {
            image_path: "frame.jpg".into(),
            model_path: "yolov8n.pt".into(),
            python_program: "python".into(),
            min_confidence: 0.25,
            fallback_kind: Some(EventKind::PackageDetected),
            message: None,
            mock_output_file: Some(mock_file.to_string_lossy().to_string()),
        })
        .unwrap();

        assert_eq!(detections.len(), 1);
        assert_eq!(detections[0].class_name, "person");
        let _ = std::fs::remove_dir_all(&temp_root);
    }
}