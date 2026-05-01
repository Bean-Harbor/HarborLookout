use std::env;
use std::fs;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail};
use harborlookout_domain::{EventKind, EventSeverity};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    frame_path: String,
    event_kind: Option<EventKind>,
    event_severity: Option<EventSeverity>,
    backend: DetectorBackend,
    min_bytes: u64,
    message: Option<String>,
    always_detect: bool,
    never_detect: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DetectorBackend {
    Heuristic,
    Runner {
        program: String,
        args: Vec<String>,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct DetectionResult {
    detected: bool,
    message: Option<String>,
    event_kind: Option<EventKind>,
    event_severity: Option<EventSeverity>,
}

fn main() -> Result<()> {
    let config = parse_args(env::args().skip(1).collect(), EnvDefaults::from_process())?;
    let result = evaluate_detection(&config)?;

    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

#[derive(Debug, Clone, Default)]
struct EnvDefaults {
    frame_path: Option<String>,
    event_kind: Option<String>,
    event_severity: Option<String>,
}

impl EnvDefaults {
    fn from_process() -> Self {
        Self {
            frame_path: env::var("HARBORLOOKOUT_FRAME_PATH").ok(),
            event_kind: env::var("HARBORLOOKOUT_EVENT_KIND").ok(),
            event_severity: env::var("HARBORLOOKOUT_EVENT_SEVERITY").ok(),
        }
    }
}

fn parse_args(args: Vec<String>, defaults: EnvDefaults) -> Result<Config> {
    if matches!(args.first().map(String::as_str), Some("--help") | Some("-h")) {
        bail!(usage())
    }

    let mut remaining = args;
    let frame_path = take_option(&mut remaining, "--frame")?
        .or(defaults.frame_path)
        .or_else(|| remaining.first().cloned())
        .ok_or_else(|| anyhow!("missing frame path; pass --frame or set HARBORLOOKOUT_FRAME_PATH"))?;
    if !remaining.is_empty() && !remaining[0].starts_with("--") {
        remaining.remove(0);
    }

    let event_kind = take_option(&mut remaining, "--event-kind")?
        .or(defaults.event_kind)
        .map(|value| parse_event_kind(&value))
        .transpose()?;
    let event_severity = take_option(&mut remaining, "--event-severity")?
        .or(defaults.event_severity)
        .map(|value| parse_event_severity(&value))
        .transpose()?;
    let backend = parse_backend(&mut remaining)?;
    let min_bytes = take_option(&mut remaining, "--min-bytes")?
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("invalid integer for --min-bytes: {value}"))
        })
        .transpose()?
        .unwrap_or(1);
    let message = take_option(&mut remaining, "--message")?;
    let always_detect = take_flag(&mut remaining, "--always-detect");
    let never_detect = take_flag(&mut remaining, "--never-detect");

    if always_detect && never_detect {
        bail!("--always-detect and --never-detect cannot be used together")
    }
    if !remaining.is_empty() {
        bail!("unexpected arguments: {}", remaining.join(" "))
    }

    Ok(Config {
        frame_path,
        event_kind,
        event_severity,
        backend,
        min_bytes,
        message,
        always_detect,
        never_detect,
    })
}

fn parse_backend(args: &mut Vec<String>) -> Result<DetectorBackend> {
    let backend_name = take_option(args, "--backend")?.unwrap_or_else(|| "heuristic".into());
    match backend_name.as_str() {
        "heuristic" => Ok(DetectorBackend::Heuristic),
        "runner" => {
            let program = required_option(args, "--runner-program")?;
            let runner_args = take_options(args, "--runner-arg")?;
            Ok(DetectorBackend::Runner {
                program,
                args: runner_args,
            })
        }
        other => bail!("unknown backend: {other}"),
    }
}

fn evaluate_detection(config: &Config) -> Result<DetectionResult> {
    if config.never_detect {
        return Ok(detection_result(config, false, None, None, None));
    }

    let metadata = fs::metadata(&config.frame_path)
        .with_context(|| format!("failed to read sampled frame: {}", config.frame_path))?;
    if !metadata.is_file() {
        bail!("sampled frame is not a file: {}", config.frame_path)
    }

    if config.always_detect {
        return Ok(detection_result(config, true, None, None, None));
    }

    match &config.backend {
        DetectorBackend::Heuristic => Ok(detection_result(
            config,
            metadata.len() >= config.min_bytes,
            None,
            None,
            Some("heuristic adapter"),
        )),
        DetectorBackend::Runner { program, args } => run_runner_backend(config, program, args),
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

fn required_option(args: &mut Vec<String>, flag: &str) -> Result<String> {
    take_option(args, flag)?.ok_or_else(|| anyhow!("missing value for option: {flag}"))
}

fn take_options(args: &mut Vec<String>, flag: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    while let Some(index) = args.iter().position(|arg| arg == flag) {
        if index + 1 >= args.len() {
            bail!("missing value for option: {flag}")
        }
        values.push(args.remove(index + 1));
        args.remove(index);
    }
    Ok(values)
}

fn take_flag(args: &mut Vec<String>, flag: &str) -> bool {
    if let Some(index) = args.iter().position(|arg| arg == flag) {
        args.remove(index);
        true
    } else {
        false
    }
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

fn parse_event_severity(value: &str) -> Result<EventSeverity> {
    match value {
        "Info" => Ok(EventSeverity::Info),
        "Warning" => Ok(EventSeverity::Warning),
        "Critical" => Ok(EventSeverity::Critical),
        other => bail!("unknown event severity: {other}"),
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

fn detection_result(
    config: &Config,
    detected: bool,
    message: Option<String>,
    event_kind: Option<EventKind>,
    source: Option<&str>,
) -> DetectionResult {
    DetectionResult {
        detected,
        message: detected.then(|| {
            message.unwrap_or_else(|| {
                config.message.clone().unwrap_or_else(|| {
                    format!(
                        "{} detected by {}",
                        config
                            .event_kind
                            .as_ref()
                            .or(event_kind.as_ref())
                            .map(event_kind_name)
                            .unwrap_or("event"),
                        source.unwrap_or("detector adapter")
                    )
                })
            })
        }),
        event_kind: detected.then(|| event_kind.or_else(|| config.event_kind.clone())).flatten(),
        event_severity: detected.then(|| config.event_severity.clone()).flatten(),
    }
}

fn run_runner_backend(config: &Config, program: &str, args: &[String]) -> Result<DetectionResult> {
    let resolved_args = args
        .iter()
        .map(|arg| resolve_placeholder(arg, config))
        .collect::<Vec<_>>();

    let output = Command::new(program)
        .args(&resolved_args)
        .env("HARBORLOOKOUT_FRAME_PATH", &config.frame_path)
        .env(
            "HARBORLOOKOUT_EVENT_KIND",
            config
                .event_kind
                .as_ref()
                .map(debug_event_kind)
                .unwrap_or_default(),
        )
        .env(
            "HARBORLOOKOUT_EVENT_SEVERITY",
            config
                .event_severity
                .as_ref()
                .map(debug_event_severity)
                .unwrap_or_default(),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run runner backend: {program}"))?;

    if !output.status.success() {
        bail!(
            "runner backend `{program}` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
    }

    let stdout = String::from_utf8(output.stdout)?;
    let normalized: RunnerDetectionResult = serde_json::from_str(stdout.trim())
        .with_context(|| format!("runner backend `{program}` returned invalid JSON"))?;

    Ok(detection_result(
        config,
        normalized.detected,
        normalized.message,
        normalized.event_kind,
        Some(program),
    ))
}

fn resolve_placeholder(template: &str, config: &Config) -> String {
    template
        .replace("{frame_path}", &config.frame_path)
        .replace(
            "{event_kind}",
            &config
                .event_kind
                .as_ref()
                .map(debug_event_kind)
                .unwrap_or_default(),
        )
        .replace(
            "{event_severity}",
            &config
                .event_severity
                .as_ref()
                .map(debug_event_severity)
                .unwrap_or_default(),
        )
}

fn debug_event_kind(kind: &EventKind) -> String {
    format!("{kind:?}")
}

fn debug_event_severity(severity: &EventSeverity) -> String {
    format!("{severity:?}")
}

#[derive(Debug, Deserialize)]
struct RunnerDetectionResult {
    detected: bool,
    message: Option<String>,
    event_kind: Option<EventKind>,
}

fn usage() -> &'static str {
    "HarborLookout detector adapter\n\nUsage:\n  harborlookout-detector-adapter --frame sample.jpg [--backend heuristic] [--event-kind PackageDetected] [--event-severity Warning] [--min-bytes 1] [--message text] [--always-detect|--never-detect]\n  harborlookout-detector-adapter --frame sample.jpg --backend runner --runner-program model-runner.exe [--runner-arg {frame_path}] [--runner-arg {event_kind}] [--runner-arg {event_severity}]\n\nEnvironment:\n  HARBORLOOKOUT_FRAME_PATH\n  HARBORLOOKOUT_EVENT_KIND\n  HARBORLOOKOUT_EVENT_SEVERITY"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cli_with_defaults() {
        let config = parse_args(
            vec!["--always-detect".into()],
            EnvDefaults {
                frame_path: Some("sample.jpg".into()),
                event_kind: Some("PackageDetected".into()),
                event_severity: Some("Warning".into()),
            },
        )
        .unwrap();

        assert_eq!(config.frame_path, "sample.jpg");
        assert_eq!(config.event_kind, Some(EventKind::PackageDetected));
        assert_eq!(config.event_severity, Some(EventSeverity::Warning));
        assert_eq!(config.backend, DetectorBackend::Heuristic);
        assert!(config.always_detect);
    }

    #[test]
    fn rejects_conflicting_detection_flags() {
        let error = parse_args(
            vec![
                "--frame".into(),
                "sample.jpg".into(),
                "--always-detect".into(),
                "--never-detect".into(),
            ],
            EnvDefaults::default(),
        )
        .expect_err("conflicting flags should fail");

        assert!(error.to_string().contains("cannot be used together"));
    }

    #[test]
    fn evaluates_positive_detection_by_file_size() {
        let temp_root = std::env::temp_dir().join(format!(
            "harborlookout-detector-adapter-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(&temp_root).unwrap();
        let frame_path = temp_root.join("frame.jpg");
        fs::write(&frame_path, b"frame-data").unwrap();

        let detected = evaluate_detection(&Config {
            frame_path: frame_path.to_string_lossy().to_string(),
            event_kind: Some(EventKind::PackageDetected),
            event_severity: Some(EventSeverity::Warning),
            backend: DetectorBackend::Heuristic,
            min_bytes: 1,
            message: None,
            always_detect: false,
            never_detect: false,
        })
        .unwrap();

        assert!(detected.detected);
        let _ = fs::remove_dir_all(&temp_root);
    }

    #[test]
    fn evaluates_negative_detection_when_threshold_not_met() {
        let temp_root = std::env::temp_dir().join(format!(
            "harborlookout-detector-adapter-threshold-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(&temp_root).unwrap();
        let frame_path = temp_root.join("frame.jpg");
        fs::write(&frame_path, b"x").unwrap();

        let detected = evaluate_detection(&Config {
            frame_path: frame_path.to_string_lossy().to_string(),
            event_kind: None,
            event_severity: None,
            backend: DetectorBackend::Heuristic,
            min_bytes: 100,
            message: None,
            always_detect: false,
            never_detect: false,
        })
        .unwrap();

        assert!(!detected.detected);
        let _ = fs::remove_dir_all(&temp_root);
    }

    #[test]
    fn parses_runner_backend() {
        let config = parse_args(
            vec![
                "--frame".into(),
                "sample.jpg".into(),
                "--backend".into(),
                "runner".into(),
                "--runner-program".into(),
                "model-runner.exe".into(),
                "--runner-arg".into(),
                "{frame_path}".into(),
            ],
            EnvDefaults::default(),
        )
        .unwrap();

        assert_eq!(
            config.backend,
            DetectorBackend::Runner {
                program: "model-runner.exe".into(),
                args: vec!["{frame_path}".into()],
            }
        );
    }

    #[test]
    fn resolves_runner_placeholders() {
        let config = Config {
            frame_path: "sample.jpg".into(),
            event_kind: Some(EventKind::PackageDetected),
            event_severity: Some(EventSeverity::Warning),
            backend: DetectorBackend::Heuristic,
            min_bytes: 1,
            message: None,
            always_detect: false,
            never_detect: false,
        };

        assert_eq!(
            resolve_placeholder("--frame={frame_path}:{event_kind}:{event_severity}", &config),
            "--frame=sample.jpg:PackageDetected:Warning"
        );
    }

    #[test]
    fn serializes_detection_result() {
        let payload = serde_json::to_string(&DetectionResult {
            detected: true,
            message: Some("package detected".into()),
            event_kind: Some(EventKind::PackageDetected),
            event_severity: Some(EventSeverity::Warning),
        })
        .unwrap();

        assert!(payload.contains("\"detected\":true"));
        assert!(payload.contains("PackageDetected"));
    }
}