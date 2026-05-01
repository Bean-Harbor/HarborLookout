use std::env;
use std::fs;

use anyhow::{Context, Result, anyhow, bail};
use harborlookout_domain::EventKind;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    image_path: String,
    label: Option<EventKind>,
    min_bytes: u64,
    message: Option<String>,
    force_positive: bool,
    force_negative: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct RunnerOutput {
    detected: bool,
    message: Option<String>,
    event_kind: Option<EventKind>,
}

fn main() -> Result<()> {
    let config = parse_args(env::args().skip(1).collect())?;
    let output = evaluate(&config)?;
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

    let label = take_option(&mut remaining, "--label")?
        .or_else(|| env::var("HARBORLOOKOUT_EVENT_KIND").ok())
        .map(|value| parse_event_kind(&value))
        .transpose()?;
    let min_bytes = take_option(&mut remaining, "--min-bytes")?
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("invalid integer for --min-bytes: {value}"))
        })
        .transpose()?
        .unwrap_or(64);
    let message = take_option(&mut remaining, "--message")?;
    let force_positive = take_flag(&mut remaining, "--force-positive");
    let force_negative = take_flag(&mut remaining, "--force-negative");

    if force_positive && force_negative {
        bail!("--force-positive and --force-negative cannot be used together")
    }
    if !remaining.is_empty() {
        bail!("unexpected arguments: {}", remaining.join(" "))
    }

    Ok(Config {
        image_path,
        label,
        min_bytes,
        message,
        force_positive,
        force_negative,
    })
}

fn evaluate(config: &Config) -> Result<RunnerOutput> {
    if config.force_negative {
        return Ok(build_output(config, false));
    }
    if config.force_positive {
        return Ok(build_output(config, true));
    }

    let metadata = fs::metadata(&config.image_path)
        .with_context(|| format!("failed to inspect image: {}", config.image_path))?;
    if !metadata.is_file() {
        bail!("image path is not a file: {}", config.image_path)
    }

    let data = fs::read(&config.image_path)
        .with_context(|| format!("failed to read image: {}", config.image_path))?;
    let looks_like_jpeg = data.starts_with(&[0xFF, 0xD8, 0xFF]);
    let detected = looks_like_jpeg && metadata.len() >= config.min_bytes;

    Ok(build_output(config, detected))
}

fn build_output(config: &Config, detected: bool) -> RunnerOutput {
    RunnerOutput {
        detected,
        message: detected.then(|| {
            config.message.clone().unwrap_or_else(|| {
                format!(
                    "{} candidate produced by example runner",
                    config.label.as_ref().map(event_kind_name).unwrap_or("event")
                )
            })
        }),
        event_kind: detected.then(|| config.label.clone()).flatten(),
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
    "HarborLookout example model runner\n\nUsage:\n  harborlookout-model-runner-example --image sample.jpg [--label PackageDetected] [--min-bytes 64] [--message text] [--force-positive|--force-negative]\n\nEnvironment:\n  HARBORLOOKOUT_FRAME_PATH\n  HARBORLOOKOUT_EVENT_KIND"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_missing_image_path() {
        let error = parse_args(Vec::new()).expect_err("missing image path should fail");
        assert!(error.to_string().contains("missing image path"));
    }

    #[test]
    fn rejects_conflicting_force_flags() {
        let error = parse_args(vec![
            "--image".into(),
            "sample.jpg".into(),
            "--force-positive".into(),
            "--force-negative".into(),
        ])
        .expect_err("conflicting force flags should fail");

        assert!(error.to_string().contains("cannot be used together"));
    }

    #[test]
    fn detects_valid_jpeg_like_bytes() {
        let temp_root = std::env::temp_dir().join(format!(
            "harborlookout-model-runner-example-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(&temp_root).unwrap();
        let image_path = temp_root.join("frame.jpg");
        fs::write(&image_path, [0xFF, 0xD8, 0xFF, 0x10, 0x20, 0x30, 0x40, 0x50]).unwrap();

        let output = evaluate(&Config {
            image_path: image_path.to_string_lossy().to_string(),
            label: Some(EventKind::PackageDetected),
            min_bytes: 4,
            message: None,
            force_positive: false,
            force_negative: false,
        })
        .unwrap();

        assert!(output.detected);
        assert_eq!(output.event_kind, Some(EventKind::PackageDetected));
        let _ = fs::remove_dir_all(&temp_root);
    }

    #[test]
    fn force_positive_short_circuits_file_check() {
        let output = evaluate(&Config {
            image_path: "missing.jpg".into(),
            label: Some(EventKind::PackageDetected),
            min_bytes: 1024,
            message: Some("forced detection".into()),
            force_positive: true,
            force_negative: false,
        })
        .unwrap();

        assert!(output.detected);
        assert_eq!(output.message.as_deref(), Some("forced detection"));
    }

    #[test]
    fn force_negative_short_circuits_file_check() {
        let output = evaluate(&Config {
            image_path: "missing.jpg".into(),
            label: Some(EventKind::PackageDetected),
            min_bytes: 1,
            message: None,
            force_positive: false,
            force_negative: true,
        })
        .unwrap();

        assert!(!output.detected);
        assert!(output.message.is_none());
    }
}