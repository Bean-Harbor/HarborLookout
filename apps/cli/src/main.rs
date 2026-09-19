use std::env;

use anyhow::{Context, Result, anyhow, bail};
use harborlookout_contracts::{
    AnalysisStatusRequest, ArtifactKindRetentionRule, ArtifactRetentionCleanupRequest,
    ArtifactRetentionPreviewRequest, CreateSurveillanceEventRequest, ListEventArtifactsRequest,
    ListRecordingSegmentsRequest, ListSurveillanceEventsRequest, LocalDetectorConfig,
    ProbeCameraRequest, RecordingStatusRequest, RegisterCameraRecordingRequest,
    RegisterCameraRequest, RetentionCleanupRequest, RetentionPreviewRequest, StartAnalysisRequest,
    StartRecordingRequest, StopAnalysisRequest, StopRecordingRequest, SyncAnalysisEventsRequest,
    SyncRecordingSegmentsRequest,
};
use harborlookout_domain::{ArtifactKind, Camera, CameraId, EventId, EventKind, EventSeverity};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:4040";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Cli {
    server_url: String,
    output_format: OutputFormat,
    command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    Health,
    Cameras(CamerasCommand),
    Recordings(RecordingsCommand),
    Analysis(AnalysisCommand),
    Events(EventsCommand),
    Storage(StorageCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CamerasCommand {
    List,
    Register {
        file: String,
        start_recording_output_directory: Option<String>,
    },
    Probe { file: String, output_directory: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecordingsCommand {
    Start {
        file: String,
        output_directory: String,
    },
    Stop {
        camera_id: String,
    },
    Restart {
        file: String,
        output_directory: String,
    },
    Status {
        camera_id: String,
    },
    ListSegments {
        camera_id: String,
        started_after_unix_ms: Option<u64>,
        started_before_unix_ms: Option<u64>,
    },
    Sync {
        camera_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AnalysisCommand {
    Start {
        file: String,
        interval_ms: u64,
        event_kind: EventKind,
        event_severity: EventSeverity,
        message: Option<String>,
        artifacts_directory: Option<String>,
        detector_program: String,
        detector_args: Vec<String>,
    },
    Status {
        camera_id: String,
    },
    Sync {
        camera_id: String,
        max_events: Option<usize>,
    },
    Stop {
        camera_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EventsCommand {
    Create { file: String },
    List {
        camera_id: Option<String>,
        occurred_after_unix_ms: Option<u64>,
        occurred_before_unix_ms: Option<u64>,
    },
    ListArtifacts { event_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StorageCommand {
    Summary,
    RetentionPreview { retain_after_unix_ms: u64 },
    RetentionCleanup { retain_after_unix_ms: u64 },
    ArtifactRetentionPreview {
        retain_after_unix_ms: u64,
        retain_after_by_kind: Vec<ArtifactKindRetentionRule>,
        protect_events_occurred_after_unix_ms: Option<u64>,
    },
    ArtifactRetentionCleanup {
        retain_after_unix_ms: u64,
        retain_after_by_kind: Vec<ArtifactKindRetentionRule>,
        protect_events_occurred_after_unix_ms: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = match parse_cli(env::args().skip(1).collect()) {
        Ok(cli) => cli,
        Err(error) => {
            eprintln!("error: {error}\n");
            eprintln!("{}", usage());
            std::process::exit(2);
        }
    };

    execute(cli).await
}

async fn execute(cli: Cli) -> Result<()> {
    let Cli {
        server_url,
        output_format,
        command,
    } = cli;

    match &command {
        Command::Health => print_get_json(&server_url, "/health", &command, output_format).await,
        Command::Cameras(CamerasCommand::List) => {
            print_get_json(&server_url, "/api/cameras", &command, output_format).await
        }
        Command::Cameras(CamerasCommand::Register {
            file,
            start_recording_output_directory,
        }) => {
            let camera = load_camera(file)?;
            print_post_json(
                &server_url,
                "/api/cameras",
                &command,
                output_format,
                &RegisterCameraRequest {
                    camera,
                    start_recording: start_recording_output_directory.clone().map(
                        |output_directory| RegisterCameraRecordingRequest {
                            output_directory,
                            external_recording_lease: None,
                        },
                    ),
                },
            )
            .await
        }
        Command::Cameras(CamerasCommand::Probe {
            file,
            output_directory,
        }) => {
            let camera = load_camera(file)?;
            print_post_json(
                &server_url,
                "/api/cameras/probe",
                &command,
                output_format,
                &ProbeCameraRequest {
                    camera,
                    output_directory: output_directory.clone(),
                },
            )
            .await
        }
        Command::Recordings(RecordingsCommand::Start {
            file,
            output_directory,
        }) => {
            let camera = load_camera(file)?;
            print_post_json(
                &server_url,
                "/api/recordings/start",
                &command,
                output_format,
                &StartRecordingRequest {
                    camera,
                    output_directory: output_directory.clone(),
                    external_recording_lease: None,
                },
            )
            .await
        }
        Command::Recordings(RecordingsCommand::Stop { camera_id }) => {
            print_post_json(
                &server_url,
                "/api/recordings/stop",
                &command,
                output_format,
                &StopRecordingRequest {
                    camera_id: CameraId(camera_id.clone()),
                },
            )
            .await
        }
        Command::Recordings(RecordingsCommand::Restart {
            file,
            output_directory,
        }) => {
            let camera = load_camera(file)?;
            print_post_json(
                &server_url,
                "/api/recordings/restart",
                &command,
                output_format,
                &harborlookout_contracts::RestartRecordingRequest {
                    camera,
                    output_directory: output_directory.clone(),
                    external_recording_lease: None,
                },
            )
            .await
        }
        Command::Recordings(RecordingsCommand::Status { camera_id }) => {
            print_post_json(
                &server_url,
                "/api/recordings/status",
                &command,
                output_format,
                &RecordingStatusRequest {
                    camera_id: CameraId(camera_id.clone()),
                },
            )
            .await
        }
        Command::Recordings(RecordingsCommand::ListSegments {
            camera_id,
            started_after_unix_ms,
            started_before_unix_ms,
        }) => {
            print_post_json(
                &server_url,
                "/api/recordings/segments/query",
                &command,
                output_format,
                &ListRecordingSegmentsRequest {
                    camera_id: CameraId(camera_id.clone()),
                    started_after_unix_ms: *started_after_unix_ms,
                    started_before_unix_ms: *started_before_unix_ms,
                },
            )
            .await
        }
        Command::Recordings(RecordingsCommand::Sync { camera_id }) => {
            print_post_json(
                &server_url,
                "/api/recordings/segments/sync",
                &command,
                output_format,
                &SyncRecordingSegmentsRequest {
                    camera_id: CameraId(camera_id.clone()),
                },
            )
            .await
        }
        Command::Analysis(AnalysisCommand::Start {
            file,
            interval_ms,
            event_kind,
            event_severity,
            message,
            artifacts_directory,
            detector_program,
            detector_args,
        }) => {
            let camera = load_camera(file)?;
            print_post_json(
                &server_url,
                "/api/analysis/start",
                &command,
                output_format,
                &StartAnalysisRequest {
                    camera,
                    interval_ms: *interval_ms,
                    event_kind: event_kind.clone(),
                    event_severity: event_severity.clone(),
                    message: message.clone(),
                    artifacts_directory: artifacts_directory.clone(),
                    detector: LocalDetectorConfig {
                        program: detector_program.clone(),
                        args: detector_args.clone(),
                    },
                },
            )
            .await
        }
        Command::Analysis(AnalysisCommand::Status { camera_id }) => {
            print_post_json(
                &server_url,
                "/api/analysis/status",
                &command,
                output_format,
                &AnalysisStatusRequest {
                    camera_id: CameraId(camera_id.clone()),
                },
            )
            .await
        }
        Command::Analysis(AnalysisCommand::Sync {
            camera_id,
            max_events,
        }) => {
            print_post_json(
                &server_url,
                "/api/analysis/events/sync",
                &command,
                output_format,
                &SyncAnalysisEventsRequest {
                    camera_id: CameraId(camera_id.clone()),
                    max_events: *max_events,
                },
            )
            .await
        }
        Command::Analysis(AnalysisCommand::Stop { camera_id }) => {
            print_post_json(
                &server_url,
                "/api/analysis/stop",
                &command,
                output_format,
                &StopAnalysisRequest {
                    camera_id: CameraId(camera_id.clone()),
                },
            )
            .await
        }
        Command::Events(EventsCommand::Create { file }) => {
            let request: CreateSurveillanceEventRequest = load_json(file)?;
            print_post_json(
                &server_url,
                "/api/events",
                &command,
                output_format,
                &request,
            )
            .await
        }
        Command::Events(EventsCommand::List {
            camera_id,
            occurred_after_unix_ms,
            occurred_before_unix_ms,
        }) => {
            print_post_json(
                &server_url,
                "/api/events/query",
                &command,
                output_format,
                &ListSurveillanceEventsRequest {
                    camera_id: camera_id.clone().map(CameraId),
                    occurred_after_unix_ms: *occurred_after_unix_ms,
                    occurred_before_unix_ms: *occurred_before_unix_ms,
                },
            )
            .await
        }
        Command::Events(EventsCommand::ListArtifacts { event_id }) => {
            print_post_json(
                &server_url,
                "/api/events/artifacts/query",
                &command,
                output_format,
                &ListEventArtifactsRequest {
                    event_id: EventId(event_id.clone()),
                },
            )
            .await
        }
        Command::Storage(StorageCommand::Summary) => {
            print_get_json(&server_url, "/api/storage/summary", &command, output_format).await
        }
        Command::Storage(StorageCommand::RetentionPreview { retain_after_unix_ms }) => {
            print_post_json(
                &server_url,
                "/api/storage/retention-preview",
                &command,
                output_format,
                &RetentionPreviewRequest {
                    retain_after_unix_ms: *retain_after_unix_ms,
                },
            )
            .await
        }
        Command::Storage(StorageCommand::RetentionCleanup { retain_after_unix_ms }) => {
            print_post_json(
                &server_url,
                "/api/storage/retention-cleanup",
                &command,
                output_format,
                &RetentionCleanupRequest {
                    retain_after_unix_ms: *retain_after_unix_ms,
                },
            )
            .await
        }
        Command::Storage(StorageCommand::ArtifactRetentionPreview {
            retain_after_unix_ms,
            retain_after_by_kind,
            protect_events_occurred_after_unix_ms,
        }) => {
            print_post_json(
                &server_url,
                "/api/storage/artifacts/retention-preview",
                &command,
                output_format,
                &ArtifactRetentionPreviewRequest {
                    retain_after_unix_ms: *retain_after_unix_ms,
                    retain_after_by_kind: retain_after_by_kind.clone(),
                    protect_events_occurred_after_unix_ms: *protect_events_occurred_after_unix_ms,
                },
            )
            .await
        }
        Command::Storage(StorageCommand::ArtifactRetentionCleanup {
            retain_after_unix_ms,
            retain_after_by_kind,
            protect_events_occurred_after_unix_ms,
        }) => {
            print_post_json(
                &server_url,
                "/api/storage/artifacts/retention-cleanup",
                &command,
                output_format,
                &ArtifactRetentionCleanupRequest {
                    retain_after_unix_ms: *retain_after_unix_ms,
                    retain_after_by_kind: retain_after_by_kind.clone(),
                    protect_events_occurred_after_unix_ms: *protect_events_occurred_after_unix_ms,
                },
            )
            .await
        }
    }
}

fn parse_cli(mut args: Vec<String>) -> Result<Cli> {
    if matches!(args.first().map(String::as_str), None | Some("help") | Some("--help") | Some("-h")) {
        bail!("help requested")
    }

    let json_output = take_flag(&mut args, "--json");
    let text_output = take_flag(&mut args, "--text");
    if json_output && text_output {
        bail!("--json and --text cannot be used together")
    }

    let server_url = take_option(&mut args, "--server")?.unwrap_or_else(default_server_url);
    let output_format = if json_output {
        OutputFormat::Json
    } else {
        let _ = text_output;
        OutputFormat::Text
    };
    if args.is_empty() {
        bail!("missing command")
    }

    let command_group = args.remove(0);
    let command = match command_group.as_str() {
        "health" => {
            ensure_no_extra_args(&args)?;
            Command::Health
        }
        "camera" | "cameras" => Command::Cameras(parse_cameras_command(args)?),
        "recording" | "recordings" => Command::Recordings(parse_recordings_command(args)?),
        "analysis" => Command::Analysis(parse_analysis_command(args)?),
        "event" | "events" => Command::Events(parse_events_command(args)?),
        "storage" => Command::Storage(parse_storage_command(args)?),
        other => bail!("unknown command group: {other}"),
    };

    Ok(Cli {
        server_url,
        output_format,
        command,
    })
}

fn parse_cameras_command(mut args: Vec<String>) -> Result<CamerasCommand> {
    let Some(subcommand) = args.first().cloned() else {
        bail!("missing cameras subcommand")
    };
    args.remove(0);

    match subcommand.as_str() {
        "list" => {
            ensure_no_extra_args(&args)?;
            Ok(CamerasCommand::List)
        }
        "register" => {
            let file = required_option(&mut args, "--file")?;
            let start_recording = take_flag(&mut args, "--start-recording");
            let output_directory = take_option(&mut args, "--output-dir")?;
            if start_recording && output_directory.is_none() {
                bail!("--start-recording requires --output-dir")
            }
            if !start_recording && output_directory.is_some() {
                bail!("--output-dir is only valid with --start-recording")
            }
            ensure_no_extra_args(&args)?;
            Ok(CamerasCommand::Register {
                file,
                start_recording_output_directory: output_directory,
            })
        }
        "probe" => {
            let file = required_option(&mut args, "--file")?;
            let output_directory = required_option(&mut args, "--output-dir")?;
            ensure_no_extra_args(&args)?;
            Ok(CamerasCommand::Probe {
                file,
                output_directory,
            })
        }
        other => bail!("unknown cameras subcommand: {other}"),
    }
}

fn parse_recordings_command(mut args: Vec<String>) -> Result<RecordingsCommand> {
    let Some(subcommand) = args.first().cloned() else {
        bail!("missing recordings subcommand")
    };
    args.remove(0);

    match subcommand.as_str() {
        "start" => {
            let file = required_option(&mut args, "--file")?;
            let output_directory = required_option(&mut args, "--output-dir")?;
            ensure_no_extra_args(&args)?;
            Ok(RecordingsCommand::Start {
                file,
                output_directory,
            })
        }
        "stop" => {
            let camera_id = required_option(&mut args, "--camera-id")?;
            ensure_no_extra_args(&args)?;
            Ok(RecordingsCommand::Stop { camera_id })
        }
        "restart" => {
            let file = required_option(&mut args, "--file")?;
            let output_directory = required_option(&mut args, "--output-dir")?;
            ensure_no_extra_args(&args)?;
            Ok(RecordingsCommand::Restart {
                file,
                output_directory,
            })
        }
        "status" => {
            let camera_id = required_option(&mut args, "--camera-id")?;
            ensure_no_extra_args(&args)?;
            Ok(RecordingsCommand::Status { camera_id })
        }
        "list-segments" => {
            let camera_id = required_option(&mut args, "--camera-id")?;
            let started_after_unix_ms = optional_u64_option(&mut args, "--started-after-ms")?;
            let started_before_unix_ms = optional_u64_option(&mut args, "--started-before-ms")?;
            ensure_no_extra_args(&args)?;
            Ok(RecordingsCommand::ListSegments {
                camera_id,
                started_after_unix_ms,
                started_before_unix_ms,
            })
        }
        "sync" => {
            let camera_id = required_option(&mut args, "--camera-id")?;
            ensure_no_extra_args(&args)?;
            Ok(RecordingsCommand::Sync { camera_id })
        }
        other => bail!("unknown recordings subcommand: {other}"),
    }
}

fn parse_storage_command(mut args: Vec<String>) -> Result<StorageCommand> {
    let Some(subcommand) = args.first().cloned() else {
        bail!("missing storage subcommand")
    };
    args.remove(0);

    match subcommand.as_str() {
        "summary" => {
            ensure_no_extra_args(&args)?;
            Ok(StorageCommand::Summary)
        }
        "retention-preview" => {
            let retain_after_unix_ms = required_u64_option(&mut args, "--retain-after-ms")?;
            ensure_no_extra_args(&args)?;
            Ok(StorageCommand::RetentionPreview {
                retain_after_unix_ms,
            })
        }
        "retention-cleanup" => {
            let retain_after_unix_ms = required_u64_option(&mut args, "--retain-after-ms")?;
            ensure_no_extra_args(&args)?;
            Ok(StorageCommand::RetentionCleanup {
                retain_after_unix_ms,
            })
        }
        "artifacts-retention-preview" => {
            let retain_after_unix_ms = required_u64_option(&mut args, "--retain-after-ms")?;
            let retain_after_by_kind = parse_kind_retention_rules(&take_options(&mut args, "--kind-retain")?)?;
            let protect_events_occurred_after_unix_ms = optional_u64_option(&mut args, "--protect-events-after-ms")?;
            ensure_no_extra_args(&args)?;
            Ok(StorageCommand::ArtifactRetentionPreview {
                retain_after_unix_ms,
                retain_after_by_kind,
                protect_events_occurred_after_unix_ms,
            })
        }
        "artifacts-retention-cleanup" => {
            let retain_after_unix_ms = required_u64_option(&mut args, "--retain-after-ms")?;
            let retain_after_by_kind = parse_kind_retention_rules(&take_options(&mut args, "--kind-retain")?)?;
            let protect_events_occurred_after_unix_ms = optional_u64_option(&mut args, "--protect-events-after-ms")?;
            ensure_no_extra_args(&args)?;
            Ok(StorageCommand::ArtifactRetentionCleanup {
                retain_after_unix_ms,
                retain_after_by_kind,
                protect_events_occurred_after_unix_ms,
            })
        }
        other => bail!("unknown storage subcommand: {other}"),
    }
}

fn parse_analysis_command(mut args: Vec<String>) -> Result<AnalysisCommand> {
    let Some(subcommand) = args.first().cloned() else {
        bail!("missing analysis subcommand")
    };
    args.remove(0);

    match subcommand.as_str() {
        "start" => {
            let file = required_option(&mut args, "--file")?;
            let interval_ms = required_u64_option(&mut args, "--interval-ms")?;
            let event_kind = parse_event_kind(&required_option(&mut args, "--kind")?)?;
            let event_severity = parse_event_severity(&required_option(&mut args, "--severity")?)?;
            let message = take_option(&mut args, "--message")?;
            let artifacts_directory = take_option(&mut args, "--artifacts-dir")?;
            let detector_program = required_option(&mut args, "--detector-program")?;
            let detector_args = take_options(&mut args, "--detector-arg")?;
            ensure_no_extra_args(&args)?;
            Ok(AnalysisCommand::Start {
                file,
                interval_ms,
                event_kind,
                event_severity,
                message,
                artifacts_directory,
                detector_program,
                detector_args,
            })
        }
        "status" => {
            let camera_id = required_option(&mut args, "--camera-id")?;
            ensure_no_extra_args(&args)?;
            Ok(AnalysisCommand::Status { camera_id })
        }
        "sync" => {
            let camera_id = required_option(&mut args, "--camera-id")?;
            let max_events = take_option(&mut args, "--max-events")?
                .map(|value| value.parse::<usize>().with_context(|| format!("invalid integer for --max-events: {value}")))
                .transpose()?;
            ensure_no_extra_args(&args)?;
            Ok(AnalysisCommand::Sync {
                camera_id,
                max_events,
            })
        }
        "stop" => {
            let camera_id = required_option(&mut args, "--camera-id")?;
            ensure_no_extra_args(&args)?;
            Ok(AnalysisCommand::Stop { camera_id })
        }
        other => bail!("unknown analysis subcommand: {other}"),
    }
}

fn parse_events_command(mut args: Vec<String>) -> Result<EventsCommand> {
    let Some(subcommand) = args.first().cloned() else {
        bail!("missing events subcommand")
    };
    args.remove(0);

    match subcommand.as_str() {
        "create" => {
            let file = required_option(&mut args, "--file")?;
            ensure_no_extra_args(&args)?;
            Ok(EventsCommand::Create { file })
        }
        "list" => {
            let camera_id = take_option(&mut args, "--camera-id")?;
            let occurred_after_unix_ms = optional_u64_option(&mut args, "--occurred-after-ms")?;
            let occurred_before_unix_ms = optional_u64_option(&mut args, "--occurred-before-ms")?;
            ensure_no_extra_args(&args)?;
            Ok(EventsCommand::List {
                camera_id,
                occurred_after_unix_ms,
                occurred_before_unix_ms,
            })
        }
        "artifacts" => {
            let event_id = required_option(&mut args, "--event-id")?;
            ensure_no_extra_args(&args)?;
            Ok(EventsCommand::ListArtifacts { event_id })
        }
        other => bail!("unknown events subcommand: {other}"),
    }
}

fn required_option(args: &mut Vec<String>, flag: &str) -> Result<String> {
    take_option(args, flag)?.ok_or_else(|| anyhow!("missing required option: {flag}"))
}

fn optional_u64_option(args: &mut Vec<String>, flag: &str) -> Result<Option<u64>> {
    take_option(args, flag)?
        .map(|value| parse_u64(flag, &value))
        .transpose()
}

fn required_u64_option(args: &mut Vec<String>, flag: &str) -> Result<u64> {
    let value = required_option(args, flag)?;
    parse_u64(flag, &value)
}

fn take_option(args: &mut Vec<String>, flag: &str) -> Result<Option<String>> {
    let Some(position) = args.iter().position(|arg| arg == flag) else {
        return Ok(None);
    };

    if position + 1 >= args.len() {
        bail!("missing value for option: {flag}")
    }

    let value = args.remove(position + 1);
    args.remove(position);
    Ok(Some(value))
}

fn take_options(args: &mut Vec<String>, flag: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    while let Some(position) = args.iter().position(|arg| arg == flag) {
        if position + 1 >= args.len() {
            bail!("missing value for option: {flag}")
        }
        values.push(args.remove(position + 1));
        args.remove(position);
    }
    Ok(values)
}

fn take_flag(args: &mut Vec<String>, flag: &str) -> bool {
    if let Some(position) = args.iter().position(|arg| arg == flag) {
        args.remove(position);
        true
    } else {
        false
    }
}

fn parse_u64(flag: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .with_context(|| format!("invalid integer for {flag}: {value}"))
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

fn parse_artifact_kind(value: &str) -> Result<ArtifactKind> {
    match value {
        "Keyframe" => Ok(ArtifactKind::Keyframe),
        "Clip" => Ok(ArtifactKind::Clip),
        "Thumbnail" => Ok(ArtifactKind::Thumbnail),
        "Metadata" => Ok(ArtifactKind::Metadata),
        other => bail!("unknown artifact kind: {other}"),
    }
}

fn parse_kind_retention_rules(values: &[String]) -> Result<Vec<ArtifactKindRetentionRule>> {
    let mut rules = Vec::new();
    for value in values {
        let (kind, retain_after_unix_ms) = value
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid --kind-retain format: {value}; expected <Kind>=<unix-ms>"))?;
        rules.push(ArtifactKindRetentionRule {
            kind: parse_artifact_kind(kind)?,
            retain_after_unix_ms: parse_u64("--kind-retain", retain_after_unix_ms)?,
        });
    }
    Ok(rules)
}

fn ensure_no_extra_args(args: &[String]) -> Result<()> {
    if args.is_empty() {
        return Ok(());
    }

    bail!("unexpected arguments: {}", args.join(" "))
}

fn default_server_url() -> String {
    env::var("HARBORLOOKOUT_SERVER_URL").unwrap_or_else(|_| DEFAULT_SERVER_URL.to_string())
}

fn load_camera(path: &str) -> Result<Camera> {
    load_json(path)
}

fn load_json<T: DeserializeOwned>(path: &str) -> Result<T> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read JSON file: {path}"))?;
    serde_json::from_str(&content).with_context(|| format!("failed to parse JSON: {path}"))
}

async fn print_get_json(base_url: &str, path: &str, command: &Command, output_format: OutputFormat) -> Result<()> {
    let response: serde_json::Value = get_json(base_url, path).await?;
    print_response(command, output_format, &response)
}

async fn print_post_json<TRequest>(
    base_url: &str,
    path: &str,
    command: &Command,
    output_format: OutputFormat,
    request: &TRequest,
) -> Result<()>
where
    TRequest: Serialize + ?Sized,
{
    let response: serde_json::Value = post_json(base_url, path, request).await?;
    print_response(command, output_format, &response)
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn print_response(command: &Command, output_format: OutputFormat, response: &serde_json::Value) -> Result<()> {
    match output_format {
        OutputFormat::Json => print_json(response),
        OutputFormat::Text => {
            println!("{}", render_text_response(command, response));
            Ok(())
        }
    }
}

fn render_text_response(command: &Command, response: &serde_json::Value) -> String {
    match command {
        Command::Health => render_health(response),
        Command::Cameras(CamerasCommand::List) => render_camera_list(response),
        Command::Cameras(CamerasCommand::Register { .. }) => render_camera_register(response),
        Command::Cameras(CamerasCommand::Probe { .. }) => render_camera_probe(response),
        Command::Recordings(RecordingsCommand::Start { .. }) => render_recording_start(response),
        Command::Recordings(RecordingsCommand::Stop { .. }) => render_recording_stop(response),
        Command::Recordings(RecordingsCommand::Restart { .. }) => render_recording_restart(response),
        Command::Recordings(RecordingsCommand::Status { .. }) => render_recording_status(response),
        Command::Recordings(RecordingsCommand::ListSegments { .. }) => render_recording_segments(response),
        Command::Recordings(RecordingsCommand::Sync { .. }) => render_recording_sync(response),
        Command::Analysis(AnalysisCommand::Start { .. }) => render_analysis_start(response),
        Command::Analysis(AnalysisCommand::Status { .. }) => render_analysis_status(response),
        Command::Analysis(AnalysisCommand::Sync { .. }) => render_analysis_sync(response),
        Command::Analysis(AnalysisCommand::Stop { .. }) => render_analysis_stop(response),
        Command::Events(EventsCommand::Create { .. }) => render_event_create(response),
        Command::Events(EventsCommand::List { .. }) => render_events_list(response),
        Command::Events(EventsCommand::ListArtifacts { .. }) => render_event_artifacts(response),
        Command::Storage(StorageCommand::Summary) => render_storage_summary(response),
        Command::Storage(StorageCommand::RetentionPreview { .. }) => render_retention_preview(response),
        Command::Storage(StorageCommand::RetentionCleanup { .. }) => render_retention_cleanup(response),
        Command::Storage(StorageCommand::ArtifactRetentionPreview { .. }) => {
            render_artifact_retention_preview(response)
        }
        Command::Storage(StorageCommand::ArtifactRetentionCleanup { .. }) => {
            render_artifact_retention_cleanup(response)
        }
    }
}

fn render_health(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("status: {}", string_field(response, "status"))];
    if let Some(backend) = optional_string_field(response, "backend") {
        lines.push(format!("backend: {backend}"));
    }
    if let Some(active_sessions) = optional_u64_field(response, "active_sessions") {
        lines.push(format!("active sessions: {active_sessions}"));
    }
    if let Some(active_analyses) = optional_u64_field(response, "active_analyses") {
        lines.push(format!("active analyses: {active_analyses}"));
    }
    if let Some(registered_cameras) = optional_u64_field(response, "registered_cameras") {
        lines.push(format!("registered cameras: {registered_cameras}"));
    }
    lines.join("\n")
}

fn render_camera_list(response: &serde_json::Value) -> String {
    let Some(cameras) = response.get("cameras").and_then(serde_json::Value::as_array) else {
        return fallback_json(response);
    };
    if cameras.is_empty() {
        return "no cameras registered".to_string();
    }

    let mut lines = vec![format!("registered cameras: {}", cameras.len())];
    for camera in cameras {
        lines.push(format!(
            "- {} | {} | {}",
            string_field(camera, "id"),
            string_field(camera, "name"),
            if camera.get("enabled").and_then(serde_json::Value::as_bool).unwrap_or(false) {
                "enabled"
            } else {
                "disabled"
            }
        ));
    }
    lines.join("\n")
}

fn render_camera_register(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("registered camera: {}", string_field(response, "camera_id"))];
    if let Some(backend) = optional_string_field(response, "backend") {
        lines.push(format!("backend: {backend}"));
    }
    if let Some(recording) = response.get("recording") {
        lines.push(format!("recording pid: {}", number_or_string_field(recording, "pid")));
    }
    lines.join("\n")
}

fn render_camera_probe(response: &serde_json::Value) -> String {
    let mut lines = Vec::new();
    if let Some(backend) = optional_string_field(response, "backend") {
        lines.push(format!("backend: {backend}"));
    }
    if let Some(output_hint) = optional_string_field(response, "output_hint") {
        lines.push(format!("output: {output_hint}"));
    }
    if lines.is_empty() {
        fallback_json(response)
    } else {
        lines.join("\n")
    }
}

fn render_recording_start(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("recording started: {}", string_field(response, "camera_id"))];
    lines.push(format!("pid: {}", number_or_string_field(response, "pid")));
    if let Some(output_hint) = optional_string_field(response, "output_hint") {
        lines.push(format!("output: {output_hint}"));
    }
    lines.join("\n")
}

fn render_recording_stop(response: &serde_json::Value) -> String {
    format!(
        "recording {}: {}",
        if response.get("stopped").and_then(serde_json::Value::as_bool).unwrap_or(false) {
            "stopped"
        } else {
            "already stopped"
        },
        string_field(response, "camera_id")
    )
}

fn render_recording_restart(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("recording restarted: {}", string_field(response, "camera_id"))];
    lines.push(format!("pid: {}", number_or_string_field(response, "pid")));
    if let Some(output_hint) = optional_string_field(response, "output_hint") {
        lines.push(format!("output: {output_hint}"));
    }
    lines.join("\n")
}

fn render_recording_status(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("camera: {}", string_field(response, "camera_id"))];
    lines.push(format!("state: {}", string_field(response, "state")));
    if let Some(pid) = optional_u64_field(response, "pid") {
        lines.push(format!("pid: {pid}"));
    }
    if let Some(output_hint) = optional_string_field(response, "output_hint") {
        lines.push(format!("output: {output_hint}"));
    }
    if let Some(exit_code) = optional_i64_field(response, "exit_code") {
        lines.push(format!("exit code: {exit_code}"));
    }
    if let Some(last_error) = optional_string_field(response, "last_error") {
        lines.push(format!("last error: {last_error}"));
    }
    lines.join("\n")
}

fn render_recording_segments(response: &serde_json::Value) -> String {
    let Some(segments) = response.get("segments").and_then(serde_json::Value::as_array) else {
        return fallback_json(response);
    };
    if segments.is_empty() {
        return "segments: 0".to_string();
    }

    let mut lines = vec![format!("segments: {}", segments.len())];
    for segment in segments {
        lines.push(format!(
            "- seq {} | {} | {} bytes | {}",
            number_or_string_field(segment, "sequence"),
            string_field(segment, "state"),
            number_or_string_field(segment, "size_bytes"),
            string_field(segment, "path")
        ));
    }
    lines.join("\n")
}

fn render_recording_sync(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("recording sync: {}", string_field(response, "camera_id"))];
    if let Some(synced_segments) = optional_u64_field(response, "synced_segments") {
        lines.push(format!("synced segments: {synced_segments}"));
    }
    if let Some(last_sequence) = optional_u64_field(response, "last_sequence") {
        lines.push(format!("last sequence: {last_sequence}"));
    }
    lines.join("\n")
}

fn render_analysis_start(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("analysis started: {}", string_field(response, "camera_id"))];
    lines.push(format!("state: {}", string_field(response, "state")));
    lines.push(format!("interval ms: {}", number_or_string_field(response, "interval_ms")));
    lines.push(format!("detector: {}", string_field(response, "detector_program")));
    lines.join("\n")
}

fn render_analysis_status(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("camera: {}", string_field(response, "camera_id"))];
    lines.push(format!("state: {}", string_field(response, "state")));
    if let Some(kind) = optional_string_field(response, "event_kind") {
        lines.push(format!("event kind: {kind}"));
    }
    if let Some(severity) = optional_string_field(response, "event_severity") {
        lines.push(format!("event severity: {severity}"));
    }
    if let Some(interval_ms) = optional_u64_field(response, "interval_ms") {
        lines.push(format!("interval ms: {interval_ms}"));
    }
    if let Some(detector_program) = optional_string_field(response, "detector_program") {
        lines.push(format!("detector: {detector_program}"));
    }
    if let Some(emitted_events) = optional_u64_field(response, "emitted_events") {
        lines.push(format!("emitted events: {emitted_events}"));
    }
    if let Some(pending_events) = optional_u64_field(response, "pending_events") {
        lines.push(format!("pending events: {pending_events}"));
    }
    if let Some(last_error) = optional_string_field(response, "last_error") {
        lines.push(format!("last error: {last_error}"));
    }
    lines.join("\n")
}

fn render_analysis_sync(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("analysis sync: {}", string_field(response, "camera_id"))];
    lines.push(format!("stored events: {}", number_or_string_field(response, "stored_events")));
    lines.push(format!(
        "stored artifacts: {}",
        number_or_string_field(response, "stored_artifacts")
    ));
    lines.join("\n")
}

fn render_analysis_stop(response: &serde_json::Value) -> String {
    format!(
        "analysis {}: {}",
        if response.get("stopped").and_then(serde_json::Value::as_bool).unwrap_or(false) {
            "stopped"
        } else {
            "already stopped"
        },
        string_field(response, "camera_id")
    )
}

fn render_event_create(response: &serde_json::Value) -> String {
    let mut lines = vec![format!("stored event: {}", string_field(response, "event_id"))];
    lines.push(format!(
        "stored artifacts: {}",
        number_or_string_field(response, "stored_artifacts")
    ));
    lines.join("\n")
}

fn render_events_list(response: &serde_json::Value) -> String {
    let Some(events) = response.get("events").and_then(serde_json::Value::as_array) else {
        return fallback_json(response);
    };
    if events.is_empty() {
        return "events: 0".to_string();
    }

    let mut lines = vec![format!("events: {}", events.len())];
    for event in events {
        lines.push(format!(
            "- {} | {} | {} | {}",
            string_field(event, "id"),
            string_field(event, "kind"),
            string_field(event, "severity"),
            string_field(event, "message")
        ));
    }
    lines.join("\n")
}

fn render_event_artifacts(response: &serde_json::Value) -> String {
    let Some(artifacts) = response.get("artifacts").and_then(serde_json::Value::as_array) else {
        return fallback_json(response);
    };
    if artifacts.is_empty() {
        return "artifacts: 0".to_string();
    }

    let mut lines = vec![format!("artifacts: {}", artifacts.len())];
    for artifact in artifacts {
        lines.push(format!(
            "- {} | {} | {} bytes | {}",
            string_field(artifact, "id"),
            string_field(artifact, "kind"),
            number_or_string_field(artifact, "size_bytes"),
            string_field(artifact, "path")
        ));
    }
    lines.join("\n")
}

fn render_storage_summary(response: &serde_json::Value) -> String {
    let mut lines = Vec::new();
    for field in ["registered_cameras", "active_recordings", "total_segments", "total_bytes"] {
        if response.get(field).is_some() {
            lines.push(format!("{}: {}", field.replace('_', " "), number_or_string_field(response, field)));
        }
    }
    if lines.is_empty() {
        fallback_json(response)
    } else {
        lines.join("\n")
    }
}

fn render_retention_preview(response: &serde_json::Value) -> String {
    let mut lines = vec![format!(
        "reclaimable segments: {}",
        number_or_string_field(response, "reclaimable_segments")
    )];
    lines.push(format!(
        "reclaimable bytes: {}",
        number_or_string_field(response, "reclaimable_bytes")
    ));
    if let Some(cameras) = response.get("cameras").and_then(serde_json::Value::as_array) {
        for camera in cameras {
            lines.push(format!(
                "- {} | {} segments | {} bytes",
                string_field(camera, "camera_id"),
                number_or_string_field(camera, "reclaimable_segments"),
                number_or_string_field(camera, "reclaimable_bytes")
            ));
        }
    }
    lines.join("\n")
}

fn render_retention_cleanup(response: &serde_json::Value) -> String {
    let mut lines = vec![format!(
        "deleted segments: {}",
        number_or_string_field(response, "deleted_segments")
    )];
    lines.push(format!(
        "deleted bytes: {}",
        number_or_string_field(response, "deleted_bytes")
    ));
    if let Some(skipped_segments) = optional_u64_field(response, "skipped_segments") {
        lines.push(format!("skipped segments: {skipped_segments}"));
    }
    if let Some(cameras) = response.get("cameras").and_then(serde_json::Value::as_array) {
        for camera in cameras {
            lines.push(format!(
                "- {} | {} deleted | {} bytes",
                string_field(camera, "camera_id"),
                number_or_string_field(camera, "deleted_segments"),
                number_or_string_field(camera, "deleted_bytes")
            ));
        }
    }
    lines.join("\n")
}

fn render_artifact_retention_preview(response: &serde_json::Value) -> String {
    let mut lines = vec![format!(
        "reclaimable artifacts: {}",
        number_or_string_field(response, "reclaimable_artifacts")
    )];
    lines.push(format!(
        "reclaimable bytes: {}",
        number_or_string_field(response, "reclaimable_bytes")
    ));
    if let Some(protected_artifacts) = optional_u64_field(response, "protected_artifacts") {
        lines.push(format!("protected artifacts: {protected_artifacts}"));
    }
    if let Some(cameras) = response.get("cameras").and_then(serde_json::Value::as_array) {
        for camera in cameras {
            lines.push(format!(
                "- {} | {} artifacts | {} bytes",
                string_field(camera, "camera_id"),
                number_or_string_field(camera, "reclaimable_artifacts"),
                number_or_string_field(camera, "reclaimable_bytes")
            ));
        }
    }
    lines.join("\n")
}

fn render_artifact_retention_cleanup(response: &serde_json::Value) -> String {
    let mut lines = vec![format!(
        "deleted artifacts: {}",
        number_or_string_field(response, "deleted_artifacts")
    )];
    lines.push(format!(
        "deleted bytes: {}",
        number_or_string_field(response, "deleted_bytes")
    ));
    if let Some(protected_artifacts) = optional_u64_field(response, "protected_artifacts") {
        lines.push(format!("protected artifacts: {protected_artifacts}"));
    }
    if let Some(skipped_artifacts) = optional_u64_field(response, "skipped_artifacts") {
        lines.push(format!("skipped artifacts: {skipped_artifacts}"));
    }
    if let Some(cameras) = response.get("cameras").and_then(serde_json::Value::as_array) {
        for camera in cameras {
            lines.push(format!(
                "- {} | {} deleted | {} bytes",
                string_field(camera, "camera_id"),
                number_or_string_field(camera, "deleted_artifacts"),
                number_or_string_field(camera, "deleted_bytes")
            ));
        }
    }
    lines.join("\n")
}

fn fallback_json(response: &serde_json::Value) -> String {
    serde_json::to_string_pretty(response).unwrap_or_else(|_| "{}".to_string())
}

fn string_field(value: &serde_json::Value, field: &str) -> String {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| number_or_string_field(value, field))
}

fn number_or_string_field(value: &serde_json::Value, field: &str) -> String {
    match value.get(field) {
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(serde_json::Value::Number(number)) => number.to_string(),
        Some(serde_json::Value::Bool(flag)) => flag.to_string(),
        Some(serde_json::Value::Null) | None => "-".to_string(),
        Some(other) => other.to_string(),
    }
}

fn optional_string_field(value: &serde_json::Value, field: &str) -> Option<String> {
    value.get(field).and_then(serde_json::Value::as_str).map(ToString::to_string)
}

fn optional_u64_field(value: &serde_json::Value, field: &str) -> Option<u64> {
    value.get(field).and_then(serde_json::Value::as_u64)
}

fn optional_i64_field(value: &serde_json::Value, field: &str) -> Option<i64> {
    value.get(field).and_then(serde_json::Value::as_i64)
}

async fn get_json<T>(base_url: &str, path: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let (host, port) = parse_base_url(base_url)?;
    let mut stream = TcpStream::connect((host.as_str(), port)).await?;
    let request_text = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request_text.as_bytes()).await?;

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).await?;
    parse_json_response(&response_bytes)
}

async fn post_json<TRequest, TResponse>(base_url: &str, path: &str, request: &TRequest) -> Result<TResponse>
where
    TRequest: Serialize + ?Sized,
    TResponse: DeserializeOwned,
{
    let (host, port) = parse_base_url(base_url)?;
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

fn parse_base_url(base_url: &str) -> Result<(String, u16)> {
    let trimmed = base_url.trim();
    let authority = trimmed
        .strip_prefix("http://")
        .ok_or_else(|| anyhow!("unsupported server url: {trimmed}"))?
        .trim_end_matches('/');
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("server url must include host and port: {trimmed}"))?;
    Ok((host.to_string(), port.parse()?))
}

fn parse_json_response<T: DeserializeOwned>(response_bytes: &[u8]) -> Result<T> {
    let response = String::from_utf8(response_bytes.to_vec())?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .or_else(|| response.split_once("\n\n"))
        .ok_or_else(|| anyhow!("invalid HTTP response"))?;
    let status_line = head.lines().next().ok_or_else(|| anyhow!("missing HTTP status line"))?;
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("missing HTTP status code"))?
        .parse()?;

    if status_code >= 400 {
        let message = body.trim();
        if message.is_empty() {
            bail!("request failed: {status_code}")
        }
        bail!("request failed: {status_code} {message}")
    }

    Ok(serde_json::from_str(body)?)
}

fn usage() -> &'static str {
    "HarborLookout CLI

Usage:
    harborlookout-cli [--server http://127.0.0.1:4040] [--json|--text] health
    harborlookout-cli [--server URL] [--json|--text] cameras list
        harborlookout-cli [--server URL] [--json|--text] cameras register --file camera.json [--start-recording --output-dir data/segments/cam01]
    harborlookout-cli [--server URL] [--json|--text] cameras probe --file camera.json --output-dir data/segments/cam01
    harborlookout-cli [--server URL] [--json|--text] recordings start --file camera.json --output-dir data/segments/cam01
    harborlookout-cli [--server URL] [--json|--text] recordings restart --file camera.json --output-dir data/segments/cam01
    harborlookout-cli [--server URL] [--json|--text] recordings stop --camera-id cam01
    harborlookout-cli [--server URL] [--json|--text] recordings status --camera-id cam01
    harborlookout-cli [--server URL] [--json|--text] recordings list-segments --camera-id cam01 [--started-after-ms 0] [--started-before-ms 0]
    harborlookout-cli [--server URL] [--json|--text] recordings sync --camera-id cam01
        harborlookout-cli [--server URL] [--json|--text] analysis start --file camera.json --interval-ms 1000 --kind PackageDetected --severity Warning --detector-program detector.exe [--detector-arg {frame_path}] [--message text] [--artifacts-dir data/artifacts]
        harborlookout-cli [--server URL] [--json|--text] analysis status --camera-id cam01
        harborlookout-cli [--server URL] [--json|--text] analysis sync --camera-id cam01 [--max-events 10]
        harborlookout-cli [--server URL] [--json|--text] analysis stop --camera-id cam01
        harborlookout-cli [--server URL] [--json|--text] events create --file event.json
        harborlookout-cli [--server URL] [--json|--text] events list [--camera-id cam01] [--occurred-after-ms 0] [--occurred-before-ms 0]
        harborlookout-cli [--server URL] [--json|--text] events artifacts --event-id event-1
    harborlookout-cli [--server URL] [--json|--text] storage summary
    harborlookout-cli [--server URL] [--json|--text] storage retention-preview --retain-after-ms 0
        harborlookout-cli [--server URL] [--json|--text] storage retention-cleanup --retain-after-ms 0
    harborlookout-cli [--server URL] [--json|--text] storage artifacts-retention-preview --retain-after-ms 0 [--kind-retain Keyframe=0] [--protect-events-after-ms 0]
        harborlookout-cli [--server URL] [--json|--text] storage artifacts-retention-cleanup --retain-after-ms 0 [--kind-retain Clip=0] [--protect-events-after-ms 0]

Environment:
    HARBORLOOKOUT_SERVER_URL  Default server URL when --server is omitted

Output:
    Default output is concise text for operators.
    Use --json for script-friendly JSON output."
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_health_command_with_explicit_server() {
        let cli = parse_cli(vec![
            "--server".into(),
            "http://127.0.0.1:4400".into(),
            "health".into(),
        ])
        .expect("health command should parse");

        assert_eq!(
            cli,
            Cli {
                server_url: "http://127.0.0.1:4400".into(),
                output_format: OutputFormat::Text,
                command: Command::Health,
            }
        );
    }

    #[test]
    fn parses_json_output_flag() {
        let cli = parse_cli(vec!["--json".into(), "health".into()])
            .expect("json output flag should parse");

        assert_eq!(cli.output_format, OutputFormat::Json);
    }

    #[test]
    fn rejects_conflicting_output_flags() {
        let error = parse_cli(vec!["--json".into(), "--text".into(), "health".into()])
            .expect_err("conflicting output flags should fail");

        assert!(error.to_string().contains("cannot be used together"));
    }
    #[test]
    fn parses_storage_retention_cleanup() {
        let cli = parse_cli(vec![
            "storage".into(),
            "retention-cleanup".into(),
            "--retain-after-ms".into(),
            "12345".into(),
        ])
        .expect("retention cleanup should parse");

        assert_eq!(
            cli.command,
            Command::Storage(StorageCommand::RetentionCleanup {
                retain_after_unix_ms: 12345,
            })
        );
    }

    #[test]
    fn parses_artifact_retention_preview() {
        let cli = parse_cli(vec![
            "storage".into(),
            "artifacts-retention-preview".into(),
            "--retain-after-ms".into(),
            "999".into(),
        ])
        .expect("artifact retention preview should parse");

        assert_eq!(
            cli.command,
            Command::Storage(StorageCommand::ArtifactRetentionPreview {
                retain_after_unix_ms: 999,
                retain_after_by_kind: Vec::new(),
                protect_events_occurred_after_unix_ms: None,
            })
        );
    }

    #[test]
    fn parses_artifact_retention_cleanup_with_tiered_rules() {
        let cli = parse_cli(vec![
            "storage".into(),
            "artifacts-retention-cleanup".into(),
            "--retain-after-ms".into(),
            "5000".into(),
            "--kind-retain".into(),
            "Keyframe=1000".into(),
            "--kind-retain".into(),
            "Clip=2000".into(),
            "--protect-events-after-ms".into(),
            "4500".into(),
        ])
        .expect("artifact retention cleanup v2 should parse");

        assert_eq!(
            cli.command,
            Command::Storage(StorageCommand::ArtifactRetentionCleanup {
                retain_after_unix_ms: 5000,
                retain_after_by_kind: vec![
                    ArtifactKindRetentionRule {
                        kind: ArtifactKind::Keyframe,
                        retain_after_unix_ms: 1000,
                    },
                    ArtifactKindRetentionRule {
                        kind: ArtifactKind::Clip,
                        retain_after_unix_ms: 2000,
                    },
                ],
                protect_events_occurred_after_unix_ms: Some(4500),
            })
        );
    }

    #[test]
    fn parses_event_list_filters() {
        let cli = parse_cli(vec![
            "events".into(),
            "list".into(),
            "--camera-id".into(),
            "cam-garage".into(),
            "--occurred-after-ms".into(),
            "100".into(),
            "--occurred-before-ms".into(),
            "200".into(),
        ])
        .expect("event list should parse");

        assert_eq!(
            cli.command,
            Command::Events(EventsCommand::List {
                camera_id: Some("cam-garage".into()),
                occurred_after_unix_ms: Some(100),
                occurred_before_unix_ms: Some(200),
            })
        );
    }

    #[test]
    fn parses_event_artifact_lookup() {
        let cli = parse_cli(vec![
            "events".into(),
            "artifacts".into(),
            "--event-id".into(),
            "event-1".into(),
        ])
        .expect("event artifacts command should parse");

        assert_eq!(
            cli.command,
            Command::Events(EventsCommand::ListArtifacts {
                event_id: "event-1".into(),
            })
        );
    }

    #[test]
    fn parses_analysis_start_command() {
        let cli = parse_cli(vec![
            "analysis".into(),
            "start".into(),
            "--file".into(),
            "fixtures/cameras/front-door.json".into(),
            "--interval-ms".into(),
            "1000".into(),
            "--kind".into(),
            "PackageDetected".into(),
            "--severity".into(),
            "Warning".into(),
            "--detector-program".into(),
            "detector.exe".into(),
            "--detector-arg".into(),
            "{frame_path}".into(),
            "--artifacts-dir".into(),
            "data/artifacts/front-door".into(),
        ])
        .expect("analysis start should parse");

        assert_eq!(
            cli.command,
            Command::Analysis(AnalysisCommand::Start {
                file: "fixtures/cameras/front-door.json".into(),
                interval_ms: 1000,
                event_kind: EventKind::PackageDetected,
                event_severity: EventSeverity::Warning,
                message: None,
                artifacts_directory: Some("data/artifacts/front-door".into()),
                detector_program: "detector.exe".into(),
                detector_args: vec!["{frame_path}".into()],
            })
        );
    }

    #[test]
    fn parses_analysis_sync_command() {
        let cli = parse_cli(vec![
            "analysis".into(),
            "sync".into(),
            "--camera-id".into(),
            "cam-front-door".into(),
            "--max-events".into(),
            "5".into(),
        ])
        .expect("analysis sync should parse");

        assert_eq!(
            cli.command,
            Command::Analysis(AnalysisCommand::Sync {
                camera_id: "cam-front-door".into(),
                max_events: Some(5),
            })
        );
    }

    #[test]
    fn parses_camera_register_command() {
        let cli = parse_cli(vec![
            "cameras".into(),
            "register".into(),
            "--file".into(),
            "fixtures/cameras/front-door.json".into(),
        ])
        .expect("register command should parse");

        assert_eq!(
            cli.command,
            Command::Cameras(CamerasCommand::Register {
                file: "fixtures/cameras/front-door.json".into(),
                start_recording_output_directory: None,
            })
        );
    }

    #[test]
    fn parses_camera_register_with_immediate_start() {
        let cli = parse_cli(vec![
            "cameras".into(),
            "register".into(),
            "--file".into(),
            "fixtures/cameras/front-door.json".into(),
            "--start-recording".into(),
            "--output-dir".into(),
            "data/segments/front-door".into(),
        ])
        .expect("register with start should parse");

        assert_eq!(
            cli.command,
            Command::Cameras(CamerasCommand::Register {
                file: "fixtures/cameras/front-door.json".into(),
                start_recording_output_directory: Some("data/segments/front-door".into()),
            })
        );
    }

    #[test]
    fn parses_segment_query_bounds() {
        let cli = parse_cli(vec![
            "recordings".into(),
            "list-segments".into(),
            "--camera-id".into(),
            "cam-smoke".into(),
            "--started-after-ms".into(),
            "1000".into(),
            "--started-before-ms".into(),
            "2000".into(),
        ])
        .expect("segment query should parse");

        assert_eq!(
            cli.command,
            Command::Recordings(RecordingsCommand::ListSegments {
                camera_id: "cam-smoke".into(),
                started_after_unix_ms: Some(1000),
                started_before_unix_ms: Some(2000),
            })
        );
    }

    #[test]
    fn rejects_unknown_extra_args() {
        let error = parse_cli(vec!["storage".into(), "summary".into(), "extra".into()])
            .expect_err("unexpected positional argument should fail");

        assert!(error.to_string().contains("unexpected arguments"));
    }

    #[test]
    fn renders_recording_status_as_text() {
        let response = serde_json::json!({
            "camera_id": "cam-front-door",
            "state": "Running",
            "pid": 4242,
            "output_hint": "data/segments/cam-front-door-%06d.mp4"
        });

        let rendered = render_text_response(
            &Command::Recordings(RecordingsCommand::Status {
                camera_id: "cam-front-door".into(),
            }),
            &response,
        );

        assert!(rendered.contains("camera: cam-front-door"));
        assert!(rendered.contains("state: Running"));
        assert!(rendered.contains("pid: 4242"));
    }

    #[test]
    fn renders_event_list_as_text() {
        let response = serde_json::json!({
            "events": [
                {
                    "id": "event-1",
                    "kind": "PersonDetected",
                    "severity": "Warning",
                    "message": "person detected by YOLO runner"
                }
            ]
        });

        let rendered = render_text_response(
            &Command::Events(EventsCommand::List {
                camera_id: Some("cam-front-door".into()),
                occurred_after_unix_ms: None,
                occurred_before_unix_ms: None,
            }),
            &response,
        );

        assert!(rendered.contains("events: 1"));
        assert!(rendered.contains("event-1 | PersonDetected | Warning | person detected by YOLO runner"));
    }
}
