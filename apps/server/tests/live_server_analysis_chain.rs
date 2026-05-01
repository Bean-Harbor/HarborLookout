#![cfg(windows)]

use std::fs;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use harborlookout_contracts::{
    AnalysisStatusRequest, ListEventArtifactsRequest, ListSurveillanceEventsRequest,
    LocalDetectorConfig, StartAnalysisRequest, StartAnalysisResponse, StopAnalysisRequest,
    StopAnalysisResponse, SyncAnalysisEventsRequest, SyncAnalysisEventsResponse,
};
use harborlookout_domain::{
    Camera, CameraId, CameraStream, EventId, EventKind, EventSeverity, RtspSource,
    RtspTransport, StreamProfile, StreamRole,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn real_server_syncs_analysis_events_from_real_worker_chain() -> Result<()> {
    let temp_root = std::env::temp_dir().join(format!(
        "harborlookout-live-server-analysis-{}-{}",
        std::process::id(),
        chrono_like_now()
    ));
    let _ = fs::remove_dir_all(&temp_root);
    fs::create_dir_all(&temp_root)?;

    let ffmpeg_shim = temp_root.join("fake-ffmpeg.cmd");
    write_fake_ffmpeg_shim(&ffmpeg_shim)?;
    let mock_yolo_output = temp_root.join("mock-yolo-output.json");
    fs::write(
        &mock_yolo_output,
        r#"{"detections":[{"class_name":"person","confidence":0.97}]}"#,
    )?;

    let worker_address = format!("127.0.0.1:{}", reserve_local_port()?);
    let server_address = format!("127.0.0.1:{}", reserve_local_port()?);
    let data_directory = temp_root.join("server-data");
    fs::create_dir_all(&data_directory)?;

    let mut worker = spawn_worker(&worker_address, &ffmpeg_shim)?;
    if let Err(error) = wait_for_health(&worker_address).await {
        cleanup_child(&mut worker);
        return Err(error);
    }

    let mut server = spawn_server(&server_address, &worker_address, &data_directory)?;
    if let Err(error) = wait_for_health(&server_address).await {
        cleanup_child(&mut server);
        cleanup_child(&mut worker);
        return Err(error);
    }

    let detector_program = ensure_binary("harborlookout-detector-adapter")?;
    let yolo_runner_program = ensure_binary("harborlookout-model-runner-yolo")?;
    let artifacts_directory = temp_root.join("artifacts");
    let camera = sample_camera();

    let start_response: StartAnalysisResponse = post_json(
        &server_address,
        "/api/analysis/start",
        &StartAnalysisRequest {
            camera: camera.clone(),
            interval_ms: 1,
            event_kind: EventKind::PackageDetected,
            event_severity: EventSeverity::Warning,
            message: Some("fallback package detection".into()),
            artifacts_directory: Some(artifacts_directory.to_string_lossy().to_string()),
            detector: LocalDetectorConfig {
                program: detector_program.to_string_lossy().to_string(),
                args: vec![
                    "--frame".into(),
                    "{frame_path}".into(),
                    "--event-kind".into(),
                    "{event_kind}".into(),
                    "--event-severity".into(),
                    "{event_severity}".into(),
                    "--backend".into(),
                    "runner".into(),
                    "--runner-program".into(),
                    yolo_runner_program.to_string_lossy().to_string(),
                    "--runner-arg".into(),
                    "--image".into(),
                    "--runner-arg".into(),
                    "{frame_path}".into(),
                    "--runner-arg".into(),
                    "--model".into(),
                    "--runner-arg".into(),
                    "models/yolov8n.pt".into(),
                    "--runner-arg".into(),
                    "--fallback-kind".into(),
                    "--runner-arg".into(),
                    "{event_kind}".into(),
                    "--runner-arg".into(),
                    "--mock-output-file".into(),
                    "--runner-arg".into(),
                    mock_yolo_output.to_string_lossy().to_string(),
                ],
            },
        },
    )
    .await?;
    assert_eq!(start_response.camera_id, "cam-live-analysis");

    let status_response: serde_json::Value = post_json(
        &server_address,
        "/api/analysis/status",
        &AnalysisStatusRequest {
            camera_id: camera.id.clone(),
        },
    )
    .await?;
    assert_eq!(status_response["state"], "Running");
    assert_eq!(status_response["detector_program"], detector_program.to_string_lossy().to_string());

    let sync_response: SyncAnalysisEventsResponse = post_json(
        &server_address,
        "/api/analysis/events/sync",
        &SyncAnalysisEventsRequest {
            camera_id: camera.id.clone(),
            max_events: Some(1),
        },
    )
    .await?;
    assert_eq!(sync_response.stored_events, 1);
    assert_eq!(sync_response.stored_artifacts, 1);

    let events_response: serde_json::Value = post_json(
        &server_address,
        "/api/events/query",
        &ListSurveillanceEventsRequest {
            camera_id: Some(camera.id.clone()),
            occurred_after_unix_ms: None,
            occurred_before_unix_ms: None,
        },
    )
    .await?;
    assert_eq!(events_response["events"].as_array().unwrap().len(), 1);
    assert_eq!(events_response["events"][0]["kind"], "PersonDetected");
    assert_eq!(events_response["events"][0]["message"], "person detected by YOLO runner");

    let artifact_event_id = events_response["events"][0]["id"]
        .as_str()
        .expect("event id should be present")
        .to_string();
    let artifacts_response: serde_json::Value = post_json(
        &server_address,
        "/api/events/artifacts/query",
        &ListEventArtifactsRequest {
            event_id: EventId(artifact_event_id),
        },
    )
    .await?;
    assert_eq!(artifacts_response["artifacts"].as_array().unwrap().len(), 1);
    let artifact_path = artifacts_response["artifacts"][0]["path"]
        .as_str()
        .expect("artifact path should be present");
    assert!(Path::new(artifact_path).exists());

    let stop_response: StopAnalysisResponse = post_json(
        &server_address,
        "/api/analysis/stop",
        &StopAnalysisRequest {
            camera_id: CameraId("cam-live-analysis".into()),
        },
    )
    .await?;
    assert!(stop_response.stopped);

    cleanup_child(&mut server);
    cleanup_child(&mut worker);
    let _ = fs::remove_dir_all(&temp_root);
    Ok(())
}

fn spawn_worker(bind_address: &str, ffmpeg_program: &Path) -> Result<Child> {
    let worker_path = ensure_binary("harborlookout-worker")?;
    Command::new(worker_path)
        .env("HARBORLOOKOUT_WORKER_BIND", bind_address)
        .env("HARBORLOOKOUT_FFMPEG_PROGRAM", ffmpeg_program)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn real worker process")
}

fn spawn_server(bind_address: &str, worker_address: &str, data_directory: &Path) -> Result<Child> {
    let server_path = ensure_binary("harborlookout-server")?;
    Command::new(server_path)
        .env("HARBORLOOKOUT_SERVER_BIND", bind_address)
        .env("HARBORLOOKOUT_WORKER_URL", format!("http://{worker_address}"))
        .env("HARBORLOOKOUT_DATA_DIR", data_directory)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn real server process")
}

async fn wait_for_health(address: &str) -> Result<()> {
    for _ in 0..60 {
        if get_json::<serde_json::Value>(address, "/health").await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(anyhow!("service did not become healthy in time"))
}

fn cleanup_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn write_fake_ffmpeg_shim(path: &Path) -> Result<()> {
    fs::write(
        path,
        r#"@echo off
setlocal enabledelayedexpansion
set LAST=
:loop
if "%~1"=="" goto done
set LAST=%~1
shift
goto loop
:done
powershell -NoProfile -Command "$output = '%LAST%'; [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($output)) | Out-Null; [IO.File]::WriteAllBytes($output, [byte[]](255,216,255,224,0,16,74,70,73,70,0,1,1,0,0,1,0,1,0,0,16,32,48,64,80,96,112,128));"
exit /b 0
"#,
    )?;
    Ok(())
}

fn reserve_local_port() -> Result<u16> {
    let listener = StdTcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn ensure_binary(name: &str) -> Result<PathBuf> {
    let workspace_root = workspace_root()?;
    let status = Command::new("cargo")
        .args(["build", "-p", name, "--offline"])
        .current_dir(&workspace_root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("failed to build binary `{name}` for integration smoke"))?;
    if !status.success() {
        return Err(anyhow!("cargo build failed for binary `{name}`"));
    }

    let path = binary_path(name)?;
    if !path.exists() {
        return Err(anyhow!("binary not found after build: {}", path.display()));
    }
    Ok(path)
}

fn binary_path(name: &str) -> Result<PathBuf> {
    let current_exe = std::env::current_exe()?;
    let target_debug = current_exe
        .parent()
        .and_then(|path| path.parent())
        .ok_or_else(|| anyhow!("failed to locate target/debug directory"))?;
    Ok(target_debug.join(format!("{name}.exe")))
}

fn workspace_root() -> Result<PathBuf> {
    let current_exe = std::env::current_exe()?;
    current_exe
        .parent()
        .and_then(|path| path.parent())
        .and_then(|path| path.parent())
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("failed to locate workspace root"))
}

async fn post_json<TRequest, TResponse>(address: &str, path: &str, request: &TRequest) -> Result<TResponse>
where
    TRequest: serde::Serialize + ?Sized,
    TResponse: serde::de::DeserializeOwned,
{
    let body = serde_json::to_string(request)?;
    let mut stream = TcpStream::connect(address).await?;
    let request_text = format!(
        "POST {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body,
    );
    stream.write_all(request_text.as_bytes()).await?;

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).await?;
    parse_json_response(&response_bytes)
}

async fn get_json<TResponse>(address: &str, path: &str) -> Result<TResponse>
where
    TResponse: serde::de::DeserializeOwned,
{
    let mut stream = TcpStream::connect(address).await?;
    let request_text = format!(
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request_text.as_bytes()).await?;

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).await?;
    parse_json_response(&response_bytes)
}

fn parse_json_response<T: serde::de::DeserializeOwned>(response_bytes: &[u8]) -> Result<T> {
    let response = String::from_utf8(response_bytes.to_vec())?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .or_else(|| response.split_once("\n\n"))
        .ok_or_else(|| anyhow!("invalid HTTP response"))?;
    let status_code: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| anyhow!("missing status code"))?
        .parse()?;

    if status_code >= 400 {
        return Err(anyhow!("request failed: {status_code} {body}"));
    }

    Ok(serde_json::from_str(body)?)
}

fn sample_camera() -> Camera {
    Camera {
        id: CameraId("cam-live-analysis".into()),
        name: "Live Analysis".into(),
        streams: vec![
            CameraStream {
                role: StreamRole::Detect,
                source: RtspSource {
                    url: "rtsp://camera.local/live-analysis/detect".into(),
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
                    url: "rtsp://camera.local/live-analysis/record".into(),
                    transport: RtspTransport::Tcp,
                },
                stream_profile: StreamProfile {
                    width: 1920,
                    height: 1080,
                    fps: 15,
                    segment_seconds: 10,
                },
            },
        ],
        source: RtspSource {
            url: "rtsp://camera.local/live-analysis/record".into(),
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

fn chrono_like_now() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_millis()
}