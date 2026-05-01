#![cfg(windows)]

use std::fs;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use harborlookout_domain::{
    Camera, CameraId, CameraStream, RtspSource, RtspTransport, StreamProfile, StreamRole,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn real_cli_drives_analysis_chain_end_to_end() -> Result<()> {
    let temp_root = std::env::temp_dir().join(format!(
        "harborlookout-live-cli-analysis-{}-{}",
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
    let artifacts_directory = temp_root.join("artifacts");
    fs::create_dir_all(&data_directory)?;
    fs::create_dir_all(&artifacts_directory)?;

    let camera_path = temp_root.join("camera-live-analysis.json");
    write_camera_json(&camera_path)?;

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

    let start_output = run_cli(
        &server_address,
        &[
            "analysis",
            "start",
            "--file",
            camera_path.to_string_lossy().as_ref(),
            "--interval-ms",
            "1",
            "--kind",
            "PackageDetected",
            "--severity",
            "Warning",
            "--message",
            "fallback package detection",
            "--artifacts-dir",
            artifacts_directory.to_string_lossy().as_ref(),
            "--detector-program",
            detector_program.to_string_lossy().as_ref(),
            "--detector-arg",
            "--frame",
            "--detector-arg",
            "{frame_path}",
            "--detector-arg",
            "--event-kind",
            "--detector-arg",
            "{event_kind}",
            "--detector-arg",
            "--event-severity",
            "--detector-arg",
            "{event_severity}",
            "--detector-arg",
            "--backend",
            "--detector-arg",
            "runner",
            "--detector-arg",
            "--runner-program",
            "--detector-arg",
            yolo_runner_program.to_string_lossy().as_ref(),
            "--detector-arg",
            "--runner-arg",
            "--detector-arg",
            "--image",
            "--detector-arg",
            "--runner-arg",
            "--detector-arg",
            "{frame_path}",
            "--detector-arg",
            "--runner-arg",
            "--detector-arg",
            "--model",
            "--detector-arg",
            "--runner-arg",
            "--detector-arg",
            "models/yolov8n.pt",
            "--detector-arg",
            "--runner-arg",
            "--detector-arg",
            "--fallback-kind",
            "--detector-arg",
            "--runner-arg",
            "--detector-arg",
            "{event_kind}",
            "--detector-arg",
            "--runner-arg",
            "--detector-arg",
            "--mock-output-file",
            "--detector-arg",
            "--runner-arg",
            "--detector-arg",
            mock_yolo_output.to_string_lossy().as_ref(),
        ],
    )?;
    assert_eq!(start_output["camera_id"], "cam-live-analysis");

    let status_output = run_cli(
        &server_address,
        &["analysis", "status", "--camera-id", "cam-live-analysis"],
    )?;
    assert_eq!(status_output["state"], "Running");
    assert_eq!(
        status_output["detector_program"],
        detector_program.to_string_lossy().to_string()
    );

    let sync_output = run_cli(
        &server_address,
        &[
            "analysis",
            "sync",
            "--camera-id",
            "cam-live-analysis",
            "--max-events",
            "1",
        ],
    )?;
    assert_eq!(sync_output["stored_events"], 1);
    assert_eq!(sync_output["stored_artifacts"], 1);

    let events_output = run_cli(
        &server_address,
        &["events", "list", "--camera-id", "cam-live-analysis"],
    )?;
    assert_eq!(events_output["events"].as_array().unwrap().len(), 1);
    assert_eq!(events_output["events"][0]["kind"], "PersonDetected");
    assert_eq!(events_output["events"][0]["message"], "person detected by YOLO runner");

    let event_id = events_output["events"][0]["id"]
        .as_str()
        .expect("event id should be present")
        .to_string();
    let artifacts_output = run_cli(
        &server_address,
        &["events", "artifacts", "--event-id", &event_id],
    )?;
    assert_eq!(artifacts_output["artifacts"].as_array().unwrap().len(), 1);
    let artifact_path = artifacts_output["artifacts"][0]["path"]
        .as_str()
        .expect("artifact path should be present");
    assert!(Path::new(artifact_path).exists());

    let stop_output = run_cli(
        &server_address,
        &["analysis", "stop", "--camera-id", "cam-live-analysis"],
    )?;
    assert_eq!(stop_output["stopped"], true);

    cleanup_child(&mut server);
    cleanup_child(&mut worker);
    let _ = fs::remove_dir_all(&temp_root);
    Ok(())
}

fn write_camera_json(path: &Path) -> Result<()> {
    let camera = sample_camera();
    fs::write(path, serde_json::to_string_pretty(&camera)?)?;
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

fn run_cli(server_address: &str, args: &[&str]) -> Result<serde_json::Value> {
    let cli_path = ensure_binary("harborlookout-cli")?;
    let output = Command::new(cli_path)
        .arg("--server")
        .arg(format!("http://{server_address}"))
        .arg("--json")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to run CLI process")?;

    if !output.status.success() {
        return Err(anyhow!(
            "CLI command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let stdout = String::from_utf8(output.stdout)?;
    serde_json::from_str(&stdout).context("CLI returned invalid JSON")
}

async fn wait_for_health(address: &str) -> Result<()> {
    for _ in 0..60 {
        if get_json::<serde_json::Value>(address, "/health").await.is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
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