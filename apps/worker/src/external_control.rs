use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;

use anyhow::{Context, Result, bail};
use axum::extract::State;
use harborlookout_domain::{Camera, CameraId, ExternalRecordingLease};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tracing::warn;

use super::{
    WorkerAppState, recording_status, start_external_recording, stop_external_recording,
    valid_camera_id, valid_opaque_ref,
};
use harborlookout_contracts::RecordingStatusRequest;

const SCHEMA: &str = "harborlookout.external-recording-control.v1";
const MAX_FRAME_BYTES: usize = 64 * 1024;
const DEFAULT_SOCKET: &str = "/run/harborlookout/external-control.sock";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    schema: String,
    request_id: String,
    operation: String,
    payload: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StartPayload {
    camera: Camera,
    recording_lease: ExternalRecordingLease,
    session_ref: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionPayload {
    camera_id: String,
    session_ref: String,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: &'a str,
}

#[derive(Serialize)]
struct Response<'a> {
    schema: &'static str,
    request_id: &'a str,
    ok: bool,
    data: Option<Value>,
    error: Option<ErrorBody<'a>>,
}

pub(super) async fn start_if_configured(state: WorkerAppState) -> Result<()> {
    let allowed_uid = match std::env::var("HARBORLOOKOUT_EXTERNAL_CONTROL_UID") {
        Ok(value) => value
            .parse::<u32>()
            .context("invalid external control UID")?,
        Err(std::env::VarError::NotPresent) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let socket_path = std::env::var("HARBORLOOKOUT_EXTERNAL_CONTROL_SOCKET")
        .unwrap_or_else(|_| DEFAULT_SOCKET.into());
    let path = Path::new(&socket_path);
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.file_type().is_socket() {
            bail!("external control socket path is occupied by a non-socket file");
        }
        if UnixStream::connect(path).await.is_ok() {
            bail!("external control socket is already active");
        }
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)
        .with_context(|| format!("bind external control socket {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))?;
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let state = state.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_connection(stream, allowed_uid, state).await {
                            warn!("external recording control connection failed: {error:#}");
                        }
                    });
                }
                Err(error) => warn!("external recording control accept failed: {error}"),
            }
        }
    });
    Ok(())
}

async fn serve_connection(
    mut stream: UnixStream,
    allowed_uid: u32,
    state: WorkerAppState,
) -> Result<()> {
    if stream.peer_cred()?.uid() != allowed_uid {
        bail!("unauthorized external recording control peer");
    }
    let length = stream.read_u32().await? as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        bail!("invalid external recording control frame length");
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    let request: Request = serde_json::from_slice(&bytes)?;
    let response = dispatch(&state, &request).await;
    let body = serde_json::to_vec(&response)?;
    if body.len() > MAX_FRAME_BYTES {
        bail!("external recording control response too large");
    }
    stream.write_u32(body.len() as u32).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn dispatch<'a>(state: &WorkerAppState, request: &'a Request) -> Response<'a> {
    if request.schema != SCHEMA || !valid_opaque_ref(&request.request_id) {
        return failure(
            &request.request_id,
            "BAD_REQUEST",
            "invalid schema or request ID",
        );
    }
    let result = match request.operation.as_str() {
        "start" => match serde_json::from_value::<StartPayload>(request.payload.clone()) {
            Ok(payload) => start_external_recording(
                State(state.clone()),
                payload.camera,
                payload.recording_lease,
                payload.session_ref,
            )
            .await
            .map(|response| {
                serde_json::to_value(response.0).expect("serializable recording response")
            }),
            Err(_) => return failure(&request.request_id, "BAD_REQUEST", "invalid start payload"),
        },
        "stop" | "status" => {
            let payload = match serde_json::from_value::<SessionPayload>(request.payload.clone()) {
                Ok(payload) => payload,
                Err(_) => {
                    return failure(
                        &request.request_id,
                        "BAD_REQUEST",
                        "invalid session payload",
                    );
                }
            };
            if !valid_camera_id(&payload.camera_id) || !valid_opaque_ref(&payload.session_ref) {
                return failure(
                    &request.request_id,
                    "BAD_REQUEST",
                    "invalid session identity",
                );
            }
            if request.operation == "stop" {
                stop_external_recording(state, &payload.camera_id, &payload.session_ref)
                    .await
                    .map(|response| {
                        serde_json::to_value(response).expect("serializable stop response")
                    })
            } else {
                let sessions = state.external_sessions.lock().await;
                if sessions
                    .get(&payload.camera_id)
                    .is_none_or(|session| session.session_ref != payload.session_ref)
                {
                    return failure(
                        &request.request_id,
                        "NOT_FOUND",
                        "external recording session not found",
                    );
                }
                drop(sessions);
                recording_status(
                    State(state.clone()),
                    axum::Json(RecordingStatusRequest {
                        camera_id: CameraId(payload.camera_id),
                    }),
                )
                .await
                .map(|response| {
                    serde_json::to_value(response.0).expect("serializable status response")
                })
            }
        }
        _ => {
            return failure(
                &request.request_id,
                "BAD_OPERATION",
                "unsupported operation",
            );
        }
    };
    match result {
        Ok(data) => Response {
            schema: SCHEMA,
            request_id: &request.request_id,
            ok: true,
            data: Some(data),
            error: None,
        },
        Err((status, _)) => {
            let code = if status.is_client_error() {
                "REJECTED"
            } else {
                "FAILED"
            };
            failure(
                &request.request_id,
                code,
                "external recording operation failed",
            )
        }
    }
}

fn failure<'a>(request_id: &'a str, code: &'a str, message: &'a str) -> Response<'a> {
    Response {
        schema: SCHEMA,
        request_id,
        ok: false,
        data: None,
        error: Some(ErrorBody { code, message }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use harborlookout_media_ffmpeg::FfmpegBackend;
    use tokio::sync::Mutex;

    fn state() -> WorkerAppState {
        WorkerAppState {
            backend: FfmpegBackend,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            external_sessions: Arc::new(Mutex::new(HashMap::new())),
            external_control_gate: Arc::new(Mutex::new(())),
            analyses: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[tokio::test]
    async fn rejects_unknown_schema_and_operation_without_starting_recording() {
        let state = state();
        let mut request = Request {
            schema: "wrong.v1".into(),
            request_id: "request-1".into(),
            operation: "start".into(),
            payload: serde_json::json!({}),
        };
        assert!(!dispatch(&state, &request).await.ok);
        request.schema = SCHEMA.into();
        request.operation = "execute".into();
        assert!(!dispatch(&state, &request).await.ok);
        request.operation = "start".into();
        request.payload = serde_json::json!({"output_directory": "/tmp/video"});
        assert!(!dispatch(&state, &request).await.ok);
        assert!(state.external_sessions.lock().await.is_empty());
    }

    #[tokio::test]
    async fn rejects_path_shaped_session_identity() {
        let state = state();
        let request = Request {
            schema: SCHEMA.into(),
            request_id: "request-2".into(),
            operation: "stop".into(),
            payload: serde_json::json!({"camera_id": "camera-1", "session_ref": "../secret"}),
        };
        assert!(!dispatch(&state, &request).await.ok);
    }
}
