use std::path::{Path, PathBuf};

#[cfg(unix)]
use anyhow::Context;
use anyhow::{Result, anyhow, bail};
#[cfg(unix)]
use harborlookout_contracts::HARBOROS_RECORDING_WRITER_SCHEMA;
use harborlookout_contracts::{
    HARBOROS_RECORDING_WRITER_MAX_CHUNK_BYTES, RecordingWriterLeaseResolveRequest,
    RecordingWriterLeaseResponse, RecordingWriterSegmentCompleteRequest,
    RecordingWriterSegmentResponse, RecordingWriterSegmentStartRequest,
    RecordingWriterSegmentWriteRequest,
};
#[cfg(unix)]
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
#[cfg(unix)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::UnixStream;

#[cfg(unix)]
const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;

#[cfg(unix)]
#[derive(Debug, Serialize)]
struct Request<'a, T> {
    schema: &'static str,
    request_id: String,
    operation: &'a str,
    payload: T,
}

#[cfg(unix)]
#[derive(Debug, Deserialize)]
struct Response<T> {
    schema: String,
    request_id: String,
    ok: bool,
    data: Option<T>,
    error: Option<ResponseError>,
}

#[cfg(unix)]
#[derive(Debug, Deserialize)]
struct ResponseError {
    code: String,
    message: String,
}

#[derive(Debug, Clone)]
pub struct RecordingWriterClient {
    #[cfg_attr(not(unix), allow(dead_code))]
    socket_path: PathBuf,
}

impl RecordingWriterClient {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: path.as_ref().to_path_buf(),
        }
    }

    pub async fn resolve(
        &self,
        request_id: impl Into<String>,
        lease_ref: String,
    ) -> Result<RecordingWriterLeaseResponse> {
        self.call(
            request_id.into(),
            "resolve",
            RecordingWriterLeaseResolveRequest { lease_ref },
        )
        .await
    }

    pub async fn renew(
        &self,
        request_id: impl Into<String>,
        lease_ref: String,
    ) -> Result<RecordingWriterLeaseResponse> {
        self.call(
            request_id.into(),
            "renew",
            RecordingWriterLeaseResolveRequest { lease_ref },
        )
        .await
    }

    pub async fn start(
        &self,
        request_id: impl Into<String>,
        request: RecordingWriterSegmentStartRequest,
    ) -> Result<RecordingWriterSegmentResponse> {
        self.call(request_id.into(), "segment/start", request).await
    }

    pub async fn write(
        &self,
        request_id: impl Into<String>,
        request: RecordingWriterSegmentWriteRequest,
    ) -> Result<RecordingWriterSegmentResponse> {
        request
            .validate_shape()
            .map_err(|error| anyhow!("invalid writer chunk: {error}"))?;
        let padding = request
            .chunk_base64
            .bytes()
            .rev()
            .take_while(|byte| *byte == b'=')
            .count();
        let decoded_length = request
            .chunk_base64
            .len()
            .saturating_div(4)
            .saturating_mul(3)
            .saturating_sub(padding);
        if decoded_length > HARBOROS_RECORDING_WRITER_MAX_CHUNK_BYTES {
            bail!("recording writer chunk exceeds 1 MiB");
        }
        self.call(request_id.into(), "segment/write", request).await
    }

    pub async fn complete(
        &self,
        request_id: impl Into<String>,
        request: RecordingWriterSegmentCompleteRequest,
    ) -> Result<RecordingWriterSegmentResponse> {
        request
            .validate_shape()
            .map_err(|error| anyhow!("invalid writer completion: {error}"))?;
        self.call(request_id.into(), "segment/complete", request)
            .await
    }

    async fn call<T, R>(&self, request_id: String, operation: &str, payload: T) -> Result<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        #[cfg(not(unix))]
        {
            let _ = (request_id, operation, payload);
            bail!("recording writer IPC requires Unix-domain sockets");
        }
        #[cfg(unix)]
        {
            let request = Request {
                schema: HARBOROS_RECORDING_WRITER_SCHEMA,
                request_id: request_id.clone(),
                operation,
                payload,
            };
            let body = serde_json::to_vec(&request)?;
            if body.len() > MAX_FRAME_BYTES {
                bail!("recording writer request exceeds the maximum frame size");
            }
            let mut stream = UnixStream::connect(&self.socket_path)
                .await
                .with_context(|| {
                    format!(
                        "connect to recording writer socket {}",
                        self.socket_path.display()
                    )
                })?;
            stream.write_u32(body.len() as u32).await?;
            stream.write_all(&body).await?;
            stream.flush().await?;
            let length = stream.read_u32().await? as usize;
            if length == 0 || length > MAX_FRAME_BYTES {
                bail!("recording writer returned an invalid frame length");
            }
            let mut response_bytes = vec![0; length];
            stream.read_exact(&mut response_bytes).await?;
            let response: Response<R> = serde_json::from_slice(&response_bytes)?;
            if response.schema != HARBOROS_RECORDING_WRITER_SCHEMA
                || response.request_id != request_id
            {
                bail!("recording writer response identity does not match the request");
            }
            if !response.ok {
                let error = response
                    .error
                    .ok_or_else(|| anyhow!("recording writer returned an error without details"))?;
                bail!("{}: {}", error.code, error.message);
            }
            response
                .data
                .ok_or_else(|| anyhow!("recording writer response did not contain data"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

    #[cfg(unix)]
    use serde_json::{Value, json};
    #[cfg(unix)]
    use std::time::{SystemTime, UNIX_EPOCH};
    #[cfg(unix)]
    use tokio::net::UnixListener;

    #[cfg(unix)]
    fn test_socket_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "harborlookout-writer-{label}-{}-{nonce}.sock",
            std::process::id()
        ))
    }

    #[cfg(unix)]
    async fn spawn_response_server(
        label: &str,
        declared_length: u32,
        response: Vec<u8>,
    ) -> (PathBuf, tokio::task::JoinHandle<()>) {
        let path = test_socket_path(label);
        let listener = UnixListener::bind(&path).unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request_length = stream.read_u32().await.unwrap() as usize;
            assert!(request_length > 0 && request_length <= MAX_FRAME_BYTES);
            let mut request_bytes = vec![0; request_length];
            stream.read_exact(&mut request_bytes).await.unwrap();
            let request: Value = serde_json::from_slice(&request_bytes).unwrap();
            assert_eq!(request["schema"], HARBOROS_RECORDING_WRITER_SCHEMA);
            assert_eq!(request["request_id"], "request-1");
            assert_eq!(request["operation"], "resolve");
            let encoded = String::from_utf8(request_bytes).unwrap();
            assert!(!encoded.contains("output_directory"));
            assert!(!encoded.contains("/dev/"));
            stream.write_u32(declared_length).await.unwrap();
            if !response.is_empty() {
                stream.write_all(&response).await.unwrap();
            }
        });
        (path, task)
    }

    #[test]
    fn client_has_no_path_bearing_writer_payload() {
        let request = RecordingWriterSegmentStartRequest {
            lease_ref: "lease-1".into(),
            segment_id: "segment-1".into(),
            sequence: 1,
            started_at: None,
        };
        let encoded = serde_json::to_string(&request).unwrap();
        assert!(!encoded.contains("output_directory"));
        assert!(!encoded.contains("mount"));
        assert!(!encoded.contains("/data"));
    }

    #[test]
    fn client_accepts_one_mib_chunk_and_rejects_larger_chunk() {
        let client = RecordingWriterClient::new("missing-recording-writer.sock");
        let request = |size| RecordingWriterSegmentWriteRequest {
            lease_ref: "lease-1".into(),
            segment_id: "segment-1".into(),
            chunk_base64: BASE64.encode(vec![0u8; size]),
        };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let exact = runtime
            .block_on(client.write(
                "request-exact",
                request(HARBOROS_RECORDING_WRITER_MAX_CHUNK_BYTES),
            ))
            .unwrap_err();
        assert!(!exact.to_string().contains("exceeds 1 MiB"));

        let oversized = runtime
            .block_on(client.write(
                "request-oversized",
                request(HARBOROS_RECORDING_WRITER_MAX_CHUNK_BYTES + 1),
            ))
            .unwrap_err();
        assert!(oversized.to_string().contains("exceeds 1 MiB"));
    }

    #[cfg(unix)]
    #[test]
    fn client_rejects_invalid_chunk_before_connecting() {
        let client = RecordingWriterClient::new("/run/harboros/recording-writer.sock");
        let request = RecordingWriterSegmentWriteRequest {
            lease_ref: "lease-1".into(),
            segment_id: "segment-1".into(),
            chunk_base64: "not base64".into(),
        };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let error = runtime
            .block_on(client.write("request-1", request))
            .unwrap_err();
        assert!(error.to_string().contains("invalid writer chunk"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn client_uses_length_prefixed_frames_and_accepts_matching_identity() {
        let response = serde_json::to_vec(&json!({
            "schema": HARBOROS_RECORDING_WRITER_SCHEMA,
            "request_id": "request-1",
            "ok": true,
            "data": {
                "lease_ref": "lease-1",
                "job_id": "job-1",
                "slot_id": "TF-1",
                "recording_expires_at": "2099-01-01T00:00:00Z",
                "recording_state": "active",
                "capability_profile": "tf_recording",
                "active_segment_id": null
            },
            "error": null
        }))
        .unwrap();
        let (path, server) =
            spawn_response_server("success", response.len() as u32, response).await;
        let result = RecordingWriterClient::new(&path)
            .resolve("request-1", "lease-1".to_string())
            .await
            .unwrap();
        assert_eq!(result.slot_id, "TF-1");
        server.await.unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn client_rejects_response_identity_mismatch() {
        let response = serde_json::to_vec(&json!({
            "schema": HARBOROS_RECORDING_WRITER_SCHEMA,
            "request_id": "different-request",
            "ok": true,
            "data": {
                "lease_ref": "lease-1",
                "job_id": "job-1",
                "slot_id": "TF-1",
                "recording_expires_at": "2099-01-01T00:00:00Z",
                "recording_state": "active",
                "capability_profile": "tf_recording",
                "active_segment_id": null
            },
            "error": null
        }))
        .unwrap();
        let (path, server) =
            spawn_response_server("identity", response.len() as u32, response).await;
        let error = RecordingWriterClient::new(&path)
            .resolve("request-1", "lease-1".to_string())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("identity does not match"));
        server.await.unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn client_rejects_oversized_response_before_allocating_payload() {
        let (path, server) =
            spawn_response_server("oversized", (MAX_FRAME_BYTES + 1) as u32, Vec::new()).await;
        let error = RecordingWriterClient::new(&path)
            .resolve("request-1", "lease-1".to_string())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalid frame length"));
        server.await.unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn client_rejects_truncated_response() {
        let (path, server) = spawn_response_server("truncated", 32, b"{}".to_vec()).await;
        let error = RecordingWriterClient::new(&path)
            .resolve("request-1", "lease-1".to_string())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("early eof") || error.to_string().contains("failed to fill")
        );
        server.await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
