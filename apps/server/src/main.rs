use anyhow::Result;
use harborlookout_control_plane::HttpWorkerClient;
use harborlookout_media_ffmpeg::FfmpegBackend;
use harborlookout_server::{build_app, AppState, ServerControlPlane};
use harborlookout_storage::SqliteCameraStore;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let data_directory = std::env::var("HARBORLOOKOUT_DATA_DIR")
        .unwrap_or_else(|_| "data".to_string());
    std::fs::create_dir_all(&data_directory)?;
    let database_path = std::path::Path::new(&data_directory).join("harborlookout.db");
    let store = SqliteCameraStore::open(&database_path)?;
    let worker_url = std::env::var("HARBORLOOKOUT_WORKER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:5050".to_string());
    let bind_address = std::env::var("HARBORLOOKOUT_SERVER_BIND")
        .unwrap_or_else(|_| "127.0.0.1:4040".to_string());

    let state = AppState {
        control_plane: std::sync::Arc::new(ServerControlPlane::new(
            FfmpegBackend,
            store,
            HttpWorkerClient::new(worker_url),
        )),
    };
    let app = build_app(state);

    let listener = TcpListener::bind(&bind_address).await?;
    info!(address = %listener.local_addr()?, "HarborLookout server listening");

    axum::serve(listener, app).await?;
    Ok(())
}
