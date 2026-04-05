use tracing::info;

fn main() {
    tracing_subscriber::fmt::init();
    info!("HarborLookout CLI bootstrap");
}