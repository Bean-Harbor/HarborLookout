use anyhow::Result;
use harborlookout_domain::{Camera, RtspTransport};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeSummary {
    pub backend: &'static str,
    pub transport: RtspTransport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingPlan {
    pub program: String,
    pub args: Vec<String>,
    pub output_hint: String,
}

impl RecordingPlan {
    pub fn program_string(&self) -> String {
        self.program.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPlan {
    pub program: String,
    pub args: Vec<String>,
    pub output_path: String,
}

impl SnapshotPlan {
    pub fn program_string(&self) -> String {
        self.program.clone()
    }
}

pub trait MediaBackend: Send + Sync {
    fn name(&self) -> &'static str;

    fn probe_camera(&self, camera: &Camera) -> Result<ProbeSummary>;

    fn build_recording_plan(&self, camera: &Camera, output_directory: &str) -> Result<RecordingPlan>;

    fn build_snapshot_plan(&self, camera: &Camera, output_path: &str) -> Result<SnapshotPlan>;
}