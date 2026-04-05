use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;
use harborlookout_domain::{
    Camera, CameraId, CameraStream, RecordingSegment, RecordingSession, RecordingState, RtspSource,
    StreamProfile,
};
use harborlookout_contracts::{
    RetentionPreviewCamera, RetentionPreviewResponse, StorageCameraSummary, StorageSummaryResponse,
};
use rusqlite::{params, Connection};

pub trait CameraStore: Send + Sync {
    fn upsert_camera(&self, camera: &Camera) -> Result<()>;
    fn get_camera(&self, camera_id: &CameraId) -> Result<Option<Camera>>;
    fn list_cameras(&self) -> Result<Vec<Camera>>;
}

pub trait RecordingSegmentStore: Send + Sync {
    fn upsert_segment(&self, segment: &RecordingSegment) -> Result<()>;
    fn list_segments(
        &self,
        camera_id: &CameraId,
        started_after_unix_ms: Option<u64>,
        started_before_unix_ms: Option<u64>,
    ) -> Result<Vec<RecordingSegment>>;
    fn storage_summary(&self, active_recordings: usize) -> Result<StorageSummaryResponse>;
    fn retention_preview(&self, retain_after_unix_ms: u64) -> Result<RetentionPreviewResponse>;
}

pub trait RecordingSessionStore: Send + Sync {
    fn upsert_session(&self, session: &RecordingSession) -> Result<()>;
    fn get_session(&self, camera_id: &CameraId) -> Result<Option<RecordingSession>>;
    fn list_sessions(&self) -> Result<Vec<RecordingSession>>;
    fn delete_session(&self, camera_id: &CameraId) -> Result<()>;
}

pub struct SqliteCameraStore {
    connection: Mutex<Connection>,
}

impl SqliteCameraStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let connection = Connection::open(path)?;
        let store = Self {
            connection: Mutex::new(connection),
        };
        store.initialize()?;
        Ok(store)
    }

    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        let connection = Connection::open_in_memory()?;
        let store = Self {
            connection: Mutex::new(connection),
        };
        store.initialize()?;
        Ok(store)
    }

    fn initialize(&self) -> Result<()> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cameras (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                streams_json TEXT NOT NULL DEFAULT '[]',
                source_json TEXT NOT NULL,
                stream_profile_json TEXT NOT NULL,
                enabled INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS recording_segments (
                camera_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                path TEXT NOT NULL,
                started_at_unix_ms INTEGER NOT NULL,
                ended_at_unix_ms INTEGER,
                duration_ms INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL,
                state TEXT NOT NULL,
                PRIMARY KEY (camera_id, sequence)
            );

            CREATE TABLE IF NOT EXISTS recording_sessions (
                camera_id TEXT PRIMARY KEY,
                worker_name TEXT NOT NULL,
                output_directory TEXT NOT NULL,
                output_hint TEXT,
                state TEXT NOT NULL,
                pid INTEGER,
                started_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                last_error TEXT
            );
            ",
        )?;
        let _ = connection.execute(
            "ALTER TABLE cameras ADD COLUMN streams_json TEXT NOT NULL DEFAULT '[]'",
            [],
        );
        Ok(())
    }
}

impl CameraStore for SqliteCameraStore {
    fn upsert_camera(&self, camera: &Camera) -> Result<()> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        connection.execute(
            "
            INSERT INTO cameras (id, name, streams_json, source_json, stream_profile_json, enabled)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                streams_json = excluded.streams_json,
                source_json = excluded.source_json,
                stream_profile_json = excluded.stream_profile_json,
                enabled = excluded.enabled
            ",
            params![
                camera.id.0,
                camera.name,
                serde_json::to_string(&camera.streams)?,
                serde_json::to_string(&camera.source)?,
                serde_json::to_string(&camera.stream_profile)?,
                if camera.enabled { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    fn get_camera(&self, camera_id: &CameraId) -> Result<Option<Camera>> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let mut statement = connection.prepare(
            "SELECT id, name, streams_json, source_json, stream_profile_json, enabled FROM cameras WHERE id = ?1",
        )?;

        match statement.query_row(params![camera_id.0], map_camera_row) {
            Ok(camera) => Ok(Some(camera)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn list_cameras(&self) -> Result<Vec<Camera>> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let mut statement = connection.prepare(
            "SELECT id, name, streams_json, source_json, stream_profile_json, enabled FROM cameras ORDER BY id ASC",
        )?;

        let rows = statement.query_map([], |row| map_camera_row(row))?;

        let mut cameras = Vec::new();
        for camera in rows {
            cameras.push(camera?);
        }
        Ok(cameras)
    }
}

impl RecordingSegmentStore for SqliteCameraStore {
    fn upsert_segment(&self, segment: &RecordingSegment) -> Result<()> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        connection.execute(
            "
            INSERT INTO recording_segments (
                camera_id,
                sequence,
                path,
                started_at_unix_ms,
                ended_at_unix_ms,
                duration_ms,
                size_bytes,
                state
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(camera_id, sequence) DO UPDATE SET
                path = excluded.path,
                started_at_unix_ms = excluded.started_at_unix_ms,
                ended_at_unix_ms = excluded.ended_at_unix_ms,
                duration_ms = excluded.duration_ms,
                size_bytes = excluded.size_bytes,
                state = excluded.state
            ",
            params![
                segment.camera_id.0,
                segment.sequence as i64,
                segment.path,
                segment.started_at_unix_ms as i64,
                segment.ended_at_unix_ms.map(|value| value as i64),
                segment.duration_ms as i64,
                segment.size_bytes as i64,
                encode_recording_state(segment.state),
            ],
        )?;
        Ok(())
    }

    fn list_segments(
        &self,
        camera_id: &CameraId,
        started_after_unix_ms: Option<u64>,
        started_before_unix_ms: Option<u64>,
    ) -> Result<Vec<RecordingSegment>> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let mut statement = connection.prepare(
            "
            SELECT camera_id, sequence, path, started_at_unix_ms, ended_at_unix_ms, duration_ms, size_bytes, state
            FROM recording_segments
            WHERE camera_id = ?1
              AND (?2 IS NULL OR started_at_unix_ms >= ?2)
              AND (?3 IS NULL OR started_at_unix_ms <= ?3)
            ORDER BY started_at_unix_ms ASC, sequence ASC
            ",
        )?;

        let rows = statement.query_map(
            params![
                camera_id.0,
                started_after_unix_ms.map(saturating_u64_to_i64),
                started_before_unix_ms.map(saturating_u64_to_i64),
            ],
            map_segment_row,
        )?;

        let mut segments = Vec::new();
        for segment in rows {
            segments.push(segment?);
        }
        Ok(segments)
    }

    fn storage_summary(&self, active_recordings: usize) -> Result<StorageSummaryResponse> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let totals = connection.query_row(
            "
            SELECT COUNT(*), COALESCE(SUM(size_bytes), 0)
            FROM recording_segments
            ",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;

        let mut statement = connection.prepare(
            "
            SELECT camera_id, COUNT(*), COALESCE(SUM(size_bytes), 0), MIN(started_at_unix_ms), MAX(started_at_unix_ms)
            FROM recording_segments
            GROUP BY camera_id
            ORDER BY camera_id ASC
            ",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(StorageCameraSummary {
                camera_id: row.get(0)?,
                segment_count: row.get::<_, i64>(1)? as usize,
                total_bytes: row.get::<_, i64>(2)? as u64,
                oldest_started_at_unix_ms: row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                newest_started_at_unix_ms: row.get::<_, Option<i64>>(4)?.map(|value| value as u64),
            })
        })?;

        let mut cameras = Vec::new();
        for row in rows {
            cameras.push(row?);
        }

        Ok(StorageSummaryResponse {
            total_segments: totals.0 as usize,
            total_bytes: totals.1 as u64,
            active_recordings,
            cameras,
        })
    }

    fn retention_preview(&self, retain_after_unix_ms: u64) -> Result<RetentionPreviewResponse> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let retain_after_sql = saturating_u64_to_i64(retain_after_unix_ms);
        let totals = connection.query_row(
            "
            SELECT COUNT(*), COALESCE(SUM(size_bytes), 0)
            FROM recording_segments
            WHERE started_at_unix_ms < ?1
            ",
            params![retain_after_sql],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;

        let mut statement = connection.prepare(
            "
            SELECT camera_id, COUNT(*), COALESCE(SUM(size_bytes), 0)
            FROM recording_segments
            WHERE started_at_unix_ms < ?1
            GROUP BY camera_id
            ORDER BY camera_id ASC
            ",
        )?;
        let rows = statement.query_map(params![retain_after_sql], |row| {
            Ok(RetentionPreviewCamera {
                camera_id: row.get(0)?,
                reclaimable_segments: row.get::<_, i64>(1)? as usize,
                reclaimable_bytes: row.get::<_, i64>(2)? as u64,
            })
        })?;

        let mut cameras = Vec::new();
        for row in rows {
            cameras.push(row?);
        }

        Ok(RetentionPreviewResponse {
            retain_after_unix_ms,
            reclaimable_segments: totals.0 as usize,
            reclaimable_bytes: totals.1 as u64,
            cameras,
        })
    }
}

fn saturating_u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

impl RecordingSessionStore for SqliteCameraStore {
    fn upsert_session(&self, session: &RecordingSession) -> Result<()> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        connection.execute(
            "
            INSERT INTO recording_sessions (
                camera_id,
                worker_name,
                output_directory,
                output_hint,
                state,
                pid,
                started_at_unix_ms,
                updated_at_unix_ms,
                last_error
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(camera_id) DO UPDATE SET
                worker_name = excluded.worker_name,
                output_directory = excluded.output_directory,
                output_hint = excluded.output_hint,
                state = excluded.state,
                pid = excluded.pid,
                started_at_unix_ms = excluded.started_at_unix_ms,
                updated_at_unix_ms = excluded.updated_at_unix_ms,
                last_error = excluded.last_error
            ",
            params![
                session.camera_id.0,
                session.worker_name,
                session.output_directory,
                session.output_hint,
                encode_recording_state(session.state),
                session.pid.map(i64::from),
                session.started_at_unix_ms as i64,
                session.updated_at_unix_ms as i64,
                session.last_error,
            ],
        )?;
        Ok(())
    }

    fn get_session(&self, camera_id: &CameraId) -> Result<Option<RecordingSession>> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let mut statement = connection.prepare(
            "
            SELECT camera_id, worker_name, output_directory, output_hint, state, pid, started_at_unix_ms, updated_at_unix_ms, last_error
            FROM recording_sessions
            WHERE camera_id = ?1
            ",
        )?;

        match statement.query_row(params![camera_id.0], map_session_row) {
            Ok(session) => Ok(Some(session)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn list_sessions(&self) -> Result<Vec<RecordingSession>> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let mut statement = connection.prepare(
            "
            SELECT camera_id, worker_name, output_directory, output_hint, state, pid, started_at_unix_ms, updated_at_unix_ms, last_error
            FROM recording_sessions
            ORDER BY camera_id ASC
            ",
        )?;

        let rows = statement.query_map([], map_session_row)?;
        let mut sessions = Vec::new();
        for session in rows {
            sessions.push(session?);
        }
        Ok(sessions)
    }

    fn delete_session(&self, camera_id: &CameraId) -> Result<()> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        connection.execute(
            "DELETE FROM recording_sessions WHERE camera_id = ?1",
            params![camera_id.0],
        )?;
        Ok(())
    }
}

fn map_camera_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Camera> {
    let streams_json: String = row.get(2)?;
    let source_json: String = row.get(3)?;
    let stream_profile_json: String = row.get(4)?;
    let streams: Vec<CameraStream> = serde_json::from_str(&streams_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;
    let source: RtspSource = serde_json::from_str(&source_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;
    let stream_profile: StreamProfile = serde_json::from_str(&stream_profile_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;

    Ok(Camera {
        id: CameraId(row.get(0)?),
        name: row.get(1)?,
        streams,
        source,
        stream_profile,
        enabled: row.get::<_, i64>(5)? != 0,
    })
}

fn map_segment_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecordingSegment> {
    let state: String = row.get(7)?;
    Ok(RecordingSegment {
        camera_id: CameraId(row.get(0)?),
        sequence: row.get::<_, i64>(1)? as u64,
        path: row.get(2)?,
        started_at_unix_ms: row.get::<_, i64>(3)? as u64,
        ended_at_unix_ms: row.get::<_, Option<i64>>(4)?.map(|value| value as u64),
        duration_ms: row.get::<_, i64>(5)? as u64,
        size_bytes: row.get::<_, i64>(6)? as u64,
        state: decode_recording_state(&state).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
    })
}

fn map_session_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecordingSession> {
    let state: String = row.get(4)?;
    Ok(RecordingSession {
        camera_id: CameraId(row.get(0)?),
        worker_name: row.get(1)?,
        output_directory: row.get(2)?,
        output_hint: row.get(3)?,
        state: decode_recording_state(&state).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        pid: row.get::<_, Option<i64>>(5)?.map(|value| value as u32),
        started_at_unix_ms: row.get::<_, i64>(6)? as u64,
        updated_at_unix_ms: row.get::<_, i64>(7)? as u64,
        last_error: row.get(8)?,
    })
}

fn encode_recording_state(state: RecordingState) -> &'static str {
    match state {
        RecordingState::Pending => "pending",
        RecordingState::Running => "running",
        RecordingState::Completed => "completed",
        RecordingState::Failed => "failed",
        RecordingState::Interrupted => "interrupted",
    }
}

fn decode_recording_state(value: &str) -> std::result::Result<RecordingState, std::io::Error> {
    match value {
        "pending" => Ok(RecordingState::Pending),
        "running" => Ok(RecordingState::Running),
        "completed" => Ok(RecordingState::Completed),
        "failed" => Ok(RecordingState::Failed),
        "interrupted" => Ok(RecordingState::Interrupted),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown recording state: {value}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harborlookout_domain::{CameraId, RtspTransport, StreamRole};

    fn sample_camera() -> Camera {
        Camera {
            id: CameraId("cam-1".into()),
            name: "Cam 1".into(),
            streams: vec![CameraStream {
                role: StreamRole::Record,
                source: RtspSource {
                    url: "rtsp://camera.local/1".into(),
                    transport: RtspTransport::Tcp,
                },
                stream_profile: StreamProfile {
                    width: 1280,
                    height: 720,
                    fps: 15,
                    segment_seconds: 10,
                },
            }],
            source: RtspSource {
                url: "rtsp://camera.local/1".into(),
                transport: RtspTransport::Tcp,
            },
            stream_profile: StreamProfile {
                width: 1280,
                height: 720,
                fps: 15,
                segment_seconds: 10,
            },
            enabled: true,
        }
    }

    #[test]
    fn stores_and_lists_cameras() {
        let store = SqliteCameraStore::open_in_memory().unwrap();
        let camera = sample_camera();

        store.upsert_camera(&camera).unwrap();
        let cameras = store.list_cameras().unwrap();
        assert_eq!(cameras.len(), 1);
        assert_eq!(cameras[0].id.0, "cam-1");
        assert_eq!(cameras[0].streams.len(), 1);
        assert!(store.get_camera(&CameraId("cam-1".into())).unwrap().is_some());
    }

    #[test]
    fn stores_and_filters_segments_by_time_range() {
        let store = SqliteCameraStore::open_in_memory().unwrap();

        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-1".into()),
                sequence: 1,
                path: "segments/cam-1-000001.mp4".into(),
                started_at_unix_ms: 1_000,
                ended_at_unix_ms: Some(11_000),
                duration_ms: 10_000,
                size_bytes: 1_024,
                state: RecordingState::Completed,
            })
            .unwrap();
        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-1".into()),
                sequence: 2,
                path: "segments/cam-1-000002.mp4".into(),
                started_at_unix_ms: 20_000,
                ended_at_unix_ms: None,
                duration_ms: 0,
                size_bytes: 0,
                state: RecordingState::Running,
            })
            .unwrap();
        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-2".into()),
                sequence: 1,
                path: "segments/cam-2-000001.mp4".into(),
                started_at_unix_ms: 30_000,
                ended_at_unix_ms: Some(40_000),
                duration_ms: 10_000,
                size_bytes: 2_048,
                state: RecordingState::Completed,
            })
            .unwrap();

        let segments = store
            .list_segments(&CameraId("cam-1".into()), Some(5_000), Some(25_000))
            .unwrap();

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].sequence, 2);
        assert_eq!(segments[0].state, RecordingState::Running);
    }

    #[test]
    fn stores_lists_and_deletes_sessions() {
        let store = SqliteCameraStore::open_in_memory().unwrap();
        let session = RecordingSession {
            camera_id: CameraId("cam-1".into()),
            worker_name: "worker-a".into(),
            output_directory: "data/segments/cam-1".into(),
            output_hint: Some("data/segments/cam-1/cam-1-%06d.mp4".into()),
            state: RecordingState::Running,
            pid: Some(1234),
            started_at_unix_ms: 100,
            updated_at_unix_ms: 200,
            last_error: None,
        };

        store.upsert_session(&session).unwrap();

        let loaded = store
            .get_session(&CameraId("cam-1".into()))
            .unwrap()
            .expect("session should exist");
        assert_eq!(loaded.worker_name, "worker-a");
        assert_eq!(loaded.state, RecordingState::Running);

        let sessions = store.list_sessions().unwrap();
        assert_eq!(sessions.len(), 1);

        store.delete_session(&CameraId("cam-1".into())).unwrap();
        assert!(store
            .get_session(&CameraId("cam-1".into()))
            .unwrap()
            .is_none());
    }

    #[test]
    fn computes_storage_summary_and_retention_preview() {
        let store = SqliteCameraStore::open_in_memory().unwrap();

        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-1".into()),
                sequence: 1,
                path: "segments/cam-1-000001.mp4".into(),
                started_at_unix_ms: 1_000,
                ended_at_unix_ms: Some(11_000),
                duration_ms: 10_000,
                size_bytes: 100,
                state: RecordingState::Completed,
            })
            .unwrap();
        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-1".into()),
                sequence: 2,
                path: "segments/cam-1-000002.mp4".into(),
                started_at_unix_ms: 20_000,
                ended_at_unix_ms: Some(30_000),
                duration_ms: 10_000,
                size_bytes: 150,
                state: RecordingState::Completed,
            })
            .unwrap();
        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-2".into()),
                sequence: 1,
                path: "segments/cam-2-000001.mp4".into(),
                started_at_unix_ms: 40_000,
                ended_at_unix_ms: None,
                duration_ms: 10_000,
                size_bytes: 200,
                state: RecordingState::Running,
            })
            .unwrap();

        let summary = store.storage_summary(1).unwrap();
        assert_eq!(summary.total_segments, 3);
        assert_eq!(summary.total_bytes, 450);
        assert_eq!(summary.active_recordings, 1);
        assert_eq!(summary.cameras.len(), 2);

        let preview = store.retention_preview(25_000).unwrap();
        assert_eq!(preview.reclaimable_segments, 2);
        assert_eq!(preview.reclaimable_bytes, 250);
        assert_eq!(preview.cameras.len(), 1);
        assert_eq!(preview.cameras[0].camera_id, "cam-1");

        let max_preview = store.retention_preview(u64::MAX).unwrap();
        assert_eq!(max_preview.reclaimable_segments, 3);
        assert_eq!(max_preview.reclaimable_bytes, 450);
        assert_eq!(max_preview.cameras.len(), 2);
    }
}