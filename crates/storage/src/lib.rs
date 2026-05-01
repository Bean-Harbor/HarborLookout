use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;
use harborlookout_domain::{
    ArtifactId, ArtifactKind, Camera, CameraId, CameraStream, EventArtifact, EventId, EventKind,
    EventSeverity, RecordingSegment, RecordingSession, RecordingState, RtspSource, StreamProfile,
    SurveillanceEvent,
};
use harborlookout_contracts::{
    ArtifactKindRetentionRule, ArtifactRetentionCleanupCamera, ArtifactRetentionCleanupRequest,
    ArtifactRetentionCleanupResponse, ArtifactRetentionPreviewCamera, ArtifactRetentionPreviewRequest,
    ArtifactRetentionPreviewResponse, RetentionCleanupCamera, RetentionCleanupResponse,
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
    fn retention_cleanup(&self, retain_after_unix_ms: u64) -> Result<RetentionCleanupResponse>;
}

pub trait RecordingSessionStore: Send + Sync {
    fn upsert_session(&self, session: &RecordingSession) -> Result<()>;
    fn get_session(&self, camera_id: &CameraId) -> Result<Option<RecordingSession>>;
    fn list_sessions(&self) -> Result<Vec<RecordingSession>>;
    fn delete_session(&self, camera_id: &CameraId) -> Result<()>;
}

pub trait SurveillanceEventStore: Send + Sync {
    fn upsert_event(&self, event: &SurveillanceEvent) -> Result<()>;
    fn get_event(&self, event_id: &EventId) -> Result<Option<SurveillanceEvent>>;
    fn list_events(
        &self,
        camera_id: Option<&CameraId>,
        occurred_after_unix_ms: Option<u64>,
        occurred_before_unix_ms: Option<u64>,
    ) -> Result<Vec<SurveillanceEvent>>;
}

pub trait EventArtifactStore: Send + Sync {
    fn upsert_event_artifact(&self, artifact: &EventArtifact) -> Result<()>;
    fn list_event_artifacts(&self, event_id: &EventId) -> Result<Vec<EventArtifact>>;
    fn artifact_retention_preview(
        &self,
        request: &ArtifactRetentionPreviewRequest,
    ) -> Result<ArtifactRetentionPreviewResponse>;
    fn artifact_retention_cleanup(
        &self,
        request: &ArtifactRetentionCleanupRequest,
    ) -> Result<ArtifactRetentionCleanupResponse>;
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

            CREATE TABLE IF NOT EXISTS surveillance_events (
                id TEXT PRIMARY KEY,
                camera_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                severity TEXT NOT NULL,
                occurred_at_unix_ms INTEGER NOT NULL,
                message TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event_artifacts (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                camera_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                path TEXT NOT NULL,
                mime_type TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                started_at_unix_ms INTEGER,
                ended_at_unix_ms INTEGER,
                size_bytes INTEGER NOT NULL
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

    fn retention_cleanup(&self, retain_after_unix_ms: u64) -> Result<RetentionCleanupResponse> {
        let retain_after_sql = saturating_u64_to_i64(retain_after_unix_ms);
        let eligible_segments = {
            let connection = self.connection.lock().expect("sqlite connection poisoned");
            let mut statement = connection.prepare(
                "
                SELECT camera_id, sequence, path, started_at_unix_ms, ended_at_unix_ms, duration_ms, size_bytes, state
                FROM recording_segments
                WHERE started_at_unix_ms < ?1
                  AND state != 'running'
                ORDER BY camera_id ASC, sequence ASC
                ",
            )?;
            let rows = statement.query_map(params![retain_after_sql], map_segment_row)?;

            let mut segments = Vec::new();
            for segment in rows {
                segments.push(segment?);
            }
            segments
        };

        let attempted_segments = eligible_segments.len();
        let mut deleted_segments = 0usize;
        let mut deleted_bytes = 0u64;
        let mut skipped_segments = 0usize;
        let mut cameras: BTreeMap<String, (usize, u64)> = BTreeMap::new();

        for segment in eligible_segments {
            match std::fs::remove_file(&segment.path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    skipped_segments += 1;
                    continue;
                }
            }

            let connection = self.connection.lock().expect("sqlite connection poisoned");
            connection.execute(
                "DELETE FROM recording_segments WHERE camera_id = ?1 AND sequence = ?2",
                params![segment.camera_id.0, segment.sequence as i64],
            )?;
            drop(connection);

            deleted_segments += 1;
            deleted_bytes = deleted_bytes.saturating_add(segment.size_bytes);
            let entry = cameras
                .entry(segment.camera_id.0)
                .or_insert((0usize, 0u64));
            entry.0 += 1;
            entry.1 = entry.1.saturating_add(segment.size_bytes);
        }

        Ok(RetentionCleanupResponse {
            retain_after_unix_ms,
            attempted_segments,
            deleted_segments,
            deleted_bytes,
            skipped_segments,
            cameras: cameras
                .into_iter()
                .map(|(camera_id, (deleted_segments, deleted_bytes))| RetentionCleanupCamera {
                    camera_id,
                    deleted_segments,
                    deleted_bytes,
                })
                .collect(),
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

impl SurveillanceEventStore for SqliteCameraStore {
    fn upsert_event(&self, event: &SurveillanceEvent) -> Result<()> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        connection.execute(
            "
            INSERT INTO surveillance_events (id, camera_id, kind, severity, occurred_at_unix_ms, message)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(id) DO UPDATE SET
                camera_id = excluded.camera_id,
                kind = excluded.kind,
                severity = excluded.severity,
                occurred_at_unix_ms = excluded.occurred_at_unix_ms,
                message = excluded.message
            ",
            params![
                event.id.0,
                event.camera_id.0,
                encode_event_kind(&event.kind),
                encode_event_severity(&event.severity),
                event.occurred_at_unix_ms as i64,
                event.message,
            ],
        )?;
        Ok(())
    }

    fn get_event(&self, event_id: &EventId) -> Result<Option<SurveillanceEvent>> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let mut statement = connection.prepare(
            "
            SELECT id, camera_id, kind, severity, occurred_at_unix_ms, message
            FROM surveillance_events
            WHERE id = ?1
            ",
        )?;

        match statement.query_row(params![event_id.0], map_event_row) {
            Ok(event) => Ok(Some(event)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn list_events(
        &self,
        camera_id: Option<&CameraId>,
        occurred_after_unix_ms: Option<u64>,
        occurred_before_unix_ms: Option<u64>,
    ) -> Result<Vec<SurveillanceEvent>> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let mut statement = connection.prepare(
            "
            SELECT id, camera_id, kind, severity, occurred_at_unix_ms, message
            FROM surveillance_events
            WHERE (?1 IS NULL OR camera_id = ?1)
              AND (?2 IS NULL OR occurred_at_unix_ms >= ?2)
              AND (?3 IS NULL OR occurred_at_unix_ms <= ?3)
            ORDER BY occurred_at_unix_ms ASC, id ASC
            ",
        )?;

        let rows = statement.query_map(
            params![
                camera_id.map(|value| value.0.as_str()),
                occurred_after_unix_ms.map(saturating_u64_to_i64),
                occurred_before_unix_ms.map(saturating_u64_to_i64),
            ],
            map_event_row,
        )?;

        let mut events = Vec::new();
        for event in rows {
            events.push(event?);
        }
        Ok(events)
    }
}

impl EventArtifactStore for SqliteCameraStore {
    fn upsert_event_artifact(&self, artifact: &EventArtifact) -> Result<()> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        connection.execute(
            "
            INSERT INTO event_artifacts (
                id,
                event_id,
                camera_id,
                kind,
                path,
                mime_type,
                created_at_unix_ms,
                started_at_unix_ms,
                ended_at_unix_ms,
                size_bytes
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(id) DO UPDATE SET
                event_id = excluded.event_id,
                camera_id = excluded.camera_id,
                kind = excluded.kind,
                path = excluded.path,
                mime_type = excluded.mime_type,
                created_at_unix_ms = excluded.created_at_unix_ms,
                started_at_unix_ms = excluded.started_at_unix_ms,
                ended_at_unix_ms = excluded.ended_at_unix_ms,
                size_bytes = excluded.size_bytes
            ",
            params![
                artifact.id.0,
                artifact.event_id.0,
                artifact.camera_id.0,
                encode_artifact_kind(&artifact.kind),
                artifact.path,
                artifact.mime_type,
                artifact.created_at_unix_ms as i64,
                artifact.started_at_unix_ms.map(saturating_u64_to_i64),
                artifact.ended_at_unix_ms.map(saturating_u64_to_i64),
                artifact.size_bytes as i64,
            ],
        )?;
        Ok(())
    }

    fn list_event_artifacts(&self, event_id: &EventId) -> Result<Vec<EventArtifact>> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let mut statement = connection.prepare(
            "
            SELECT id, event_id, camera_id, kind, path, mime_type, created_at_unix_ms, started_at_unix_ms, ended_at_unix_ms, size_bytes
            FROM event_artifacts
            WHERE event_id = ?1
            ORDER BY created_at_unix_ms ASC, id ASC
            ",
        )?;

        let rows = statement.query_map(params![event_id.0], map_event_artifact_row)?;
        let mut artifacts = Vec::new();
        for artifact in rows {
            artifacts.push(artifact?);
        }
        Ok(artifacts)
    }

    fn artifact_retention_preview(&self, request: &ArtifactRetentionPreviewRequest) -> Result<ArtifactRetentionPreviewResponse> {
        let candidates = self.list_artifact_retention_candidates()?;
        let rules = build_kind_rule_map(request.retain_after_unix_ms, &request.retain_after_by_kind);

        let mut reclaimable_artifacts = 0usize;
        let mut reclaimable_bytes = 0u64;
        let mut protected_artifacts = 0usize;
        let mut cameras: BTreeMap<String, (usize, u64)> = BTreeMap::new();

        for candidate in candidates {
            if !is_artifact_old_enough(candidate.artifact.created_at_unix_ms, &candidate.artifact.kind, &rules) {
                continue;
            }
            if is_artifact_protected(candidate.event_occurred_at_unix_ms, request.protect_events_occurred_after_unix_ms)
            {
                protected_artifacts += 1;
                continue;
            }

            reclaimable_artifacts += 1;
            reclaimable_bytes = reclaimable_bytes.saturating_add(candidate.artifact.size_bytes);
            let entry = cameras
                .entry(candidate.artifact.camera_id.0)
                .or_insert((0usize, 0u64));
            entry.0 += 1;
            entry.1 = entry.1.saturating_add(candidate.artifact.size_bytes);
        }

        Ok(ArtifactRetentionPreviewResponse {
            retain_after_unix_ms: request.retain_after_unix_ms,
            reclaimable_artifacts,
            reclaimable_bytes,
            protected_artifacts,
            cameras: cameras
                .into_iter()
                .map(
                    |(camera_id, (reclaimable_artifacts, reclaimable_bytes))| ArtifactRetentionPreviewCamera {
                        camera_id,
                        reclaimable_artifacts,
                        reclaimable_bytes,
                    },
                )
                .collect(),
        })
    }

    fn artifact_retention_cleanup(&self, request: &ArtifactRetentionCleanupRequest) -> Result<ArtifactRetentionCleanupResponse> {
        let candidates = self.list_artifact_retention_candidates()?;
        let rules = build_kind_rule_map(request.retain_after_unix_ms, &request.retain_after_by_kind);

        let mut deletable_artifacts = Vec::new();
        let mut protected_artifacts = 0usize;
        for candidate in candidates {
            if !is_artifact_old_enough(candidate.artifact.created_at_unix_ms, &candidate.artifact.kind, &rules) {
                continue;
            }
            if is_artifact_protected(candidate.event_occurred_at_unix_ms, request.protect_events_occurred_after_unix_ms)
            {
                protected_artifacts += 1;
                continue;
            }
            deletable_artifacts.push(candidate.artifact);
        }

        let attempted_artifacts = deletable_artifacts.len();
        let mut deleted_artifacts = 0usize;
        let mut deleted_bytes = 0u64;
        let mut skipped_artifacts = 0usize;
        let mut cameras: BTreeMap<String, (usize, u64)> = BTreeMap::new();

        for artifact in deletable_artifacts {
            match std::fs::remove_file(&artifact.path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    skipped_artifacts += 1;
                    continue;
                }
            }

            let connection = self.connection.lock().expect("sqlite connection poisoned");
            connection.execute(
                "DELETE FROM event_artifacts WHERE id = ?1",
                params![artifact.id.0],
            )?;
            drop(connection);

            deleted_artifacts += 1;
            deleted_bytes = deleted_bytes.saturating_add(artifact.size_bytes);
            let entry = cameras
                .entry(artifact.camera_id.0)
                .or_insert((0usize, 0u64));
            entry.0 += 1;
            entry.1 = entry.1.saturating_add(artifact.size_bytes);
        }

        Ok(ArtifactRetentionCleanupResponse {
            retain_after_unix_ms: request.retain_after_unix_ms,
            attempted_artifacts,
            deleted_artifacts,
            deleted_bytes,
            protected_artifacts,
            skipped_artifacts,
            cameras: cameras
                .into_iter()
                .map(|(camera_id, (deleted_artifacts, deleted_bytes))| ArtifactRetentionCleanupCamera {
                    camera_id,
                    deleted_artifacts,
                    deleted_bytes,
                })
                .collect(),
        })
    }
}

struct ArtifactRetentionCandidate {
    artifact: EventArtifact,
    event_occurred_at_unix_ms: Option<u64>,
}

impl SqliteCameraStore {
    fn list_artifact_retention_candidates(&self) -> Result<Vec<ArtifactRetentionCandidate>> {
        let connection = self.connection.lock().expect("sqlite connection poisoned");
        let mut statement = connection.prepare(
            "
            SELECT
                a.id,
                a.event_id,
                a.camera_id,
                a.kind,
                a.path,
                a.mime_type,
                a.created_at_unix_ms,
                a.started_at_unix_ms,
                a.ended_at_unix_ms,
                a.size_bytes,
                e.occurred_at_unix_ms
            FROM event_artifacts a
            LEFT JOIN surveillance_events e ON e.id = a.event_id
            ORDER BY a.camera_id ASC, a.created_at_unix_ms ASC, a.id ASC
            ",
        )?;

        let rows = statement.query_map([], |row| {
            let artifact = map_event_artifact_row(row)?;
            let event_occurred_at_unix_ms = row.get::<_, Option<i64>>(10)?.map(|value| value as u64);
            Ok(ArtifactRetentionCandidate {
                artifact,
                event_occurred_at_unix_ms,
            })
        })?;

        let mut candidates = Vec::new();
        for candidate in rows {
            candidates.push(candidate?);
        }
        Ok(candidates)
    }
}

fn build_kind_rule_map(
    default_retain_after_unix_ms: u64,
    rules: &[ArtifactKindRetentionRule],
) -> BTreeMap<ArtifactKind, u64> {
    let mut map = BTreeMap::new();
    map.insert(ArtifactKind::Keyframe, default_retain_after_unix_ms);
    map.insert(ArtifactKind::Clip, default_retain_after_unix_ms);
    map.insert(ArtifactKind::Thumbnail, default_retain_after_unix_ms);
    map.insert(ArtifactKind::Metadata, default_retain_after_unix_ms);

    for rule in rules {
        map.insert(rule.kind.clone(), rule.retain_after_unix_ms);
    }
    map
}

fn is_artifact_old_enough(
    created_at_unix_ms: u64,
    kind: &ArtifactKind,
    rules: &BTreeMap<ArtifactKind, u64>,
) -> bool {
    let retain_after_unix_ms = rules.get(kind).copied().unwrap_or(u64::MAX);
    created_at_unix_ms < retain_after_unix_ms
}

fn is_artifact_protected(
    event_occurred_at_unix_ms: Option<u64>,
    protect_events_occurred_after_unix_ms: Option<u64>,
) -> bool {
    match (event_occurred_at_unix_ms, protect_events_occurred_after_unix_ms) {
        (Some(event_occurred), Some(cutoff)) => event_occurred >= cutoff,
        _ => false,
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

fn map_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SurveillanceEvent> {
    let kind: String = row.get(2)?;
    let severity: String = row.get(3)?;

    Ok(SurveillanceEvent {
        id: EventId(row.get(0)?),
        camera_id: CameraId(row.get(1)?),
        kind: decode_event_kind(&kind).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        severity: decode_event_severity(&severity).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        occurred_at_unix_ms: row.get::<_, i64>(4)? as u64,
        message: row.get(5)?,
    })
}

fn map_event_artifact_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventArtifact> {
    let kind: String = row.get(3)?;

    Ok(EventArtifact {
        id: ArtifactId(row.get(0)?),
        event_id: EventId(row.get(1)?),
        camera_id: CameraId(row.get(2)?),
        kind: decode_artifact_kind(&kind).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        path: row.get(4)?,
        mime_type: row.get(5)?,
        created_at_unix_ms: row.get::<_, i64>(6)? as u64,
        started_at_unix_ms: row.get::<_, Option<i64>>(7)?.map(|value| value as u64),
        ended_at_unix_ms: row.get::<_, Option<i64>>(8)?.map(|value| value as u64),
        size_bytes: row.get::<_, i64>(9)? as u64,
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

    #[test]
    fn executes_retention_cleanup_without_touching_running_segments() {
        let temp_dir = std::env::temp_dir().join(format!("harborlookout-retention-cleanup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let store = SqliteCameraStore::open_in_memory().unwrap();
        let completed_path = temp_dir.join("cam-1-000001.mp4");
        let missing_path = temp_dir.join("cam-1-000002.mp4");
        let running_path = temp_dir.join("cam-2-000001.mp4");
        std::fs::write(&completed_path, b"completed").unwrap();
        std::fs::write(&running_path, b"running").unwrap();

        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-1".into()),
                sequence: 1,
                path: completed_path.to_string_lossy().to_string(),
                started_at_unix_ms: 1_000,
                ended_at_unix_ms: Some(2_000),
                duration_ms: 1_000,
                size_bytes: 100,
                state: RecordingState::Completed,
            })
            .unwrap();
        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-1".into()),
                sequence: 2,
                path: missing_path.to_string_lossy().to_string(),
                started_at_unix_ms: 1_500,
                ended_at_unix_ms: Some(2_500),
                duration_ms: 1_000,
                size_bytes: 150,
                state: RecordingState::Interrupted,
            })
            .unwrap();
        store
            .upsert_segment(&RecordingSegment {
                camera_id: CameraId("cam-2".into()),
                sequence: 1,
                path: running_path.to_string_lossy().to_string(),
                started_at_unix_ms: 1_200,
                ended_at_unix_ms: None,
                duration_ms: 0,
                size_bytes: 200,
                state: RecordingState::Running,
            })
            .unwrap();

        let cleanup = store.retention_cleanup(10_000).unwrap();
        assert_eq!(cleanup.attempted_segments, 2);
        assert_eq!(cleanup.deleted_segments, 2);
        assert_eq!(cleanup.deleted_bytes, 250);
        assert_eq!(cleanup.skipped_segments, 0);
        assert_eq!(cleanup.cameras.len(), 1);
        assert!(!completed_path.exists());
        assert!(running_path.exists());

        let summary = store.storage_summary(1).unwrap();
        assert_eq!(summary.total_segments, 1);
        assert_eq!(summary.total_bytes, 200);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn stores_events_and_event_artifacts() {
        let store = SqliteCameraStore::open_in_memory().unwrap();

        let event = SurveillanceEvent {
            id: EventId("event-1".into()),
            camera_id: CameraId("cam-1".into()),
            kind: EventKind::PackageDetected,
            severity: EventSeverity::Warning,
            occurred_at_unix_ms: 15_000,
            message: "package detected at front porch".into(),
        };
        let artifact = EventArtifact {
            id: ArtifactId("artifact-1".into()),
            event_id: event.id.clone(),
            camera_id: event.camera_id.clone(),
            kind: ArtifactKind::Keyframe,
            path: "artifacts/event-1/frame-001.jpg".into(),
            mime_type: Some("image/jpeg".into()),
            created_at_unix_ms: 15_100,
            started_at_unix_ms: Some(15_000),
            ended_at_unix_ms: Some(15_100),
            size_bytes: 4_096,
        };

        store.upsert_event(&event).unwrap();
        store.upsert_event_artifact(&artifact).unwrap();

        let loaded = store.get_event(&EventId("event-1".into())).unwrap().unwrap();
    assert_eq!(loaded.kind, EventKind::PackageDetected);
        assert_eq!(loaded.severity, EventSeverity::Warning);

        let events = store
            .list_events(Some(&CameraId("cam-1".into())), Some(10_000), Some(20_000))
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, "package detected at front porch");

        let artifacts = store.list_event_artifacts(&EventId("event-1".into())).unwrap();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].kind, ArtifactKind::Keyframe);
        assert_eq!(artifacts[0].mime_type.as_deref(), Some("image/jpeg"));
    }

    #[test]
    fn executes_artifact_retention_cleanup() {
        let temp_dir = std::env::temp_dir().join(format!(
            "harborlookout-artifact-retention-cleanup-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let store = SqliteCameraStore::open_in_memory().unwrap();
        let old_artifact_path = temp_dir.join("artifact-old.jpg");
        let new_artifact_path = temp_dir.join("artifact-new.jpg");
        std::fs::write(&old_artifact_path, b"old").unwrap();
        std::fs::write(&new_artifact_path, b"new").unwrap();

        store
            .upsert_event_artifact(&EventArtifact {
                id: ArtifactId("artifact-old".into()),
                event_id: EventId("event-1".into()),
                camera_id: CameraId("cam-1".into()),
                kind: ArtifactKind::Keyframe,
                path: old_artifact_path.to_string_lossy().to_string(),
                mime_type: Some("image/jpeg".into()),
                created_at_unix_ms: 1_000,
                started_at_unix_ms: Some(1_000),
                ended_at_unix_ms: Some(1_050),
                size_bytes: 3,
            })
            .unwrap();
        store
            .upsert_event_artifact(&EventArtifact {
                id: ArtifactId("artifact-new".into()),
                event_id: EventId("event-2".into()),
                camera_id: CameraId("cam-1".into()),
                kind: ArtifactKind::Keyframe,
                path: new_artifact_path.to_string_lossy().to_string(),
                mime_type: Some("image/jpeg".into()),
                created_at_unix_ms: 20_000,
                started_at_unix_ms: Some(20_000),
                ended_at_unix_ms: Some(20_050),
                size_bytes: 3,
            })
            .unwrap();

        let preview = store
            .artifact_retention_preview(&ArtifactRetentionPreviewRequest {
                retain_after_unix_ms: 10_000,
                retain_after_by_kind: Vec::new(),
                protect_events_occurred_after_unix_ms: None,
            })
            .unwrap();
        assert_eq!(preview.reclaimable_artifacts, 1);
        assert_eq!(preview.reclaimable_bytes, 3);
        assert_eq!(preview.protected_artifacts, 0);

        let cleanup = store
            .artifact_retention_cleanup(&ArtifactRetentionCleanupRequest {
                retain_after_unix_ms: 10_000,
                retain_after_by_kind: Vec::new(),
                protect_events_occurred_after_unix_ms: None,
            })
            .unwrap();
        assert_eq!(cleanup.attempted_artifacts, 1);
        assert_eq!(cleanup.deleted_artifacts, 1);
        assert_eq!(cleanup.deleted_bytes, 3);
        assert_eq!(cleanup.protected_artifacts, 0);
        assert_eq!(cleanup.skipped_artifacts, 0);
        assert_eq!(cleanup.cameras.len(), 1);
        assert!(!old_artifact_path.exists());
        assert!(new_artifact_path.exists());

        let remaining = store.list_event_artifacts(&EventId("event-2".into())).unwrap();
        assert_eq!(remaining.len(), 1);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn artifact_retention_v2_respects_kind_rules_and_event_protection() {
        let temp_dir = std::env::temp_dir().join(format!(
            "harborlookout-artifact-retention-v2-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let store = SqliteCameraStore::open_in_memory().unwrap();

        let protected_path = temp_dir.join("protected-keyframe.jpg");
        let deletable_path = temp_dir.join("deletable-keyframe.jpg");
        let clip_path = temp_dir.join("clip-kept.mp4");
        std::fs::write(&protected_path, b"p").unwrap();
        std::fs::write(&deletable_path, b"d").unwrap();
        std::fs::write(&clip_path, b"c").unwrap();

        store
            .upsert_event(&SurveillanceEvent {
                id: EventId("event-protected".into()),
                camera_id: CameraId("cam-v2".into()),
                kind: EventKind::PersonDetected,
                severity: EventSeverity::Warning,
                occurred_at_unix_ms: 8_000,
                message: "protected".into(),
            })
            .unwrap();
        store
            .upsert_event(&SurveillanceEvent {
                id: EventId("event-old".into()),
                camera_id: CameraId("cam-v2".into()),
                kind: EventKind::PersonDetected,
                severity: EventSeverity::Warning,
                occurred_at_unix_ms: 1_000,
                message: "old".into(),
            })
            .unwrap();

        store
            .upsert_event_artifact(&EventArtifact {
                id: ArtifactId("artifact-protected".into()),
                event_id: EventId("event-protected".into()),
                camera_id: CameraId("cam-v2".into()),
                kind: ArtifactKind::Keyframe,
                path: protected_path.to_string_lossy().to_string(),
                mime_type: Some("image/jpeg".into()),
                created_at_unix_ms: 500,
                started_at_unix_ms: Some(500),
                ended_at_unix_ms: Some(550),
                size_bytes: 1,
            })
            .unwrap();
        store
            .upsert_event_artifact(&EventArtifact {
                id: ArtifactId("artifact-delete".into()),
                event_id: EventId("event-old".into()),
                camera_id: CameraId("cam-v2".into()),
                kind: ArtifactKind::Keyframe,
                path: deletable_path.to_string_lossy().to_string(),
                mime_type: Some("image/jpeg".into()),
                created_at_unix_ms: 400,
                started_at_unix_ms: Some(400),
                ended_at_unix_ms: Some(450),
                size_bytes: 1,
            })
            .unwrap();
        store
            .upsert_event_artifact(&EventArtifact {
                id: ArtifactId("artifact-clip".into()),
                event_id: EventId("event-old".into()),
                camera_id: CameraId("cam-v2".into()),
                kind: ArtifactKind::Clip,
                path: clip_path.to_string_lossy().to_string(),
                mime_type: Some("video/mp4".into()),
                created_at_unix_ms: 200,
                started_at_unix_ms: Some(200),
                ended_at_unix_ms: Some(260),
                size_bytes: 1,
            })
            .unwrap();

        let request = ArtifactRetentionCleanupRequest {
            retain_after_unix_ms: 5_000,
            retain_after_by_kind: vec![ArtifactKindRetentionRule {
                kind: ArtifactKind::Clip,
                retain_after_unix_ms: 100,
            }],
            protect_events_occurred_after_unix_ms: Some(7_000),
        };

        let preview = store
            .artifact_retention_preview(&ArtifactRetentionPreviewRequest {
                retain_after_unix_ms: request.retain_after_unix_ms,
                retain_after_by_kind: request.retain_after_by_kind.clone(),
                protect_events_occurred_after_unix_ms: request.protect_events_occurred_after_unix_ms,
            })
            .unwrap();
        assert_eq!(preview.reclaimable_artifacts, 1);
        assert_eq!(preview.protected_artifacts, 1);

        let cleanup = store.artifact_retention_cleanup(&request).unwrap();
        assert_eq!(cleanup.attempted_artifacts, 1);
        assert_eq!(cleanup.deleted_artifacts, 1);
        assert_eq!(cleanup.protected_artifacts, 1);
        assert!(protected_path.exists());
        assert!(!deletable_path.exists());
        assert!(clip_path.exists());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}

fn encode_event_kind(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::MotionDetected => "motion_detected",
        EventKind::MotionInZone => "motion_in_zone",
        EventKind::PersonDetected => "person_detected",
        EventKind::VehicleDetected => "vehicle_detected",
        EventKind::PetDetected => "pet_detected",
        EventKind::PackageDetected => "package_detected",
        EventKind::DrinkContainerDetected => "drink_container_detected",
        EventKind::ObjectRemoved => "object_removed",
        EventKind::SpillCandidateDetected => "spill_candidate_detected",
        EventKind::RecordingStarted => "recording_started",
        EventKind::RecordingStopped => "recording_stopped",
        EventKind::CameraOffline => "camera_offline",
        EventKind::CameraOnline => "camera_online",
    }
}

fn decode_event_kind(value: &str) -> std::result::Result<EventKind, std::io::Error> {
    match value {
        "motion_detected" => Ok(EventKind::MotionDetected),
        "motion_in_zone" => Ok(EventKind::MotionInZone),
        "person_detected" => Ok(EventKind::PersonDetected),
        "vehicle_detected" => Ok(EventKind::VehicleDetected),
        "pet_detected" => Ok(EventKind::PetDetected),
        "package_detected" => Ok(EventKind::PackageDetected),
        "drink_container_detected" => Ok(EventKind::DrinkContainerDetected),
        "object_removed" => Ok(EventKind::ObjectRemoved),
        "spill_candidate_detected" => Ok(EventKind::SpillCandidateDetected),
        "recording_started" => Ok(EventKind::RecordingStarted),
        "recording_stopped" => Ok(EventKind::RecordingStopped),
        "camera_offline" => Ok(EventKind::CameraOffline),
        "camera_online" => Ok(EventKind::CameraOnline),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown event kind: {value}"),
        )),
    }
}

fn encode_event_severity(severity: &EventSeverity) -> &'static str {
    match severity {
        EventSeverity::Info => "info",
        EventSeverity::Warning => "warning",
        EventSeverity::Critical => "critical",
    }
}

fn decode_event_severity(value: &str) -> std::result::Result<EventSeverity, std::io::Error> {
    match value {
        "info" => Ok(EventSeverity::Info),
        "warning" => Ok(EventSeverity::Warning),
        "critical" => Ok(EventSeverity::Critical),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown event severity: {value}"),
        )),
    }
}

fn encode_artifact_kind(kind: &ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Keyframe => "keyframe",
        ArtifactKind::Clip => "clip",
        ArtifactKind::Thumbnail => "thumbnail",
        ArtifactKind::Metadata => "metadata",
    }
}

fn decode_artifact_kind(value: &str) -> std::result::Result<ArtifactKind, std::io::Error> {
    match value {
        "keyframe" => Ok(ArtifactKind::Keyframe),
        "clip" => Ok(ArtifactKind::Clip),
        "thumbnail" => Ok(ArtifactKind::Thumbnail),
        "metadata" => Ok(ArtifactKind::Metadata),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown artifact kind: {value}"),
        )),
    }
}