use crate::{
    recording::{Config, FrameRef, RecordedBatch},
    runtime::Shared,
    source::SessionInfo,
};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection};

#[derive(Default)]
struct SampleTimestamp {
    second: Option<i64>,
    prefix: String,
    text: String,
}
impl SampleTimestamp {
    fn format(&mut self, timestamp: chrono::DateTime<chrono::Utc>) -> &str {
        use std::fmt::Write;
        // Keep Chrono's extended-year and leap-second representation unchanged.
        if timestamp.timestamp_subsec_nanos() >= 1_000_000_000 {
            self.text = timestamp.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
            return &self.text;
        }
        if self.second != Some(timestamp.timestamp()) {
            self.prefix = timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            self.prefix.pop(); // UTC suffix; append microseconds before it below.
            self.second = Some(timestamp.timestamp());
        }
        self.text.clear();
        self.text.push_str(&self.prefix);
        write!(self.text, ".{:06}Z", timestamp.timestamp_subsec_micros()).unwrap();
        &self.text
    }
}

pub struct Store {
    conn: Connection,
    session_id: i64,
    device: String,
    usb: bool,
    insert_sql: std::collections::HashMap<(bool, usize), String>,
    #[cfg(test)]
    sequence: u64,
}
impl Store {
    pub fn session_id(&self) -> i64 {
        self.session_id
    }
    #[cfg(test)]
    pub fn open(path: &str, info: &SessionInfo) -> Result<Self> {
        Self::open_config(path, info, Config::default())
    }
    pub fn open_config(path: &str, info: &SessionInfo, config: Config) -> Result<Self> {
        let mut conn = open_database(path)?;
        let tx = conn.transaction()?;
        tx.execute("INSERT INTO sessions(device_id,started_at,transport,calibration,calibration_received_at,timestamp_basis,outcome,name) VALUES (?1,?2,?3,?4,?2,?5,'incomplete',?6)",params![info.device,info.received.to_rfc3339(),info.transport,info.calibration,if info.transport=="usb-cc" {"estimated: first packet reception, 100 us/sample, packet counter"} else {"source timestamp"},info.name.clone().unwrap_or_default()])?;
        let session_id = tx.last_insert_rowid();
        tx.execute(
            "UPDATE sessions SET sample_rate_hz=?2,buffer_size_bytes=?3 WHERE id=?1",
            params![
                session_id,
                config.sample_rate_hz,
                config.buffer_size_bytes as u64
            ],
        )?;
        tx.commit()?;
        Ok(Self {
            conn,
            session_id,
            device: info.device.clone(),
            usb: info.transport == "usb-cc",
            insert_sql: std::collections::HashMap::new(),
            #[cfg(test)]
            sequence: 0,
        })
    }
    #[cfg(test)]
    pub fn insert_batches(&mut self, batches: &[crate::source::Batch]) -> Result<u64> {
        let mut sampler = crate::recording::Resampler::with_sequence(self.sequence);
        let recorded: Vec<_> = batches.iter().map(|b| sampler.process(b.clone())).collect();
        self.sequence += batches.len() as u64;
        self.insert_recorded(&recorded).map(|n| n.0)
    }
    pub fn insert_recorded(&mut self, batches: &[RecordedBatch]) -> Result<(u64, u64)> {
        let tx = self
            .conn
            .transaction()
            .context("begin measurement transaction")?;
        let mut count = 0;
        let mut represented = 0;
        let mut timestamp = SampleTimestamp::default();
        let mut timestamps: Vec<String> = (0..64).map(|_| String::with_capacity(32)).collect();
        let mut gaps = 0;
        let mut invalid = 0;
        for batch in batches {
            let frame_id = if let Some(f) = &batch.frame {
                tx.execute("INSERT INTO frames(session_id,packet_id,received_at,raw,missing_before,invalid_samples,capture_sequence) VALUES (?1,?2,?3,?4,?5,?6,?7)",params![self.session_id,f.packet_id,f.received.to_rfc3339(),f.raw,f.gaps,f.invalid,f.sequence])?;
                gaps += f.gaps;
                invalid += f.invalid;
                Some(tx.last_insert_rowid())
            } else {
                None
            };
            let resolve = |reference: Option<FrameRef>| -> Result<Option<i64>> {
                match reference {
                    None => Ok(None),
                    Some(r)
                        if batch
                            .frame
                            .as_ref()
                            .is_some_and(|f| f.sequence == r.sequence) =>
                    {
                        Ok(frame_id)
                    }
                    Some(r) => Ok(Some(tx.query_row(
                        "SELECT id FROM frames WHERE session_id=?1 AND capture_sequence=?2",
                        params![self.session_id, r.sequence],
                        |row| row.get(0),
                    )?)),
                }
            };
            for chunk in batch.records.chunks(64) {
                let aggregate = chunk[0].aggregate.is_some();
                let first = &chunk[0].measurement;
                let native = !aggregate
                    && frame_id.is_some()
                    && chunk.iter().all(|r| {
                        r.aggregate.is_none()
                            && r.first.is_some_and(|reference| {
                                batch
                                    .frame
                                    .as_ref()
                                    .is_some_and(|f| f.sequence == reference.sequence)
                            })
                            && r.measurement.device_id == first.device_id
                            && r.measurement.status == first.status
                    });
                if native {
                    let sql = self.insert_sql.entry((true,chunk.len())).or_insert_with(|| {
                        let rows = (0..chunk.len()).map(|i| {
                            let n=5+i*6;
                            format!("(?1,?{n},?2,?{},?{},?{},?{},?3,NULL,?4,?{})",n+1,n+2,n+3,n+4,n+5)
                        }).collect::<Vec<_>>().join(",");
                        format!("INSERT INTO measurements(session_id,ts,device_id,voltage_v,current_a,power_w,energy_wh,status,raw,frame_id,sample_index) VALUES {rows}")
                    });
                    let mut values: Vec<&dyn rusqlite::ToSql> =
                        Vec::with_capacity(4 + chunk.len() * 6);
                    // USB rows inherit the fixed device and timestamp status from
                    // their session. The detail view resolves both losslessly.
                    let inherit = self.usb
                        && first.device_id == self.device
                        && first.status == "estimated_timestamp";
                    let device: &dyn rusqlite::ToSql = if inherit {
                        &rusqlite::types::Null
                    } else {
                        &first.device_id
                    };
                    let status: &dyn rusqlite::ToSql = if inherit {
                        &rusqlite::types::Null
                    } else {
                        &first.status
                    };
                    values.extend_from_slice(&[&self.session_id, device, status, &frame_id]);
                    for (text, r) in timestamps.iter_mut().zip(chunk) {
                        text.clear();
                        text.push_str(timestamp.format(r.measurement.timestamp));
                    }
                    for (text, r) in timestamps.iter().zip(chunk) {
                        let m = &r.measurement;
                        values.extend_from_slice(&[
                            text,
                            &m.voltage_v,
                            &m.current_a,
                            &m.power_w,
                            &m.energy_wh,
                            &r.first.as_ref().unwrap().index,
                        ]);
                    }
                    tx.prepare_cached(sql)?
                        .execute(rusqlite::params_from_iter(values))?;
                    count += chunk.len() as u64;
                    represented += chunk.len() as u64;
                    continue;
                }
                let mut references = Vec::with_capacity(chunk.len());
                for (text, r) in timestamps.iter_mut().zip(chunk) {
                    text.clear();
                    text.push_str(timestamp.format(r.measurement.timestamp));
                    references.push((
                        resolve(r.first)?,
                        r.first.map(|f| f.index),
                        resolve(r.aggregate.as_ref().and_then(|a| a.last))?,
                        r.aggregate.as_ref().and_then(|a| a.last).map(|f| f.index),
                        r.aggregate
                            .as_ref()
                            .map(|a| a.end.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
                    ));
                }
                let columns = if aggregate {
                    ",source_count,end_ts,end_frame_id,end_sample_index,voltage_min,voltage_max,current_min,current_max,power_min,power_max,partial"
                } else {
                    ""
                };
                let placeholders =
                    format!("({})", vec!["?"; if aggregate { 22 } else { 11 }].join(","));
                let sql = format!("INSERT INTO measurements(session_id,ts,device_id,voltage_v,current_a,power_w,energy_wh,status,raw,frame_id,sample_index{columns}) VALUES {}",vec![placeholders;chunk.len()].join(","));
                let mut stmt = tx.prepare_cached(&sql)?;
                let mut values: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() * 22);
                for ((r, text), refs) in chunk.iter().zip(&timestamps).zip(&references) {
                    let m = &r.measurement;
                    let raw: &dyn rusqlite::ToSql = if r.first.is_some() {
                        &rusqlite::types::Null
                    } else {
                        &m.raw
                    };
                    values.extend_from_slice(&[
                        &self.session_id,
                        text,
                        &m.device_id,
                        &m.voltage_v,
                        &m.current_a,
                        &m.power_w,
                        &m.energy_wh,
                        &m.status,
                        raw,
                        &refs.0,
                        &refs.1,
                    ]);
                    if let Some(a) = &r.aggregate {
                        values.extend_from_slice(&[
                            &a.count, &refs.4, &refs.2, &refs.3, &a.min[0], &a.max[0], &a.min[1],
                            &a.max[1], &a.min[2], &a.max[2], &a.partial,
                        ]);
                    }
                    represented += r.count();
                }
                stmt.execute(rusqlite::params_from_iter(values))?;
                count += chunk.len() as u64;
            }
        }
        tx.execute("UPDATE sessions SET saved_count=saved_count+?2,saved_source_count=saved_source_count+?3,accepted_count=accepted_count+?3,received_count=received_count+?3,missing_packets=missing_packets+?4,invalid_samples=invalid_samples+?5 WHERE id=?1",params![self.session_id,count,represented,gaps,invalid])?;
        tx.commit().context("commit measurement batch")?;
        Ok((count, represented))
    }
    pub fn finish(&self, status: &Shared) -> Result<()> {
        let complete = status.error.is_none()
            && status.gaps == 0
            && status.invalid == 0
            && status.dropped == 0
            && status.accepted == status.saved_source;
        self.conn.execute("UPDATE sessions SET ended_at=?2,outcome=?3,error=?4,received_count=?5,accepted_count=?6,saved_count=?7,missing_packets=?8,invalid_samples=?9,dropped_samples=?10,discarded_bytes=?11,saved_source_count=?12 WHERE id=?1",params![self.session_id,chrono::Utc::now().to_rfc3339(),if complete{"complete"}else{"incomplete"},status.error,status.received,status.accepted,status.saved,status.gaps,status.invalid,status.dropped,status.discarded_bytes,status.saved_source])?;
        // Commits already fsync the WAL. Backfill only after the session ends,
        // rather than making every capacity write also copy pages to the SD DB.
        // PASSIVE leaves pinned exports intact; their WAL pages stay available.
        self.conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        Ok(())
    }
}

/// Initialize/migrate without creating a capture session.
pub fn open_database(path: &str) -> Result<Connection> {
    if let Some(parent) = std::path::Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).context("create database directory")?;
    }
    let mut conn = Connection::open(path).context("open measurement database")?;
    conn.busy_timeout(std::time::Duration::from_secs(2))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA cache_size=-8192; PRAGMA wal_autocheckpoint=0;",
    )?;
    let tx = conn.transaction()?;
    initialize_schema(&tx)?;
    tx.commit()?;
    Ok(conn)
}

pub(crate) fn initialize_schema(tx: &rusqlite::Transaction<'_>) -> Result<()> {
    tx.execute_batch("CREATE TABLE IF NOT EXISTS sessions(id INTEGER PRIMARY KEY,device_id TEXT,started_at TEXT NOT NULL,ended_at TEXT);
        CREATE TABLE IF NOT EXISTS measurements(id INTEGER PRIMARY KEY,session_id INTEGER NOT NULL,ts TEXT NOT NULL,device_id TEXT,voltage_v REAL,current_a REAL,power_w REAL,energy_wh REAL,status TEXT,raw BLOB);")?;
    let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    ensure!(
        version <= 2,
        "database schema is newer than this application"
    );
    tx.execute_batch("CREATE TABLE IF NOT EXISTS frames(id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL REFERENCES sessions(id), packet_id INTEGER, received_at TEXT NOT NULL, raw BLOB NOT NULL, missing_before INTEGER NOT NULL, invalid_samples INTEGER NOT NULL);")?;
    // Inspect columns as well as user_version to preserve databases made by the MVP.
    for (table, name, definition) in [
        ("sessions", "transport", "TEXT"),
        ("sessions", "calibration", "BLOB"),
        ("sessions", "calibration_received_at", "TEXT"),
        ("sessions", "timestamp_basis", "TEXT"),
        ("sessions", "outcome", "TEXT"),
        ("sessions", "error", "TEXT"),
        ("sessions", "received_count", "INTEGER NOT NULL DEFAULT 0"),
        ("sessions", "accepted_count", "INTEGER NOT NULL DEFAULT 0"),
        ("sessions", "saved_count", "INTEGER NOT NULL DEFAULT 0"),
        ("sessions", "missing_packets", "INTEGER NOT NULL DEFAULT 0"),
        ("sessions", "invalid_samples", "INTEGER NOT NULL DEFAULT 0"),
        ("sessions", "dropped_samples", "INTEGER NOT NULL DEFAULT 0"),
        ("sessions", "discarded_bytes", "INTEGER NOT NULL DEFAULT 0"),
        ("measurements", "frame_id", "INTEGER REFERENCES frames(id)"),
        ("measurements", "sample_index", "INTEGER"),
        (
            "sessions",
            "sample_rate_hz",
            "INTEGER NOT NULL DEFAULT 10000",
        ),
        (
            "sessions",
            "buffer_size_bytes",
            "INTEGER NOT NULL DEFAULT 10000000",
        ),
        (
            "sessions",
            "saved_source_count",
            "INTEGER NOT NULL DEFAULT 0",
        ),
        ("sessions", "name", "TEXT NOT NULL DEFAULT ''"),
        ("frames", "capture_sequence", "INTEGER"),
        ("measurements", "source_count", "INTEGER NOT NULL DEFAULT 1"),
        ("measurements", "end_ts", "TEXT"),
        (
            "measurements",
            "end_frame_id",
            "INTEGER REFERENCES frames(id)",
        ),
        ("measurements", "end_sample_index", "INTEGER"),
        ("measurements", "voltage_min", "REAL"),
        ("measurements", "voltage_max", "REAL"),
        ("measurements", "current_min", "REAL"),
        ("measurements", "current_max", "REAL"),
        ("measurements", "power_min", "REAL"),
        ("measurements", "power_max", "REAL"),
        ("measurements", "partial", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        let names = tx
            .prepare(&format!("PRAGMA table_info({table})"))?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if !names.iter().any(|s| s == name) {
            tx.execute_batch(&format!(
                "ALTER TABLE {table} ADD COLUMN {name} {definition}"
            ))?;
        }
    }
    tx.execute_batch("CREATE TABLE IF NOT EXISTS frames(id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL REFERENCES sessions(id), packet_id INTEGER, received_at TEXT NOT NULL, raw BLOB NOT NULL, missing_before INTEGER NOT NULL, invalid_samples INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS measurements_session ON measurements(session_id);
        CREATE INDEX IF NOT EXISTS frames_session ON frames(session_id);
        CREATE UNIQUE INDEX IF NOT EXISTS frames_sequence ON frames(session_id,capture_sequence); PRAGMA user_version=2;")?;
    if version < 2 {
        tx.execute("UPDATE sessions SET saved_source_count=saved_count", [])?;
    }
    let columns = tx
        .prepare("PRAGMA table_info(measurements)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let projection = columns.iter().map(|name|match name.as_str() {
        "device_id" => "coalesce(m.device_id,s.device_id) AS device_id".into(),
        "status" => "coalesce(m.status,CASE WHEN s.transport='usb-cc' THEN 'estimated_timestamp' END) AS status".into(),
        _ => format!("m.{name}"),
    }).collect::<Vec<_>>().join(",");
    tx.execute_batch(&format!("CREATE VIEW IF NOT EXISTS measurement_details AS SELECT {projection} FROM measurements m LEFT JOIN sessions s ON s.id=m.session_id"))?;
    Ok(())
}

/// Import a closed capture in one transaction. SQLite streams rows; no sample
/// collection is loaded into Rust memory. Source IDs are never reused directly.
pub fn import_capture(
    source: &std::path::Path,
    target: &std::path::Path,
    progress: impl Fn(u64, u64),
) -> Result<u64> {
    let mut conn = Connection::open(target).context("open save destination")?;
    conn.busy_timeout(std::time::Duration::from_secs(2))?;
    conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;")?;
    conn.execute(
        "ATTACH DATABASE ?1 AS pending",
        params![source.to_str().context("non-UTF8 capture path")?],
    )?;
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    initialize_schema(&tx)?;
    let total: u64 = tx.query_row("SELECT count(*) FROM pending.measurements", [], |r| {
        r.get(0)
    })?;
    progress(0, total);
    tx.execute_batch(
        "CREATE TEMP TABLE session_map(old_id INTEGER PRIMARY KEY,new_id INTEGER NOT NULL);",
    )?;
    let columns_for = |table: &str| -> Result<Vec<String>> {
        Ok(tx
            .prepare(&format!("PRAGMA pending.table_info({table})"))?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .filter(|n| n != "id")
            .collect())
    };
    let session_columns = columns_for("sessions")?;
    let columns = session_columns.join(",");
    {
        let mut statement = tx.prepare("SELECT id FROM pending.sessions ORDER BY id")?;
        let mut ids = statement.query([])?;
        while let Some(row) = ids.next()? {
            let old: i64 = row.get(0)?;
            tx.execute(&format!("INSERT INTO main.sessions({columns}) SELECT {columns} FROM pending.sessions WHERE id=?1"),params![old])?;
            let new = tx.last_insert_rowid();
            tx.execute("INSERT INTO session_map VALUES (?1,?2)", params![old, new])?;
        }
    }
    let offset: i64 = tx.query_row("SELECT coalesce(max(id),0) FROM main.frames", [], |r| {
        r.get(0)
    })?;
    let source_max: i64 =
        tx.query_row("SELECT coalesce(max(id),0) FROM pending.frames", [], |r| {
            r.get(0)
        })?;
    ensure!(
        offset.checked_add(source_max).is_some(),
        "frame ID overflow"
    );
    let frame_columns = columns_for("frames")?;
    let frame_select = frame_columns
        .iter()
        .map(|n| {
            if n == "session_id" {
                "s.new_id".into()
            } else {
                format!("f.{n}")
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    tx.execute(&format!("INSERT INTO main.frames(id,{}) SELECT f.id+?1,{frame_select} FROM pending.frames f JOIN session_map s ON s.old_id=f.session_id",frame_columns.join(",")),params![offset])?;
    let measurement_columns = columns_for("measurements")?;
    let measurement_select = measurement_columns
        .iter()
        .map(|n| match n.as_str() {
            "session_id" => "s.new_id".into(),
            "frame_id" | "end_frame_id" => {
                format!("CASE WHEN m.{n} IS NULL THEN NULL ELSE m.{n}+?1 END")
            }
            _ => format!("m.{n}"),
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut last = 0_i64;
    let mut copied = 0_u64;
    loop {
        let end:Option<i64>=tx.query_row("SELECT max(id) FROM (SELECT id FROM pending.measurements WHERE id>?1 ORDER BY id LIMIT 4096)",params![last],|r|r.get(0))?;
        let Some(end) = end else { break };
        let n=tx.execute(&format!("INSERT INTO main.measurements({}) SELECT {measurement_select} FROM pending.measurements m JOIN session_map s ON s.old_id=m.session_id WHERE m.id>?2 AND m.id<=?3 ORDER BY m.id",measurement_columns.join(",")),params![offset,last,end])?;
        copied += n as u64;
        last = end;
        progress(copied, total);
    }
    if !session_columns.iter().any(|n| n == "saved_source_count") {
        tx.execute("UPDATE sessions SET saved_source_count=saved_count WHERE id IN (SELECT new_id FROM session_map)",[])?;
    }
    ensure!(copied == total, "import count mismatch: {copied}/{total}");
    tx.commit().context("commit captured data")?;
    Ok(copied)
}

pub fn time_range_name(start: &str, end: &str) -> String {
    fn display(value: &str) -> String {
        chrono::DateTime::parse_from_rfc3339(value)
            .map(|t| t.with_timezone(&chrono::Utc).format("%Y-%m-%d %H:%M:%SZ").to_string())
            .unwrap_or_else(|_| value.into())
    }
    format!("{} - {}", display(start), display(end))
}
pub fn validate_session_name(name: &str) -> Result<String> {
    ensure!(!name.chars().any(char::is_control), "name contains control characters");
    let name = name.trim();
    ensure!(name.len() <= 128, "name exceeds 128 UTF-8 bytes");
    Ok(name.into())
}

pub fn update_session_name(path: &std::path::Path, id: i64, name: &str) -> Result<()> {
    let mut conn = Connection::open(path)?;
    let tx = conn.transaction()?;
    let mut name = validate_session_name(name)?;
    if name.is_empty() {
        let (start, end): (String, Option<String>) = tx.query_row("SELECT started_at,ended_at FROM sessions WHERE id=?1", [id], |r| Ok((r.get(0)?,r.get(1)?)))?;
        name = time_range_name(&start, &end.unwrap_or_else(|| chrono::Utc::now().to_rfc3339()));
    }
    let changed = tx.execute("UPDATE sessions SET name=?1 WHERE id=?2", params![name, id])?;
    ensure!(changed == 1, "session not found");
    tx.commit()?;
    Ok(())
}

pub fn delete_session(path: &std::path::Path, id: i64) -> Result<()> {
    let mut conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA foreign_keys=ON")?;
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM measurements WHERE session_id=?1", [id])?;
    tx.execute("DELETE FROM frames WHERE session_id=?1", [id])?;
    ensure!(tx.execute("DELETE FROM sessions WHERE id=?1", [id])? == 1, "session not found");
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn session_name_validation_trims_and_rejects_invalid_values() {
        assert_eq!(super::validate_session_name("  测试  ").unwrap(), "测试");
        assert!(super::validate_session_name("").unwrap().is_empty());
        assert!(super::validate_session_name("a\n").is_err());
        assert!(super::validate_session_name(&"x".repeat(129)).is_err());
    }
    #[test]
    #[ignore = "manual hardware throughput benchmark; synthetic data only"]
    fn profile_recording_stages() {
        use crate::recording::{tests::batch, Resampler};
        use std::time::Instant;
        let dir = tempfile::tempdir().unwrap();
        let info = SessionInfo {
            device: "synthetic".into(),
            transport: "usb-cc",
            calibration: vec![],
            received: chrono::Utc::now(),
            name: None,
        };
        for cache_kib in [8192, 8194] {
            let path = dir.path().join(format!("profile-{cache_kib}.db"));
            let mut store = Store::open(path.to_str().unwrap(), &info).unwrap();
            store
                .conn
                .pragma_update(None, "cache_size", -cache_kib)
                .unwrap();
            store
                .conn
                .pragma_update(
                    None,
                    "wal_autocheckpoint",
                    if cache_kib == 8194 { 0 } else { 1000 },
                )
                .unwrap();
            let start = Instant::now();
            let mut sampler = Resampler::new(10000);
            let mut history = crate::history::History::default();
            let mut metrics = crate::domain::Metrics::default();
            let mut batches = Vec::new();
            for i in 0..100 {
                let batch = batch(i * 800, 800);
                for (_, m) in &batch.samples {
                    history.observe(m);
                    metrics.observe_values(m);
                }
                batches.push(sampler.process(batch));
            }
            let processing = start.elapsed();
            let start = Instant::now();
            for group in batches.chunks(32) {
                store.insert_recorded(group).unwrap();
            }
            println!(
                "cache_kib={cache_kib} samples=80000 processing={processing:?} write={:?}",
                start.elapsed()
            );
        }
    }
    #[test]
    fn version_one_migration_backfills_represented_samples() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE sessions(id INTEGER PRIMARY KEY,device_id TEXT,started_at TEXT NOT NULL,ended_at TEXT,saved_count INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE measurements(id INTEGER PRIMARY KEY,session_id INTEGER NOT NULL,ts TEXT NOT NULL,device_id TEXT,voltage_v REAL,current_a REAL,power_w REAL,energy_wh REAL,status TEXT,raw BLOB);
            INSERT INTO sessions VALUES(1,'legacy','2026-01-01',NULL,1);
            INSERT INTO measurements(id,session_id,ts,voltage_v) VALUES(1,1,'2026-01-01',5);
            PRAGMA user_version=1;").unwrap();
        drop(conn);
        let conn = open_database(path.to_str().unwrap()).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row("SELECT saved_source_count FROM sessions", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT source_count FROM measurements", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT voltage_v FROM measurements", [], |r| r
                .get::<_, f64>(0))
                .unwrap(),
            5.0
        );
    }
    #[test]
    fn aggregate_import_maps_both_frame_ends_and_preserves_history() {
        use crate::recording::{tests::batch, Resampler};
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        let target = dir.path().join("target.db");
        let info = SessionInfo {
            device: "synthetic".into(),
            transport: "usb-cc",
            calibration: vec![1, 2],
            received: chrono::Utc::now(),
            name: None,
        };
        let config = Config {
            sample_rate_hz: 1,
            ..Config::default()
        };
        let mut store = Store::open_config(source.to_str().unwrap(), &info, config).unwrap();
        let mut sampler = Resampler::new(1);
        for n in 0..2 {
            store
                .insert_recorded(&[sampler.process(batch(n * 800, 800))])
                .unwrap();
        }
        store
            .insert_recorded(&[RecordedBatch {
                frame: None,
                records: vec![sampler.finish().unwrap()],
            }])
            .unwrap();
        store
            .finish(&Shared {
                accepted: 1600,
                received: 1600,
                saved: 1,
                saved_source: 1600,
                ..Shared::default()
            })
            .unwrap();
        drop(store);
        let mut old = Store::open(target.to_str().unwrap(), &info).unwrap();
        old.insert_recorded(&[Resampler::new(10000).process(batch(0, 1))])
            .unwrap();
        drop(old);
        assert_eq!(import_capture(&source, &target, |_, _| {}).unwrap(), 1);
        let conn = Connection::open(&target).unwrap();
        let row:(i64,i64,i64,i64,i64) = conn.query_row("SELECT source_count,frame_id,end_frame_id,sample_index,end_sample_index FROM measurements WHERE session_id=2",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
        assert_eq!(row, (1600, 2, 3, 0, 799));
        let inherited: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT device_id,status FROM measurements WHERE session_id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(inherited, (None, None));
        let detail: (String, String) = conn
            .query_row(
                "SELECT device_id,status FROM measurement_details WHERE session_id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(detail, ("synthetic".into(), "estimated_timestamp".into()));
        assert_eq!(
            conn.query_row("SELECT sample_rate_hz FROM sessions WHERE id=2", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM measurements WHERE session_id=1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }
    #[test]
    fn cached_timestamps_preserve_chrono_precision_boundaries_and_leap_seconds() {
        let mut formatter = super::SampleTimestamp::default();
        for text in [
            "2026-09-12T23:59:59.999999999Z",
            "2026-09-13T00:00:00.000001Z",
            "2026-09-13T00:00:00.123456Z",
            "1969-12-31T23:59:59.000001Z",
            "2016-12-31T23:59:60.123456Z",
            "2016-12-31T23:59:59.123456Z",
        ] {
            let time = chrono::DateTime::parse_from_rfc3339(text)
                .unwrap()
                .with_timezone(&chrono::Utc);
            assert_eq!(
                formatter.format(time),
                time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
            );
        }
        use chrono::TimeZone;
        let extended = chrono::Utc.with_ymd_and_hms(12026, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(
            formatter.format(extended),
            extended.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
        );
    }
    use super::*;
    use crate::protocol::{
        tests::{packet, status},
        Calibration, Decoder,
    };
    #[test]
    fn bulk_insert_rolls_back_partial_chunks_and_retries_with_exact_samples() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bulk.db");
        let now = chrono::Utc::now();
        let info = SessionInfo {
            device: "synthetic".into(),
            transport: "usb-cc",
            calibration: status(),
            received: now,
            name: None,
        };
        let mut store = Store::open(path.to_str().unwrap(), &info).unwrap();
        let mut decoder = Decoder::new(Calibration::parse(&status()).unwrap(), info.device.clone());
        let batch = decoder.decode(packet(1), now).unwrap();
        store.conn.execute_batch("CREATE TRIGGER reject_later_row BEFORE INSERT ON measurements WHEN NEW.sample_index=70 BEGIN SELECT RAISE(FAIL,'synthetic failure after first chunk'); END;").unwrap();
        assert!(store.insert_batches(std::slice::from_ref(&batch)).is_err());
        for table in ["measurements", "frames"] {
            assert_eq!(
                store
                    .conn
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                        .get::<_, u64>(0))
                    .unwrap(),
                0
            );
        }
        store
            .conn
            .execute_batch("DROP TRIGGER reject_later_row")
            .unwrap();
        assert_eq!(
            store.insert_batches(std::slice::from_ref(&batch)).unwrap(),
            800
        );
        let mut stmt = store.conn.prepare("SELECT ts,voltage_v,current_a,power_w,energy_wh,sample_index FROM measurements ORDER BY id").unwrap();
        let mut rows = stmt.query([]).unwrap();
        for (index, m) in batch.samples {
            let row = rows.next().unwrap().unwrap();
            assert_eq!(
                row.get::<_, String>(0).unwrap(),
                m.timestamp
                    .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
            );
            for (column, value) in [
                (1, m.voltage_v),
                (2, m.current_a),
                (3, m.power_w),
                (4, m.energy_wh),
            ] {
                assert_eq!(row.get::<_, f64>(column).unwrap(), value);
            }
            assert_eq!(row.get::<_, u32>(5).unwrap(), index);
        }
        assert!(rows.next().unwrap().is_none());
    }
    #[test]
    fn migrates_legacy_database_and_stores_each_frame_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE sessions(id INTEGER PRIMARY KEY,device_id TEXT,started_at TEXT NOT NULL,ended_at TEXT); CREATE TABLE measurements(id INTEGER PRIMARY KEY,session_id INTEGER NOT NULL,ts TEXT NOT NULL,device_id TEXT,voltage_v REAL,current_a REAL,power_w REAL,energy_wh REAL,status TEXT,raw BLOB);INSERT INTO sessions VALUES (1,'legacy','old',NULL); INSERT INTO measurements(session_id,ts,voltage_v) VALUES (1,'old',5);").unwrap();
        let now = chrono::Utc::now();
        let info = SessionInfo {
            device: "synthetic".into(),
            transport: "usb-cc",
            calibration: status(),
            received: now,
            name: None,
        };
        let mut store = Store::open(path.to_str().unwrap(), &info).unwrap();
        let mut decoder = Decoder::new(Calibration::parse(&status()).unwrap(), info.device.clone());
        let batch = decoder.decode(packet(17), now).unwrap();
        assert_eq!(store.insert_batches(&[batch]).unwrap(), 800);
        let state = Shared {
            received: 800,
            accepted: 800,
            saved: 800,
            ..Shared::default()
        };
        store.finish(&state).unwrap();
        let result:(i64,i64,i64)=conn.query_row("SELECT count(*),count(DISTINCT frame_id),sum(length(coalesce(raw,x''))) FROM measurements WHERE session_id=2",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(result, (800, 1, 0));
        assert_eq!(
            conn.query_row("SELECT length(raw) FROM frames", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3212
        );
        assert_eq!(
            conn.query_row(
                "SELECT max(sample_index) FROM measurements WHERE session_id=2",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            799
        );
        assert_eq!(
            conn.query_row(
                "SELECT voltage_v FROM measurements WHERE session_id=1",
                [],
                |r| r.get::<_, f64>(0)
            )
            .unwrap(),
            5.0
        );
        drop(store);
        let reopened = Store::open(path.to_str().unwrap(), &info).unwrap();
        assert_eq!(reopened.session_id, 3);
    }
}
