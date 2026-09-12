use crate::{
    runtime::Shared,
    source::{Batch, SessionInfo},
};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection};

pub struct Store {
    conn: Connection,
    session_id: i64,
}
impl Store {
    pub fn open(path: &str, info: &SessionInfo) -> Result<Self> {
        if let Some(parent) = std::path::Path::new(path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).context("create database directory")?;
        }
        let mut conn = Connection::open(path).context("open measurement database")?;
        conn.busy_timeout(std::time::Duration::from_secs(2))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let tx = conn.transaction()?;
        initialize_schema(&tx)?;
        tx.execute("INSERT INTO sessions(device_id,started_at,transport,calibration,calibration_received_at,timestamp_basis,outcome) VALUES (?1,?2,?3,?4,?2,?5,'incomplete')",params![info.device,info.received.to_rfc3339(),info.transport,info.calibration,if info.transport=="usb-cc" {"estimated: first packet reception, 100 us/sample, packet counter"} else {"source timestamp"}])?;
        let session_id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(Self { conn, session_id })
    }
    pub fn insert_batches(&mut self, batches: &[Batch]) -> Result<u64> {
        let tx = self
            .conn
            .transaction()
            .context("begin measurement transaction")?;
        let mut count = 0;
        {
            let mut frame_stmt=tx.prepare_cached("INSERT INTO frames(session_id,packet_id,received_at,raw,missing_before,invalid_samples) VALUES (?1,?2,?3,?4,?5,?6)")?;
            let mut sample_stmt=tx.prepare_cached("INSERT INTO measurements(session_id,ts,device_id,voltage_v,current_a,power_w,energy_wh,status,raw,frame_id,sample_index) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)")?;
            for batch in batches {
                let frame_id = if batch.frame.is_empty() {
                    None
                } else {
                    frame_stmt.execute(params![
                        self.session_id,
                        batch.packet_id,
                        batch.received.to_rfc3339(),
                        batch.frame,
                        batch.gaps,
                        batch.invalid
                    ])?;
                    Some(tx.last_insert_rowid())
                };
                for (index, m) in &batch.samples {
                    sample_stmt.execute(params![
                        self.session_id,
                        m.timestamp
                            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                        m.device_id,
                        m.voltage_v,
                        m.current_a,
                        m.power_w,
                        m.energy_wh,
                        m.status,
                        if frame_id.is_some() {
                            None
                        } else {
                            Some(&m.raw)
                        },
                        frame_id,
                        index
                    ])?;
                    count += 1;
                }
            }
        }
        tx.commit().context("commit measurement batch")?;
        Ok(count)
    }
    pub fn finish(&self, status: &Shared) -> Result<()> {
        let complete = status.error.is_none()
            && status.gaps == 0
            && status.invalid == 0
            && status.dropped == 0
            && status.accepted == status.saved;
        self.conn.execute("UPDATE sessions SET ended_at=?2,outcome=?3,error=?4,received_count=?5,accepted_count=?6,saved_count=?7,missing_packets=?8,invalid_samples=?9,dropped_samples=?10,discarded_bytes=?11 WHERE id=?1",params![self.session_id,chrono::Utc::now().to_rfc3339(),if complete{"complete"}else{"incomplete"},status.error,status.received,status.accepted,status.saved,status.gaps,status.invalid,status.dropped,status.discarded_bytes])?;
        Ok(())
    }
}

fn initialize_schema(tx: &rusqlite::Transaction<'_>) -> Result<()> {
    tx.execute_batch("CREATE TABLE IF NOT EXISTS sessions(id INTEGER PRIMARY KEY,device_id TEXT,started_at TEXT NOT NULL,ended_at TEXT);
        CREATE TABLE IF NOT EXISTS measurements(id INTEGER PRIMARY KEY,session_id INTEGER NOT NULL,ts TEXT NOT NULL,device_id TEXT,voltage_v REAL,current_a REAL,power_w REAL,energy_wh REAL,status TEXT,raw BLOB);")?;
    let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    ensure!(
        version <= 1,
        "database schema is newer than this application"
    );
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
        PRAGMA user_version=1;")?;
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
    let columns="device_id,started_at,ended_at,transport,calibration,calibration_received_at,timestamp_basis,outcome,error,received_count,accepted_count,saved_count,missing_packets,invalid_samples,dropped_samples,discarded_bytes";
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
    tx.execute("INSERT INTO main.frames(id,session_id,packet_id,received_at,raw,missing_before,invalid_samples) SELECT f.id+?1,s.new_id,f.packet_id,f.received_at,f.raw,f.missing_before,f.invalid_samples FROM pending.frames f JOIN session_map s ON s.old_id=f.session_id",params![offset])?;
    let mut last = 0_i64;
    let mut copied = 0_u64;
    loop {
        let end:Option<i64>=tx.query_row("SELECT max(id) FROM (SELECT id FROM pending.measurements WHERE id>?1 ORDER BY id LIMIT 4096)",params![last],|r|r.get(0))?;
        let Some(end) = end else { break };
        let n=tx.execute("INSERT INTO main.measurements(session_id,ts,device_id,voltage_v,current_a,power_w,energy_wh,status,raw,frame_id,sample_index) SELECT s.new_id,m.ts,m.device_id,m.voltage_v,m.current_a,m.power_w,m.energy_wh,m.status,m.raw,CASE WHEN m.frame_id IS NULL THEN NULL ELSE m.frame_id+?1 END,m.sample_index FROM pending.measurements m JOIN session_map s ON s.old_id=m.session_id WHERE m.id>?2 AND m.id<=?3 ORDER BY m.id",params![offset,last,end])?;
        copied += n as u64;
        last = end;
        progress(copied, total);
    }
    ensure!(copied == total, "import count mismatch: {copied}/{total}");
    tx.commit().context("commit captured data")?;
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        tests::{packet, status},
        Calibration, Decoder,
    };
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
