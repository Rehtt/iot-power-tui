//! Versioned, read-only LAN API. Capture owns the writer; exports use read snapshots.
use crate::{
    history::Bucket,
    runtime::{Runtime, Shared, State},
    Args,
};
use anyhow::{Context, Result};
use axum::{
    extract::{Path, Query, State as WebState},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Live {
    pub version: u32,
    pub instance: String,
    pub session_id: Option<i64>,
    pub state: State,
    pub device: Option<String>,
    pub received: u64,
    pub accepted: u64,
    pub saved: u64,
    pub gaps: u64,
    pub invalid: u64,
    pub dropped: u64,
    pub error: Option<String>,
    pub rate: f64,
    pub updated_at: String,
    pub latest: Option<Reading>,
    pub count: u64,
    pub averages: [f64; 3],
    pub peak_power: f64,
    pub buckets: Vec<Bucket>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Reading {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub voltage_v: f64,
    pub current_a: f64,
    pub power_w: f64,
    pub energy_wh: f64,
}
impl Live {
    fn snapshot(s: &Shared, instance: &str, rate: f64) -> Self {
        Self {
            version: 1,
            instance: instance.into(),
            session_id: s.session_id,
            state: s.state,
            device: s.device.clone(),
            received: s.received,
            accepted: s.accepted,
            saved: s.saved,
            gaps: s.gaps,
            invalid: s.invalid,
            dropped: s.dropped,
            error: s.error.clone(),
            rate,
            updated_at: chrono::Utc::now().to_rfc3339(),
            latest: s.metrics.latest.as_ref().map(|m| Reading {
                timestamp: m.timestamp,
                voltage_v: m.voltage_v,
                current_a: m.current_a,
                power_w: m.power_w,
                energy_wh: m.energy_wh,
            }),
            count: s.metrics.count,
            averages: [
                s.metrics.average_voltage,
                s.metrics.average_current,
                s.metrics.average_power,
            ],
            peak_power: s.metrics.peak_power,
            buckets: s.history.buckets(),
        }
    }
    pub fn shared(&self) -> Shared {
        let mut s = Shared {
            state: self.state,
            session_id: self.session_id,
            device: self.device.clone(),
            received: self.received,
            accepted: self.accepted,
            saved: self.saved,
            gaps: self.gaps,
            invalid: self.invalid,
            dropped: self.dropped,
            error: self.error.clone(),
            history: crate::history::History::from_buckets(self.buckets.clone()),
            ..Shared::default()
        };
        s.metrics = crate::domain::Metrics {
            latest: self.latest.as_ref().map(|m| crate::domain::Measurement {
                timestamp: m.timestamp,
                device_id: self.device.clone().unwrap_or_default(),
                voltage_v: m.voltage_v,
                current_a: m.current_a,
                power_w: m.power_w,
                energy_wh: m.energy_wh,
                status: "remote".into(),
                raw: vec![],
            }),
            count: self.count,
            average_voltage: self.averages[0],
            average_current: self.averages[1],
            average_power: self.averages[2],
            peak_power: self.peak_power,
        };
        s
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: i64,
    pub device: Option<String>,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub saved: u64,
    pub outcome: Option<String>,
}
#[derive(Default, Deserialize)]
struct Page {
    before: Option<i64>,
}
#[derive(Clone)]
struct Api {
    live: Arc<Mutex<Live>>,
    db: PathBuf,
    export: Arc<tokio::sync::Semaphore>,
}
type ApiError = (StatusCode, String);
fn internal(e: impl std::fmt::Display) -> ApiError {
    eprintln!("API: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "database operation failed".into(),
    )
}
async fn status(WebState(api): WebState<Api>) -> Json<Live> {
    Json(api.live.lock().unwrap().clone())
}
async fn sessions(
    WebState(api): WebState<Api>,
    Query(page): Query<Page>,
) -> Result<Json<Vec<Session>>, ApiError> {
    tokio::task::spawn_blocking(move || -> Result<_,anyhow::Error> {
        if !api.db.exists() { return Ok(Json(Vec::new())); }
        let conn = read_database(&api.db)?;
        let mut stmt=conn.prepare("SELECT id,device_id,started_at,ended_at,saved_count,outcome FROM sessions WHERE id<?1 ORDER BY id DESC LIMIT 50")?;
        let rows=stmt.query_map([page.before.unwrap_or(i64::MAX)],|r|Ok(Session{id:r.get(0)?,device:r.get(1)?,started_at:r.get(2)?,ended_at:r.get(3)?,saved:r.get(4)?,outcome:r.get(5)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(Json(rows))
    }).await.map_err(internal)?.map_err(internal)
}
pub fn read_database(path: &std::path::Path) -> Result<rusqlite::Connection> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(Duration::from_secs(2))?;
    Ok(conn)
}
/// Copy bounded batches from one consistent snapshot, preserving all row references.
pub fn export_session(
    source: &std::path::Path,
    target: &std::path::Path,
    id: i64,
    cancel: &AtomicBool,
) -> Result<()> {
    let mut source = read_database(source)?;
    let snapshot = source.transaction()?;
    let ended: Option<String> = snapshot
        .query_row("SELECT ended_at FROM sessions WHERE id=?1", [id], |r| {
            r.get(0)
        })
        .context("session not found")?;
    let mut dest = rusqlite::Connection::open(target)?;
    dest.execute_batch("PRAGMA foreign_keys=ON;")?;
    let tx = dest.transaction()?;
    crate::storage::initialize_schema(&tx)?;
    for table in ["sessions", "frames", "measurements"] {
        let key = if table == "sessions" {
            "id"
        } else {
            "session_id"
        };
        let mut last = 0i64;
        loop {
            anyhow::ensure!(!cancel.load(Ordering::Relaxed), "export cancelled");
            let mut stmt = snapshot.prepare(&format!(
                "SELECT * FROM {table} WHERE {key}=?1 AND id>?2 ORDER BY id LIMIT 4096"
            ))?;
            let cols = stmt.column_names().join(",");
            let n = stmt.column_count();
            let rows = stmt
                .query_map(rusqlite::params![id, last], |r| {
                    (0..n)
                        .map(|i| r.get::<_, rusqlite::types::Value>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if rows.is_empty() {
                break;
            }
            let sql = format!(
                "INSERT INTO {table}({cols}) VALUES ({})",
                vec!["?"; n].join(",")
            );
            let mut insert = tx.prepare(&sql)?;
            for row in rows {
                if let rusqlite::types::Value::Integer(v) = row[0] {
                    last = v;
                }
                insert.execute(rusqlite::params_from_iter(row))?;
            }
        }
    }
    tx.execute_batch("CREATE TABLE export_metadata(exported_at TEXT NOT NULL,source_session_id INTEGER NOT NULL,partial INTEGER NOT NULL);")?;
    tx.execute(
        "INSERT INTO export_metadata VALUES (?1,?2,?3)",
        rusqlite::params![chrono::Utc::now().to_rfc3339(), id, ended.is_none()],
    )?;
    tx.commit()?;
    Ok(())
}
struct CancelExport(Arc<AtomicBool>);
impl Drop for CancelExport {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
async fn download(WebState(api): WebState<Api>, Path(id): Path<i64>) -> Result<Response, ApiError> {
    let permit = api
        .export
        .clone()
        .try_acquire_owned()
        .map_err(|_| (StatusCode::CONFLICT, "another export is in progress".into()))?;
    let cancel = Arc::new(AtomicBool::new(false));
    let _guard = CancelExport(cancel.clone());
    let (file, dir, permit) = tokio::task::spawn_blocking(move || -> Result<_> {
        let dir = tempfile::Builder::new()
            .prefix("iot-power-export-")
            .tempdir()?;
        let path = dir.path().join("session.db");
        export_session(&api.db, &path, id, &cancel)?;
        Ok((std::fs::File::open(path)?, dir, permit))
    })
    .await
    .map_err(internal)?
    .map_err(internal)?;
    // Unfold owns both guards until EOF or client cancellation drops the body.
    let reader = tokio_util::io::ReaderStream::new(tokio::fs::File::from_std(file));
    let stream = futures_util::stream::unfold(
        (reader, dir, permit),
        |(mut reader, dir, permit)| async move {
            use futures_util::StreamExt;
            reader
                .next()
                .await
                .map(|chunk| (chunk, (reader, dir, permit)))
        },
    );
    Ok((
        [
            (header::CONTENT_TYPE, "application/vnd.sqlite3"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=session.db",
            ),
        ],
        axum::body::Body::from_stream(stream),
    )
        .into_response())
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM");
        tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
async fn supervise(
    mut source: impl FnMut() -> Box<dyn crate::source::DataSource>,
    db: &str,
    usb: bool,
    live: &Mutex<Live>,
    instance: &str,
    should_stop: impl Fn() -> bool,
) {
    let mut capture = Runtime::start(source(), db.to_owned());
    let mut retry = 2u64;
    let mut retry_at = None;
    let mut finalized = false;
    let mut rate_at = Instant::now();
    let mut rate_count = 0;
    let mut rate = 0.0;
    while !should_stop() {
        let s = capture.shared.lock().unwrap().clone();
        if rate_at.elapsed() >= Duration::from_secs(1) {
            rate = s.received.saturating_sub(rate_count) as f64 / rate_at.elapsed().as_secs_f64();
            rate_count = s.received;
            rate_at = Instant::now();
        }
        *live.lock().unwrap() = Live::snapshot(&s, instance, rate);
        if matches!(s.state, State::Fault | State::Stopped) && !finalized {
            capture.stop();
            finalized = true;
            eprintln!(
                "Session {:?}: accepted={} saved={} error={:?}",
                s.session_id,
                s.accepted,
                capture.shared.lock().unwrap().saved,
                s.error
            );
            if usb && !capture.shared.lock().unwrap().storage_fault {
                retry_at = Some(Instant::now() + Duration::from_secs(retry));
                retry = (retry * 2).min(30);
            }
        }
        if s.state == State::Capturing && s.saved > 0 {
            retry = 2;
        }
        if retry_at.is_some_and(|t| Instant::now() >= t) {
            capture = Runtime::start(source(), db.to_owned());
            retry_at = None;
            finalized = false;
            rate_count = 0;
            rate = 0.0;
            rate_at = Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    capture.stop();
    {
        let s = capture.shared.lock().unwrap();
        eprintln!("Service stopped: accepted={} saved={}", s.accepted, s.saved);
    }
}
pub fn run(args: &Args) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let addr = args.addr.unwrap_or_else(|| "0.0.0.0:8080".parse().unwrap());
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .context("bind service address")?;
        drop(crate::storage::open_database(&args.db)?);
        let instance = format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let live = Arc::new(Mutex::new(Live::snapshot(
            &Shared::default(),
            &instance,
            0.0,
        )));
        let api = Api {
            live: live.clone(),
            db: PathBuf::from(&args.db),
            export: Arc::new(tokio::sync::Semaphore::new(1)),
        };
        let app = Router::new()
            .route("/api/v1/status", get(status))
            .route("/api/v1/sessions", get(sessions))
            .route("/api/v1/sessions/{id}/download", get(download))
            .with_state(api);
        let stopping = Arc::new(AtomicBool::new(false));
        let signal = stopping.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    shutdown_signal().await;
                    signal.store(true, Ordering::Relaxed);
                })
                .await
        });
        eprintln!(
            "Service listening on {addr}; database={}; trusted LAN, no authentication",
            args.db
        );
        let usb = args.usb || (!args.mock && args.replay.is_none() && args.port.is_none());
        supervise(
            || args.source(),
            &args.db,
            usb,
            &live,
            &instance,
            || stopping.load(Ordering::Relaxed) || server.is_finished(),
        )
        .await;
        // Bound shutdown even when a remote peer stops reading a download.
        match tokio::time::timeout(Duration::from_secs(3), server).await {
            Ok(result) => {
                result??;
            }
            Err(_) => eprintln!("Closing remaining client connections"),
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::{
            tests::{packet, status},
            Calibration, Decoder,
        },
        source::SessionInfo,
        storage::Store,
    };
    struct Disconnected;
    impl crate::source::DataSource for Disconnected {
        fn run(self: Box<Self>, sink: &crate::source::Sink) -> Result<()> {
            sink.ready("synthetic".into(), "usb-cc", status())?;
            let mut decoder = Decoder::new(Calibration::parse(&status())?, "synthetic".into());
            sink.batch(decoder.decode(packet(1), chrono::Utc::now())?)?;
            anyhow::bail!("synthetic USB disconnect")
        }
    }
    #[test]
    fn supervisor_reconnects_source_but_never_retries_storage_failure() {
        use std::sync::atomic::AtomicUsize;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("reconnect.db");
        let calls = AtomicUsize::new(0);
        let live = Mutex::new(Live::snapshot(&Shared::default(), "test", 0.0));
        let executor = tokio::runtime::Runtime::new().unwrap();
        let start = Instant::now();
        executor.block_on(supervise(
            || {
                if calls.fetch_add(1, Ordering::Relaxed) == 0 {
                    Box::new(Disconnected)
                } else {
                    Box::new(crate::source::mock::MockSource)
                }
            },
            db.to_str().unwrap(),
            true,
            &live,
            "test",
            || start.elapsed() > Duration::from_secs(3),
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        let conn = read_database(&db).unwrap();
        let sessions = conn
            .prepare("SELECT id,outcome,accepted_count,saved_count FROM sessions ORDER BY id")
            .unwrap()
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, u64>(2)?,
                    r.get::<_, u64>(3)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0], (1, "incomplete".into(), 800, 800));
        assert_eq!(sessions[1].1, "complete");
        assert!(sessions[1].2 > 0);
        assert_eq!(sessions[1].2, sessions[1].3);
        calls.store(0, Ordering::Relaxed);
        let start = Instant::now();
        executor.block_on(supervise(
            || {
                calls.fetch_add(1, Ordering::Relaxed);
                Box::new(crate::source::mock::MockSource)
            },
            dir.path().to_str().unwrap(),
            true,
            &live,
            "test",
            || start.elapsed() > Duration::from_secs(3),
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(live
            .lock()
            .unwrap()
            .error
            .as_ref()
            .unwrap()
            .contains("database"));
    }
    #[test]
    fn exports_only_selected_session_with_frames_calibration_and_partial_marker() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("source.db");
        let out = dir.path().join("export.db");
        let info = SessionInfo {
            device: "synthetic".into(),
            transport: "usb-cc",
            calibration: status(),
            received: chrono::Utc::now(),
        };
        let mut first = Store::open(db.to_str().unwrap(), &info).unwrap();
        let mut decoder = Decoder::new(Calibration::parse(&status()).unwrap(), info.device.clone());
        first
            .insert_batches(&[decoder.decode(packet(1), chrono::Utc::now()).unwrap()])
            .unwrap();
        first
            .finish(&Shared {
                accepted: 800,
                saved: 800,
                received: 800,
                ..Shared::default()
            })
            .unwrap();
        let mut active = Store::open(db.to_str().unwrap(), &info).unwrap();
        active
            .insert_batches(&[decoder.decode(packet(2), chrono::Utc::now()).unwrap()])
            .unwrap();
        let stop = AtomicBool::new(false);
        export_session(&db, &out, active.session_id(), &stop).unwrap();
        let conn = read_database(&out).unwrap();
        let counts:(u64,u64,u64)=conn.query_row("SELECT (SELECT count(*) FROM sessions),(SELECT count(*) FROM frames),(SELECT count(*) FROM measurements)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(counts, (1, 1, 800));
        assert_eq!(
            conn.query_row("SELECT calibration FROM sessions", [], |r| r
                .get::<_, Vec<u8>>(0))
                .unwrap(),
            status()
        );
        assert!(conn
            .query_row("SELECT partial FROM export_metadata", [], |r| r
                .get::<_, bool>(0))
            .unwrap());
        assert_eq!(
            conn.query_row("SELECT saved_count FROM sessions", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            800
        );
        assert!(conn
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query([])
            .unwrap()
            .next()
            .unwrap()
            .is_none());
        active
            .insert_batches(&[decoder.decode(packet(3), chrono::Utc::now()).unwrap()])
            .unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM measurements", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            800
        );
        let original = read_database(&db).unwrap();
        assert_eq!(
            original
                .query_row("SELECT count(*) FROM measurements", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            2400
        );
        let completed = dir.path().join("completed.db");
        export_session(&db, &completed, first.session_id(), &stop).unwrap();
        assert!(!read_database(&completed)
            .unwrap()
            .query_row("SELECT partial FROM export_metadata", [], |r| r
                .get::<_, bool>(0))
            .unwrap());
        assert!(export_session(&db, &dir.path().join("missing.db"), 99, &stop).is_err());
        stop.store(true, Ordering::Relaxed);
        assert!(export_session(&db, &dir.path().join("cancelled.db"), 1, &stop).is_err());
    }
    #[test]
    fn live_dto_omits_raw_and_roundtrips_chart() {
        let mut s = Shared::default();
        let m = crate::domain::Measurement {
            timestamp: chrono::Utc::now(),
            device_id: "test".into(),
            voltage_v: 5.0,
            current_a: -0.1,
            power_w: -0.5,
            energy_wh: 0.0,
            status: "test".into(),
            raw: vec![42; 3212],
        };
        s.history.observe(&m);
        s.metrics.observe(m);
        let dto = Live::snapshot(&s, "test-instance", 10000.0);
        let bytes = serde_json::to_vec(&dto).unwrap();
        let json = String::from_utf8(bytes.clone()).unwrap();
        assert!(!json.contains("raw"));
        assert!(bytes.len() < 2048);
        let restored: Live = serde_json::from_slice(&bytes).unwrap();
        let state = restored.shared();
        assert_eq!(state.history.series(1, 30, 80)[0].min[0].1, -0.1);
    }
}
