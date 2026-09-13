//! Versioned LAN API with optimistic configuration control and pinned exports.
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
    #[serde(default)]
    pub saved_source: u64,
    #[serde(default)]
    pub records: u64,
    #[serde(default)]
    pub buffered_bytes: usize,
    #[serde(default)]
    pub writing_bytes: usize,
    #[serde(default)]
    pub config: crate::recording::Config,
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
            saved_source: s.saved_source,
            records: s.records,
            buffered_bytes: s.buffered_bytes,
            writing_bytes: s.writing_bytes,
            config: s.config,
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
            saved_source: self.saved_source,
            records: self.records,
            buffered_bytes: self.buffered_bytes,
            writing_bytes: self.writing_bytes,
            config: self.config,
            gaps: self.gaps,
            invalid: self.invalid,
            dropped: self.dropped,
            error: self.error.clone(),
            history: Arc::new(crate::history::History::from_buckets(self.buckets.clone())),
            ..Shared::default()
        };
        s.metrics = Arc::new(crate::domain::Metrics {
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
        });
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
    #[serde(default)]
    pub name: String,
}
#[derive(Default, Deserialize)]
struct Page {
    before: Option<i64>,
}
#[derive(Clone)]
struct Api {
    live: Arc<Mutex<Arc<Live>>>,
    db: PathBuf,
    export: Arc<tokio::sync::Semaphore>,
    config: Arc<Mutex<ConfigState>>,
    barrier: ActiveBarrier,
    rotations: Rotations,
}
type ApiError = (StatusCode, String);
fn internal(e: impl std::fmt::Display) -> ApiError {
    eprintln!("API: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "database operation failed".into(),
    )
}
// Fixed-width chart values avoid thousands of float-to-decimal conversions per
// request on ARMv6. The JSON representation remains the default public API.
const LIVE_MEDIA_TYPE: &str = "application/vnd.iot-power.live-v1";
const LIVE_MAGIC: &[u8; 8] = b"IPLIVE01";
fn encode_live(live: &Live) -> Result<Vec<u8>> {
    let mut header = live.clone();
    header.buckets.clear();
    let json = serde_json::to_vec(&header)?;
    anyhow::ensure!(
        json.len() <= 16384 && live.buckets.len() <= 600,
        "live snapshot exceeds bounds"
    );
    let mut bytes = Vec::with_capacity(14 + json.len() + live.buckets.len() * 112);
    bytes.extend_from_slice(LIVE_MAGIC);
    bytes.extend_from_slice(&(json.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(live.buckets.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&json);
    for bucket in &live.buckets {
        bytes.extend_from_slice(&bucket.time.to_le_bytes());
        bytes.extend_from_slice(&bucket.segment.to_le_bytes());
        for value in bucket.values {
            bytes.extend_from_slice(&value.count.to_le_bytes());
            for number in [value.mean, value.min, value.max] {
                bytes.extend_from_slice(&number.to_le_bytes());
            }
        }
    }
    Ok(bytes)
}
pub fn decode_live(bytes: &[u8]) -> Result<Live> {
    if !bytes.starts_with(LIVE_MAGIC) {
        return Ok(serde_json::from_slice(bytes)?);
    }
    anyhow::ensure!(bytes.len() >= 14, "truncated live header");
    let json_len = u32::from_le_bytes(bytes[8..12].try_into()?) as usize;
    let count = u16::from_le_bytes(bytes[12..14].try_into()?) as usize;
    anyhow::ensure!(
        json_len <= 16384 && count <= 600 && bytes.len() == 14 + json_len + count * 112,
        "invalid live lengths"
    );
    let mut live: Live = serde_json::from_slice(&bytes[14..14 + json_len])?;
    anyhow::ensure!(live.buckets.is_empty(), "duplicate live buckets");
    let mut words = bytes[14 + json_len..].chunks_exact(8);
    let mut word = || {
        u64::from_le_bytes(
            words
                .next()
                .expect("validated live length")
                .try_into()
                .unwrap(),
        )
    };
    for _ in 0..count {
        let time = word() as i64;
        let segment = word();
        let values = std::array::from_fn(|_| crate::history::Stats {
            count: word(),
            mean: f64::from_bits(word()),
            min: f64::from_bits(word()),
            max: f64::from_bits(word()),
        });
        anyhow::ensure!(
            values.iter().all(|v| v.count > 0
                && [v.mean, v.min, v.max].iter().all(|n| n.is_finite())
                && v.min <= v.max),
            "invalid live statistics"
        );
        live.buckets.push(Bucket {
            time,
            segment,
            values,
        });
    }
    Ok(live)
}

async fn status(WebState(api): WebState<Api>, headers: axum::http::HeaderMap) -> Response {
    let snapshot = api.live.lock().unwrap().clone();
    if headers.get(header::ACCEPT).and_then(|v| v.to_str().ok()) == Some(LIVE_MEDIA_TYPE) {
        return match encode_live(&snapshot) {
            Ok(bytes) => ([(header::CONTENT_TYPE, LIVE_MEDIA_TYPE)], bytes).into_response(),
            Err(e) => internal(e).into_response(),
        };
    }
    Json(snapshot.as_ref()).into_response()
}
async fn sessions(
    WebState(api): WebState<Api>,
    Query(page): Query<Page>,
) -> Result<Json<Vec<Session>>, ApiError> {
    tokio::task::spawn_blocking(move || -> Result<_,anyhow::Error> {
        if !api.db.exists() { return Ok(Json(Vec::new())); }
        let conn = read_database(&api.db)?;
        let mut stmt=conn.prepare("SELECT id,device_id,started_at,ended_at,saved_count,outcome,name FROM sessions WHERE id<?1 ORDER BY id DESC LIMIT 50")?;
        let rows=stmt.query_map([page.before.unwrap_or(i64::MAX)],|r|Ok(Session{id:r.get(0)?,device:r.get(1)?,started_at:r.get(2)?,ended_at:r.get(3)?,saved:r.get(4)?,outcome:r.get(5)?,name:r.get::<_,Option<String>>(6)?.unwrap_or_default()}))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(Json(rows))
    }).await.map_err(internal)?.map_err(internal)
}
#[derive(Deserialize)]
struct RotateRequest { action: String, name: Option<String> }
#[derive(Serialize)]
struct RotateResponse { old_session_id: i64, new_session_id: Option<i64>, name: String, state: String }
struct RotateJob {
    id: i64,
    save: bool,
    name: String,
    reply: tokio::sync::oneshot::Sender<Result<Json<RotateResponse>, ApiError>>,
}
type Rotations = Arc<Mutex<Option<RotateJob>>>;
async fn rotate(WebState(api): WebState<Api>, Path(id): Path<i64>, Json(req): Json<RotateRequest>) -> Result<Json<RotateResponse>, ApiError> {
    let save = match req.action.as_str() {
        "save" => true, "discard" => false,
        _ => return Err((StatusCode::BAD_REQUEST, "invalid action".into())),
    };
    let name = crate::storage::validate_session_name(&req.name.unwrap_or_default())
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let (reply, received) = tokio::sync::oneshot::channel();
    {
        let mut config = api.config.lock().unwrap();
        if config.state == "applying" || config.state == "rotating" || api.live.lock().unwrap().session_id != Some(id) {
            return Err((StatusCode::CONFLICT, "session changed or operation in progress".into()));
        }
        config.state = "rotating".into();
        *api.rotations.lock().unwrap() = Some(RotateJob { id, save, name, reply });
    }
    received.await.map_err(internal)?
}
pub fn read_database(path: &std::path::Path) -> Result<rusqlite::Connection> {
    // URI mode applies read-only to the source alone; attached export databases
    // can still be writable. Encode metacharacters so paths cannot add URI options.
    use std::fmt::Write;
    let mut uri = String::from("file:");
    for byte in path.to_str().context("non-UTF8 database path")?.bytes() {
        if byte.is_ascii_alphanumeric() || b"/.-_~".contains(&byte) {
            uri.push(byte as char);
        } else {
            write!(uri, "%{byte:02X}").unwrap();
        }
    }
    uri.push_str("?mode=ro");
    let conn = rusqlite::Connection::open_with_flags(
        uri,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )?;
    conn.busy_timeout(Duration::from_secs(2))?;
    Ok(conn)
}
/// Copy bounded batches from one consistent snapshot, preserving all row references.
#[cfg(test)]
pub fn export_session(
    source: &std::path::Path,
    target: &std::path::Path,
    id: i64,
    cancel: &AtomicBool,
) -> Result<()> {
    let source = read_database(source)?;
    source.execute_batch("BEGIN")?;
    export_snapshot(source, target, id, cancel, || false)
}
fn export_snapshot(
    snapshot: rusqlite::Connection,
    target: &std::path::Path,
    id: i64,
    cancel: &AtomicBool,
    should_pause: impl Fn() -> bool,
) -> Result<()> {
    let ended: Option<String> = snapshot
        .query_row("SELECT ended_at FROM sessions WHERE id=?1", [id], |r| {
            r.get(0)
        })
        .context("session not found")?;
    // Initialize the destination separately, then let SQLite stream rows directly
    // from the pinned source transaction. Avoid converting every value through
    // Rust/FFI on the single-core Zero W.
    let mut dest = rusqlite::Connection::open(target)?;
    let tx = dest.transaction()?;
    crate::storage::initialize_schema(&tx)?;
    tx.execute_batch("CREATE TABLE export_metadata(exported_at TEXT NOT NULL,source_session_id INTEGER NOT NULL,partial INTEGER NOT NULL);")?;
    tx.commit()?;
    drop(dest);
    snapshot.execute(
        "ATTACH DATABASE ?1 AS exported",
        [target.to_str().context("non-UTF8 export path")?],
    )?;
    for table in ["sessions", "frames", "measurements"] {
        let key = if table == "sessions" {
            "id"
        } else {
            "session_id"
        };
        let columns = snapshot
            .prepare(&format!("SELECT * FROM main.{table} LIMIT 0"))?
            .column_names()
            .join(",");
        let mut last = 0i64;
        loop {
            // Small copy steps yield between capacity writes without starving
            // downloads when the filling buffer stays close to its threshold.
            while should_pause() {
                anyhow::ensure!(!cancel.load(Ordering::Relaxed), "export cancelled");
                std::thread::sleep(Duration::from_millis(20));
            }
            anyhow::ensure!(!cancel.load(Ordering::Relaxed), "export cancelled");
            let end: Option<i64> = snapshot.query_row(&format!(
                "SELECT max(id) FROM (SELECT id FROM main.{table} WHERE {key}=?1 AND id>?2 ORDER BY id LIMIT 256)"
            ), rusqlite::params![id,last], |r|r.get(0))?;
            let Some(end) = end else { break };
            snapshot.execute(&format!(
                "INSERT INTO exported.{table}({columns}) SELECT {columns} FROM main.{table} WHERE {key}=?1 AND id>?2 AND id<=?3 ORDER BY id"
            ), rusqlite::params![id,last,end])?;
            last = end;
        }
    }
    snapshot.execute(
        "INSERT INTO exported.export_metadata VALUES (?1,?2,?3)",
        rusqlite::params![chrono::Utc::now().to_rfc3339(), id, ended.is_none()],
    )?;
    snapshot.execute_batch("COMMIT")?;
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
    let (reply, completed) = tokio::sync::oneshot::channel();
    // Linux niceness is per-thread. Use a dedicated thread so lowering export
    // priority cannot leak into unrelated work on the Tokio blocking pool.
    std::thread::Builder::new()
        .name("session-export".into())
        .spawn(move || {
            let result = (|| -> Result<_> {
                #[cfg(target_os = "linux")]
                {
                    // SAFETY: changes only this thread's scheduling priority, with no pointers.
                    let rc = unsafe {
                        let current = libc::getpriority(libc::PRIO_PROCESS, 0);
                        libc::setpriority(libc::PRIO_PROCESS, 0, current.max(10))
                    };
                    anyhow::ensure!(
                        rc == 0,
                        "lower export priority: {}",
                        std::io::Error::last_os_error()
                    );
                }
                let dir = tempfile::Builder::new()
                    .prefix("iot-power-export-")
                    .tempdir()?;
                let path = dir.path().join("session.db");
                let active = api
                    .barrier
                    .lock()
                    .unwrap()
                    .clone()
                    .filter(|(session, _)| *session == id);
                let should_pause = || {
                    let live = api.live.lock().unwrap();
                    live.state == State::Capturing && live.writing_bytes > 0
                };
                if let Some((_, barrier)) = active {
                    let snapshot = barrier.snapshot()?;
                    export_snapshot(snapshot, &path, id, &cancel, should_pause)?;
                } else {
                    let live = api.live.lock().unwrap();
                    anyhow::ensure!(
                        live.session_id != Some(id)
                            || (matches!(live.state, State::Stopped | State::Fault)
                                && live.buffered_bytes == 0
                                && live.accepted == live.saved_source),
                        "session is draining or has unsaved data; retry download after flush"
                    );
                    drop(live);
                    let snapshot = read_database(&api.db)?;
                    snapshot.execute_batch("BEGIN")?;
                    export_snapshot(snapshot, &path, id, &cancel, should_pause)?;
                }
                Ok((std::fs::File::open(path)?, dir, permit))
            })();
            let _ = reply.send(result);
        })
        .map_err(internal)?;
    let (file, dir, permit) = completed.await.map_err(internal)?.map_err(internal)?;
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigState {
    #[serde(flatten)]
    pub config: crate::recording::Config,
    pub revision: u64,
    pub state: String,
    pub pending: Option<crate::recording::Config>,
    pub error: Option<String>,
}
impl ConfigState {
    pub fn new(config: crate::recording::Config) -> Self {
        Self {
            config,
            revision: 0,
            state: "idle".into(),
            pending: None,
            error: None,
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigRequest {
    pub buffer_size_bytes: usize,
    pub sample_rate_hz: u32,
    pub revision: u64,
}
async fn get_config(WebState(api): WebState<Api>) -> Json<ConfigState> {
    Json(api.config.lock().unwrap().clone())
}
fn update_config(state: &mut ConfigState, request: ConfigRequest) -> Result<bool, ApiError> {
    let config = crate::recording::Config {
        buffer_size_bytes: request.buffer_size_bytes,
        sample_rate_hz: request.sample_rate_hz,
    }
    .validate()
    .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    if request.revision != state.revision || matches!(state.state.as_str(), "applying" | "rotating") {
        return Err((
            StatusCode::CONFLICT,
            "configuration changed or an operation is already running".into(),
        ));
    }
    if config == state.config && state.state == "idle" {
        return Ok(false);
    }
    state.pending = Some(config);
    state.state = "applying".into();
    state.error = None;
    Ok(true)
}
async fn put_config(
    WebState(api): WebState<Api>,
    Json(request): Json<ConfigRequest>,
) -> Result<(StatusCode, Json<ConfigState>), ApiError> {
    let mut state = api.config.lock().unwrap();
    let changed = update_config(&mut state, request)?;
    Ok((
        if changed {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        },
        Json(state.clone()),
    ))
}
type CaptureOperation = (std::thread::JoinHandle<(Runtime, Result<()>)>, bool);
type ActiveBarrier = Arc<Mutex<Option<(i64, crate::runtime::Barrier)>>>;
#[cfg(test)]
async fn supervise(
    source: impl FnMut() -> Box<dyn crate::source::DataSource>,
    db: &str,
    usb: bool,
    live: &Mutex<Arc<Live>>,
    instance: &str,
    stop: impl Fn() -> bool,
) {
    supervise_control(
        source,
        db,
        usb,
        live,
        instance,
        stop,
        Arc::new(Mutex::new(ConfigState::new(
            crate::recording::Config::default(),
        ))),
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(None)),
    )
    .await;
}
#[allow(clippy::too_many_arguments)]
async fn supervise_control(
    mut source: impl FnMut() -> Box<dyn crate::source::DataSource>,
    db: &str,
    usb: bool,
    live: &Mutex<Arc<Live>>,
    instance: &str,
    should_stop: impl Fn() -> bool,
    control: Arc<Mutex<ConfigState>>,
    barrier: ActiveBarrier,
    rotations: Rotations,
) {
    let mut config = control.lock().unwrap().config;
    let mut capture = Some(Runtime::start_config(source(), db.into(), config));
    let mut shared = capture.as_ref().unwrap().shared.clone();
    let mut operation: Option<CaptureOperation> = None;
    let mut rotation: Option<RotateJob> = None;
    let mut rotation_waiting = false;
    let mut finalized = false;
    let mut retry = 2u64;
    let mut retry_at = None;
    let mut rate_at = Instant::now();
    let mut rate_count = 0;
    let mut rate = 0.0;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while !should_stop() {
        tick.tick().await;
        if operation.as_ref().is_some_and(|(h, _)| h.is_finished()) {
            let (h, applying) = operation.take().unwrap();
            let (old, result) = h.join().expect("capture operation panicked");
            let summary = old.shared.lock().unwrap().clone();
            capture = Some(old);
            finalized = true;
            if rotation.is_some() {
                match result {
                    Ok(()) => {
                        if let Some(job) = rotation.as_mut() {
                            if job.save {
                                if let Ok(conn) = read_database(std::path::Path::new(db)) {
                                    if let Ok(name) = conn.query_row("SELECT name FROM sessions WHERE id=?1", [job.id], |r| r.get::<_,String>(0)) { job.name = name; }
                                }
                            }
                        }
                        retry_at = Some(Instant::now()); rotation_waiting = true;
                    }
                    Err(e) => {
                        let mut c = control.lock().unwrap();
                        c.state = "failed".into();
                        c.error = Some(format!("{e:#}"));
                        let job = rotation.take().unwrap();
                        let _ = job.reply.send(Err(internal(e)));
                    }
                }
            } else if applying {
                let mut c = control.lock().unwrap();
                match result {
                    Ok(()) => {
                        config = c.pending.take().unwrap();
                        c.config = config;
                        c.revision += 1;
                        c.state = "idle".into();
                        c.error = None;
                        retry_at = Some(Instant::now());
                    }
                    Err(e) => {
                        c.state = "failed".into();
                        c.error = Some(format!("{e:#}"));
                    }
                }
            } else {
                eprintln!(
                    "Session {:?}: accepted={} saved={} represented={} error={:?}",
                    summary.session_id,
                    summary.accepted,
                    summary.saved,
                    summary.saved_source,
                    summary.error
                );
                if summary.storage_fault {
                    let mut c = control.lock().unwrap();
                    if c.state != "applying" {
                        c.state = "failed".into();
                        c.error = summary.error.clone();
                    }
                } else if usb {
                    retry_at = Some(Instant::now() + Duration::from_secs(retry));
                    retry = (retry * 2).min(30);
                }
            }
        }
        if retry_at.is_some_and(|t| Instant::now() >= t) && operation.is_none() {
            capture = Some(Runtime::start_config(source(), db.into(), config));
            shared = capture.as_ref().unwrap().shared.clone();
            finalized = false;
            retry_at = None;
            rate_count = 0;
            rate_at = Instant::now();
        }
        let s = shared.lock().unwrap().clone();
        if rotation_waiting && rotation.as_ref().is_some_and(|job| s.session_id != Some(job.id) || s.state == State::Fault) {
            let job = rotation.take().unwrap();
            rotation_waiting = false;
            control.lock().unwrap().state = "idle".into();
            let _ = job.reply.send(Ok(Json(RotateResponse { old_session_id: job.id, new_session_id: s.session_id, name: job.name, state: format!("{:?}", s.state) })));
        }
        if rate_at.elapsed() >= Duration::from_secs(1) {
            rate = s.received.saturating_sub(rate_count) as f64 / rate_at.elapsed().as_secs_f64();
            rate_count = s.received;
            rate_at = Instant::now();
        }
        *live.lock().unwrap() = Arc::new(Live::snapshot(&s, instance, rate));
        *barrier.lock().unwrap() = if s.state == State::Capturing {
            capture
                .as_ref()
                .and_then(|r| s.session_id.map(|id| (id, r.barrier())))
        } else {
            None
        };
        if s.state == State::Capturing && s.saved > 0 {
            retry = 2;
        }
        if operation.is_none() && rotation.is_none() {
            rotation = rotations.lock().unwrap().take();
        }
        let rotating = rotation.is_some() && !rotation_waiting;
        let applying = control.lock().unwrap().state == "applying";
        if operation.is_none()
            && (rotating || applying || (!finalized && matches!(s.state, State::Fault | State::Stopped)))
        {
            retry_at = None;
            *barrier.lock().unwrap() = None;
            let mut old = capture.take().unwrap();
            let rotate_data = rotation.as_ref().map(|job| (job.id, job.save, job.name.clone()));
            let database = db.to_string();
            operation = Some((
                std::thread::spawn(move || {
                    old.stop();
                    let result = (|| -> Result<()> {
                        if applying || rotating { old.ensure_saved()?; }
                        if let Some((id, save, name)) = rotate_data {
                            if save { crate::storage::update_session_name(std::path::Path::new(&database), id, &name)?; }
                            else { crate::storage::delete_session(std::path::Path::new(&database), id)?; }
                        }
                        Ok(())
                    })();
                    (old, result)
                }),
                applying,
            ));
        }
    }
    *barrier.lock().unwrap() = None;
    if let Some((h, _)) = operation.take() {
        capture = Some(
            tokio::task::spawn_blocking(move || h.join().expect("capture operation panicked").0)
                .await
                .unwrap(),
        );
    }
    if let Some(mut old) = capture.take() {
        tokio::task::spawn_blocking(move || {
            while let Err(e) = old.ensure_saved() {
                eprintln!(
                    "Shutdown flush failed; retaining memory buffers and retrying in 2s: {e:#}"
                );
                std::thread::sleep(Duration::from_secs(2));
            }
            let s = old.shared.lock().unwrap();
            eprintln!(
                "Service stopped: accepted={} saved={} represented={}",
                s.accepted, s.saved, s.saved_source
            );
        })
        .await
        .unwrap();
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
        let live = Arc::new(Mutex::new(Arc::new(Live::snapshot(
            &Shared::default(),
            &instance,
            0.0,
        ))));
        let control = Arc::new(Mutex::new(ConfigState::new(args.config())));
        let barrier = Arc::new(Mutex::new(None));
        let rotations = Arc::new(Mutex::new(None));
        let api = Api {
            rotations: rotations.clone(),
            config: control.clone(),
            barrier: barrier.clone(),
            live: live.clone(),
            db: PathBuf::from(&args.db),
            export: Arc::new(tokio::sync::Semaphore::new(1)),
        };
        let app = Router::new()
            .route("/api/v1/config", get(get_config).put(put_config))
            .route("/api/v1/status", get(status))
            .route("/api/v1/sessions", get(sessions))
            .route("/api/v1/sessions/{id}/rotate", axum::routing::post(rotate))
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
        supervise_control(
            || args.source(),
            &args.db,
            usb,
            &live,
            &instance,
            || stopping.load(Ordering::Relaxed) || server.is_finished(),
            control,
            barrier,
            rotations,
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
    #[test]
    fn compact_live_is_lossless_bounded_and_accepts_legacy_json() {
        let mut shared = Shared::default();
        for (_, m) in crate::recording::tests::batch(0, 800).samples {
            Arc::make_mut(&mut shared.history).observe(&m);
            Arc::make_mut(&mut shared.metrics).observe(m);
        }
        let live = Live::snapshot(&shared, "test", 10000.0);
        let bytes = encode_live(&live).unwrap();
        let expected = serde_json::to_value(&live).unwrap();
        assert_eq!(
            serde_json::to_value(decode_live(&bytes).unwrap()).unwrap(),
            expected
        );
        assert_eq!(
            serde_json::to_value(decode_live(&serde_json::to_vec(&live).unwrap()).unwrap())
                .unwrap(),
            expected
        );
        for cut in [8, 13, bytes.len() - 1] {
            assert!(decode_live(&bytes[..cut]).is_err());
        }
        let mut invalid = bytes.clone();
        invalid[12..14].copy_from_slice(&601u16.to_le_bytes());
        assert!(decode_live(&invalid).is_err());
    }
    #[test]
    fn export_reader_keeps_source_readonly_and_escapes_uri_metacharacters() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("capture?#%.db");
        drop(crate::storage::open_database(path.to_str().unwrap()).unwrap());
        let conn = read_database(&path).unwrap();
        assert!(conn.execute("DELETE FROM sessions", []).is_err());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM sessions", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    #[test]
    fn config_revision_conflicts_validation_and_idempotency() {
        let mut state = ConfigState::new(crate::recording::Config::default());
        let request = |rate, revision| ConfigRequest {
            buffer_size_bytes: 10_000_000,
            sample_rate_hz: rate,
            revision,
        };
        assert!(!update_config(&mut state, request(10000, 0)).unwrap());
        assert_eq!(
            update_config(&mut state, request(1000, 1)).unwrap_err().0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            update_config(&mut state, request(0, 0)).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
        assert!(update_config(&mut state, request(333, 0)).unwrap());
        assert_eq!(state.pending.unwrap().sample_rate_hz, 333);
        assert_eq!(
            update_config(&mut state, request(1000, 0)).unwrap_err().0,
            StatusCode::CONFLICT
        );
    }

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
    fn configuration_requested_during_fault_finalization_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("blocked.db");
        std::fs::create_dir(&db).unwrap();
        let config = Arc::new(Mutex::new(ConfigState::new(
            crate::recording::Config::default(),
        )));
        let live = Mutex::new(Arc::new(Live::snapshot(&Shared::default(), "test", 0.0)));
        let requested = AtomicBool::new(false);
        let started = Instant::now();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(supervise_control(
                || Box::new(crate::source::mock::MockSource),
                db.to_str().unwrap(),
                false,
                &live,
                "test",
                || {
                    let current = live.lock().unwrap();
                    if current.state == State::Fault && !requested.swap(true, Ordering::Relaxed) {
                        std::fs::remove_dir(&db).unwrap();
                        assert!(update_config(
                            &mut config.lock().unwrap(),
                            ConfigRequest {
                                buffer_size_bytes: 10_000_000,
                                sample_rate_hz: 333,
                                revision: 0,
                            }
                        )
                        .unwrap());
                    }
                    (current.state == State::Capturing
                        && current.config.sample_rate_hz == 333
                        && current.accepted > 0)
                        || started.elapsed() > Duration::from_secs(5)
                },
                config.clone(),
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(None)),
            ));
        assert!(requested.load(Ordering::Relaxed));
        let state = config.lock().unwrap();
        assert_eq!(state.state, "idle");
        assert_eq!(state.revision, 1);
        assert_eq!(state.config.sample_rate_hz, 333);
    }
    #[test]
    fn supervisor_reconnects_source_but_never_retries_storage_failure() {
        use std::sync::atomic::AtomicUsize;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("reconnect.db");
        let calls = AtomicUsize::new(0);
        let live = Mutex::new(Arc::new(Live::snapshot(&Shared::default(), "test", 0.0)));
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
            || {
                let current = live.lock().unwrap();
                (current.session_id == Some(2) && current.accepted >= 2)
                    || start.elapsed() > Duration::from_secs(10)
            },
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
        let blocked = dir.path().join("blocked.db");
        std::fs::create_dir(&blocked).unwrap();
        let start = Instant::now();
        executor.block_on(supervise(
            || {
                calls.fetch_add(1, Ordering::Relaxed);
                Box::new(crate::source::mock::MockSource)
            },
            blocked.to_str().unwrap(),
            true,
            &live,
            "test",
            || {
                if start.elapsed() > Duration::from_secs(3) {
                    std::fs::remove_dir(&blocked).unwrap();
                    true
                } else {
                    false
                }
            },
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
            name: None,
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
        Arc::make_mut(&mut s.history).observe(&m);
        Arc::make_mut(&mut s.metrics).observe(m);
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
