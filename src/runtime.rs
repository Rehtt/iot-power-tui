use crate::{
    domain::Metrics,
    source::{Batch, DataSource, Message, Sink},
    storage::Store,
};
use anyhow::{Context, Result};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{sync_channel, Receiver, RecvTimeoutError},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum State {
    #[default]
    Connecting,
    Calibrating,
    Capturing,
    Stopped,
    Fault,
}
#[derive(Clone, Default)]
pub struct Shared {
    pub state: State,
    pub session_id: Option<i64>,
    pub storage_fault: bool,
    pub metrics: Metrics,
    pub history: crate::history::History,
    pub device: Option<String>,
    pub received: u64,
    pub accepted: u64,
    pub saved: u64,
    pub gaps: u64,
    pub invalid: u64,
    pub dropped: u64,
    pub discarded_bytes: u64,
    pub error: Option<String>,
}
fn fault(shared: &Arc<Mutex<Shared>>, cancel: &AtomicBool, error: String) {
    let mut s = shared.lock().unwrap();
    s.state = State::Fault;
    if let Some(previous) = &mut s.error {
        previous.push_str("; ");
        previous.push_str(&error);
    } else {
        s.error = Some(error);
    }
    cancel.store(true, Ordering::Relaxed);
}
pub struct Runtime {
    pub shared: Arc<Mutex<Shared>>,
    cancel: Arc<AtomicBool>,
    acquisition: Option<JoinHandle<()>>,
    writer: Option<JoinHandle<()>>,
}
impl Runtime {
    pub fn start(source: Box<dyn DataSource>, db: String) -> Self {
        let shared = Arc::new(Mutex::new(Shared::default()));
        let cancel = Arc::new(AtomicBool::new(false));
        // 32 complete USB packets = 2.56 seconds. Overflow is a visible fatal error.
        let (tx, rx) = sync_channel(32);
        let source_shared = shared.clone();
        let source_cancel = cancel.clone();
        let acquisition = thread::spawn(move || {
            let sink = Sink {
                tx,
                cancel: source_cancel.clone(),
                shared: source_shared.clone(),
            };
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| source.run(&sink)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => fault(&source_shared, &source_cancel, format!("{e:#}")),
                Err(_) => fault(
                    &source_shared,
                    &source_cancel,
                    "acquisition thread panicked".into(),
                ),
            }
            // Publish source failures before disconnecting, so session finalization sees them.
            drop(sink);
        });
        let writer_shared = shared.clone();
        let writer_cancel = cancel.clone();
        let writer = thread::spawn(move || {
            if let Err(e) = write_loop(rx, &db, &writer_shared, &writer_cancel) {
                writer_shared.lock().unwrap().storage_fault = true;
                fault(&writer_shared, &writer_cancel, format!("database: {e:#}"));
            }
            let mut s = writer_shared.lock().unwrap();
            if s.error.is_none() {
                s.state = State::Stopped;
            }
        });
        Self {
            shared,
            cancel,
            acquisition: Some(acquisition),
            writer: Some(writer),
        }
    }
    pub fn stop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        for handle in [&mut self.acquisition, &mut self.writer] {
            if let Some(handle) = handle.take() {
                if handle.join().is_err() {
                    fault(&self.shared, &self.cancel, "worker thread panicked".into());
                }
            }
        }
    }
    pub fn reset_metrics(&self) {
        let mut s = self.shared.lock().unwrap();
        s.metrics = Metrics::default();
        s.history = crate::history::History::default();
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop();
    }
}
fn flush(
    store: &mut Option<Store>,
    pending: &mut Vec<Batch>,
    shared: &Arc<Mutex<Shared>>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let n = store
        .as_mut()
        .context("samples arrived before session initialization")?
        .insert_batches(pending)?;
    let mut s = shared.lock().unwrap();
    s.saved += n;
    for batch in pending.drain(..) {
        if batch.gaps > 0 {
            s.history.break_line();
        }
        let mut previous_index = None;
        for (index, m) in batch.samples {
            if previous_index.is_some_and(|p| index != p + 1)
                || (previous_index.is_none() && index > 0)
            {
                s.history.break_line();
            }
            previous_index = Some(index);
            s.history.observe(&m);
            s.metrics.observe(m);
        }
    }
    Ok(())
}
fn write_loop(
    rx: Receiver<Message>,
    db: &str,
    shared: &Arc<Mutex<Shared>>,
    cancel: &AtomicBool,
) -> Result<()> {
    let mut store = None;
    let mut pending = Vec::new();
    let mut last_flush = Instant::now();
    let result = (|| -> Result<()> {
        loop {
            match rx.recv_timeout(Duration::from_millis(250).saturating_sub(last_flush.elapsed())) {
                Ok(Message::Ready(info)) => {
                    anyhow::ensure!(store.is_none(), "duplicate session initialization");
                    let opened = Store::open(db, &info)?;
                    shared.lock().unwrap().session_id = Some(opened.session_id());
                    store = Some(opened);
                }
                Ok(Message::Batch(batch)) => pending.push(batch),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    flush(&mut store, &mut pending, shared)?;
                    break;
                }
            }
            if pending.len() >= 4 || last_flush.elapsed() >= Duration::from_millis(250) {
                flush(&mut store, &mut pending, shared)?;
                last_flush = Instant::now();
            }
        }
        Ok(())
    })();
    if let Err(e) = &result {
        shared.lock().unwrap().storage_fault = true;
        fault(shared, cancel, format!("database write failed: {e:#}"));
        // Wait for acquisition to observe cancellation; account for every accepted packet.
        // The failed transaction remains unsaved and the session is explicitly incomplete.
        while rx.recv().is_ok() {}
    }
    if let Some(store) = store {
        store
            .finish(&shared.lock().unwrap())
            .context("finalize session")?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{mock::MockSource, SessionInfo};
    #[test]
    fn stop_flushes_and_restart_creates_new_session() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_str().unwrap().to_owned();
        for _ in 0..2 {
            let mut runtime = Runtime::start(Box::new(MockSource), path.clone());
            let deadline = Instant::now() + Duration::from_secs(5);
            while runtime.shared.lock().unwrap().accepted < 2 && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            runtime.reset_metrics();
            runtime.stop();
            let s = runtime.shared.lock().unwrap();
            assert!(s.saved >= 2);
            assert_eq!(s.accepted, s.saved);
            assert_eq!(s.state, State::Stopped);
        }
        let conn = rusqlite::Connection::open(path).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM sessions WHERE outcome='complete' AND ended_at IS NOT NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            2
        );
    }
    #[test]
    fn failed_database_cancels_and_surfaces_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut runtime = Runtime::start(Box::new(MockSource), dir.path().to_str().unwrap().into());
        let deadline = Instant::now() + Duration::from_secs(5);
        while !runtime.cancel.load(Ordering::Relaxed) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        runtime.stop();
        assert_eq!(runtime.shared.lock().unwrap().state, State::Fault);
        assert!(runtime
            .shared
            .lock()
            .unwrap()
            .error
            .as_ref()
            .unwrap()
            .contains("database"));
    }
    #[test]
    fn bounded_queue_reports_backpressure() {
        let (tx, _rx) = sync_channel(1);
        let shared = Arc::new(Mutex::new(Shared::default()));
        let sink = Sink {
            tx,
            cancel: Arc::new(AtomicBool::new(false)),
            shared,
        };
        sink.tx
            .send(Message::Ready(SessionInfo {
                device: "test".into(),
                transport: "mock",
                calibration: vec![],
                received: chrono::Utc::now(),
            }))
            .unwrap();
        assert!(sink
            .ready("test".into(), "mock", vec![])
            .unwrap_err()
            .to_string()
            .contains("queue full"));
    }
    struct DisconnectSource;
    impl DataSource for DisconnectSource {
        fn run(self: Box<Self>, sink: &Sink) -> Result<()> {
            use crate::protocol::{
                tests::{packet, status},
                Calibration, Decoder,
            };
            sink.ready("synthetic".into(), "usb-cc", status())?;
            let mut decoder = Decoder::new(Calibration::parse(&status())?, "synthetic".into());
            for id in 0..3 {
                sink.batch(decoder.decode(packet(id), chrono::Utc::now())?)?;
            }
            anyhow::bail!("synthetic device disconnected")
        }
    }
    #[test]
    fn source_failure_drains_accepted_packets_and_marks_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disconnect.db");
        let mut runtime = Runtime::start(Box::new(DisconnectSource), path.to_str().unwrap().into());
        let deadline = Instant::now() + Duration::from_secs(5);
        while !runtime.cancel.load(Ordering::Relaxed) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        runtime.stop();
        let s = runtime.shared.lock().unwrap();
        assert_eq!(s.state, State::Fault);
        assert_eq!(s.accepted, 2400);
        assert_eq!(s.saved, 2400);
        let conn = rusqlite::Connection::open(path).unwrap();
        assert_eq!(
            conn.query_row("SELECT outcome FROM sessions", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "incomplete"
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM frames", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
    }
    #[test]
    fn writer_failure_rolls_back_and_records_unsaved_samples() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("write-failure.db");
        let info = SessionInfo {
            device: "synthetic".into(),
            transport: "mock",
            calibration: vec![],
            received: chrono::Utc::now(),
        };
        drop(Store::open(path.to_str().unwrap(), &info).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TRIGGER reject_sample BEFORE INSERT ON measurements BEGIN SELECT RAISE(FAIL,'synthetic disk failure'); END;").unwrap();
        let mut runtime = Runtime::start(Box::new(DisconnectSource), path.to_str().unwrap().into());
        let deadline = Instant::now() + Duration::from_secs(5);
        while !runtime.cancel.load(Ordering::Relaxed) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        runtime.stop();
        let s = runtime.shared.lock().unwrap();
        assert_eq!(s.accepted, 2400);
        assert_eq!(s.saved, 0);
        assert!(s.error.as_ref().unwrap().contains("synthetic disk failure"));
        assert_eq!(
            conn.query_row("SELECT count(*) FROM frames", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        let summary: (String, u64, u64) = conn
            .query_row(
                "SELECT outcome,accepted_count,saved_count FROM sessions WHERE id=2",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(summary, ("incomplete".into(), 2400, 0));
    }
    #[test]
    fn replay_larger_than_queue_is_lossless_and_reports_bad_json() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replay.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        for _ in 0..200 {
            writeln!(f,"{{\"timestamp\":\"2026-01-01T00:00:00Z\",\"device_id\":\"synthetic\",\"voltage_v\":5,\"current_a\":0.1,\"power_w\":0.5,\"energy_wh\":0,\"status\":\"test\",\"raw\":[]}}").unwrap();
        }
        writeln!(f, "not json").unwrap();
        drop(f);
        let db = dir.path().join("replay.db");
        let mut runtime = Runtime::start(
            Box::new(crate::source::replay::ReplaySource {
                path: path.to_str().unwrap().into(),
            }),
            db.to_str().unwrap().into(),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !runtime.cancel.load(Ordering::Relaxed) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        runtime.stop();
        let s = runtime.shared.lock().unwrap();
        assert_eq!(s.saved, 200);
        assert_eq!(s.dropped, 0);
        assert!(s.error.as_ref().unwrap().contains("decode replay JSONL"));
    }
}
