use crate::{
    domain::Metrics,
    recording::{Config, RecordedBatch, Resampler},
    source::{DataSource, Message, SessionInfo, Sink},
    storage::Store,
};
use anyhow::{ensure, Context, Result};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, sync_channel, Receiver, RecvTimeoutError},
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
    pub metrics: Arc<Metrics>,
    pub history: Arc<crate::history::History>,
    pub display_generation: u64,
    pub device: Option<String>,
    pub received: u64,
    pub accepted: u64,
    pub saved: u64,
    pub saved_source: u64,
    pub records: u64,
    pub buffered_bytes: usize,
    pub writing_bytes: usize,
    pub committed_bytes: u64,
    pub duration_secs: f64,
    pub config: Config,
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
        if !previous.contains(&error) {
            previous.push_str("; ");
            previous.push_str(&error);
        }
    } else {
        s.error = Some(error);
    }
    cancel.store(true, Ordering::Relaxed);
}
pub type SnapshotReply = mpsc::SyncSender<std::result::Result<rusqlite::Connection, String>>;
#[derive(Clone)]
pub struct Barrier {
    tx: mpsc::SyncSender<Message>,
}
impl Barrier {
    pub fn snapshot(&self) -> Result<rusqlite::Connection> {
        let (tx, rx) = sync_channel(1);
        self.tx
            .send(Message::Snapshot(tx))
            .context("capture is no longer active")?;
        rx.recv()
            .context("snapshot worker stopped")?
            .map_err(anyhow::Error::msg)
    }
}
enum WriteJob {
    Ready(SessionInfo),
    Data(VecDeque<RecordedBatch>),
    Snapshot(SnapshotReply),
    End,
}
struct Recovery {
    store: Option<Store>,
    info: Option<SessionInfo>,
    pending: VecDeque<RecordedBatch>,
    db: String,
    config: Config,
}
impl Recovery {
    fn write(&mut self, shared: &Arc<Mutex<Shared>>) -> Result<()> {
        if self.store.is_none() {
            if let Some(info) = &self.info {
                let store = Store::open_config(&self.db, info, self.config)?;
                shared.lock().unwrap().session_id = Some(store.session_id());
                self.store = Some(store);
            }
        }
        while !self.pending.is_empty() {
            let count = self.pending.len().min(32);
            let (rows, samples) = self
                .store
                .as_mut()
                .context("samples arrived before session initialization")?
                .insert_recorded(&self.pending.make_contiguous()[..count])?;
            let bytes: usize = self.pending.drain(..count).map(|b| b.memory()).sum();
            let mut s = shared.lock().unwrap();
            s.saved += rows;
            s.saved_source += samples;
            s.committed_bytes = s.committed_bytes.saturating_add(bytes as u64);
            s.buffered_bytes = s.buffered_bytes.saturating_sub(bytes);
            s.writing_bytes = s.writing_bytes.saturating_sub(bytes);
        }
        Ok(())
    }
    fn finish(&self, shared: &Arc<Mutex<Shared>>) -> Result<()> {
        // Clone before doing any SQLite work: capture and rendering never wait for disk locks.
        let snapshot = shared.lock().unwrap().clone();
        if let Some(store) = &self.store {
            store.finish(&snapshot)?;
        }
        Ok(())
    }
}
pub struct Runtime {
    pub shared: Arc<Mutex<Shared>>,
    cancel: Arc<AtomicBool>,
    acquisition: Option<JoinHandle<()>>,
    processor: Option<JoinHandle<()>>,
    writer: Option<JoinHandle<Recovery>>,
    recovery: Option<Recovery>,
    barrier: Barrier,
}
impl Runtime {
    #[cfg(test)]
    pub fn start(source: Box<dyn DataSource>, db: String) -> Self {
        Self::start_config(source, db, Config::default())
    }
    pub fn start_config(source: Box<dyn DataSource>, db: String, config: Config) -> Self {
        let shared = Arc::new(Mutex::new(Shared {
            config,
            ..Shared::default()
        }));
        let cancel = Arc::new(AtomicBool::new(false));
        // Separate bounded ingress accounts for at most eight decoded USB packets.
        let (tx, rx) = sync_channel(8);
        let barrier = Barrier { tx: tx.clone() };
        let (jobs, job_rx) = sync_channel(4);
        let (state, stop) = (shared.clone(), cancel.clone());
        let writer = thread::spawn(move || {
            let mut recovery = Recovery {
                store: None,
                info: None,
                pending: VecDeque::new(),
                db,
                config,
            };
            let mut failed = false;
            while let Ok(job) = job_rx.recv() {
                let end = matches!(job, WriteJob::End);
                let result = (|| -> Result<()> {
                    match job {
                        WriteJob::Ready(info) => {
                            ensure!(recovery.info.is_none(), "duplicate session initialization");
                            recovery.info = Some(info);
                            recovery.write(&state)?;
                        }
                        WriteJob::Data(mut data) => {
                            recovery.pending.append(&mut data);
                            if !failed {
                                recovery.write(&state)?;
                            }
                        }
                        WriteJob::Snapshot(reply) => {
                            let snapshot = (|| -> Result<rusqlite::Connection> {
                                ensure!(!failed, "write failed; cannot export uncommitted capture");
                                let c = crate::network::read_database(std::path::Path::new(
                                    &recovery.db,
                                ))?;
                                c.execute_batch("BEGIN")?;
                                let _: i64 =
                                    c.query_row("SELECT count(*) FROM sessions", [], |r| r.get(0))?;
                                Ok(c)
                            })();
                            let _ = reply.send(snapshot.map_err(|e| format!("{e:#}")));
                        }
                        WriteJob::End => {
                            recovery.finish(&state)?;
                            let mut s = state.lock().unwrap();
                            if s.error.is_none() {
                                s.state = State::Stopped;
                            }
                        }
                    }
                    Ok(())
                })();
                if let Err(e) = result {
                    failed = true;
                    state.lock().unwrap().storage_fault = true;
                    fault(&state, &stop, format!("database: {e:#}"));
                }
                if end {
                    break;
                }
            }
            recovery
        });
        let (state, stop) = (shared.clone(), cancel.clone());
        let processor = thread::spawn(move || process(rx, jobs, state, stop, config));
        let (state, stop) = (shared.clone(), cancel.clone());
        let acquisition = thread::spawn(move || {
            let sink = Sink {
                tx: tx.clone(),
                cancel: stop.clone(),
                shared: state.clone(),
            };
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| source.run(&sink)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => fault(&state, &stop, format!("{e:#}")),
                Err(_) => fault(&state, &stop, "acquisition thread panicked".into()),
            }
            let _ = tx.send(Message::End);
        });
        Self {
            shared,
            cancel,
            acquisition: Some(acquisition),
            processor: Some(processor),
            writer: Some(writer),
            recovery: None,
            barrier,
        }
    }
    pub fn barrier(&self) -> Barrier {
        self.barrier.clone()
    }
    pub fn stop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        for handle in [&mut self.acquisition, &mut self.processor] {
            if let Some(h) = handle.take() {
                if h.join().is_err() {
                    fault(&self.shared, &self.cancel, "capture worker panicked".into());
                }
            }
        }
        if let Some(h) = self.writer.take() {
            match h.join() {
                Ok(r) => self.recovery = Some(r),
                Err(_) => fault(
                    &self.shared,
                    &self.cancel,
                    "database worker panicked".into(),
                ),
            }
        }
        let mut s = self.shared.lock().unwrap();
        if s.error.is_none() {
            s.state = State::Stopped;
        }
    }
    pub fn ensure_saved(&mut self) -> Result<()> {
        self.stop();
        if let Some(r) = &mut self.recovery {
            r.write(&self.shared)?;
            self.shared.lock().unwrap().storage_fault = false;
            r.finish(&self.shared)?;
        }
        let s = self.shared.lock().unwrap();
        ensure!(
            s.accepted == s.saved_source,
            "uncommitted capture: {} input samples remain",
            s.accepted.saturating_sub(s.saved_source)
        );
        Ok(())
    }
    pub fn reset_metrics(&self) {
        let mut state = self.shared.lock().unwrap();
        state.display_generation += 1;
        state.metrics = Arc::new(Metrics::default());
        state.history = Arc::new(crate::history::History::default());
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop();
    }
}
fn process(
    rx: Receiver<Message>,
    jobs: mpsc::SyncSender<WriteJob>,
    shared: Arc<Mutex<Shared>>,
    cancel: Arc<AtomicBool>,
    config: Config,
) {
    let mut metrics = Metrics::default();
    let mut history = crate::history::History::default();
    let mut generation = 0;
    let mut publish = Instant::now();
    let mut sampler = Resampler::new(config.sample_rate_hz);
    let mut pending = VecDeque::new();
    let mut bytes = 0usize;
    let mut largest_block = 0usize;
    let mut capture_started: Option<Instant> = None;
    let add = |b: RecordedBatch, pending: &mut VecDeque<RecordedBatch>, bytes: &mut usize| {
        if b.frame.is_none() && b.records.is_empty() {
            return;
        }
        let size = b.memory();
        let mut s = shared.lock().unwrap();
        s.records += b.records.len() as u64;
        s.buffered_bytes += size;
        *bytes += size;
        pending.push_back(b);
    };
    let seal = |pending: &mut VecDeque<RecordedBatch>, bytes: &mut usize| -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        shared.lock().unwrap().writing_bytes += *bytes;
        *bytes = 0;
        jobs.send(WriteJob::Data(std::mem::take(pending)))
            .context("database writer disconnected")?;
        Ok(())
    };
    loop {
        let current_generation = shared.lock().unwrap().display_generation;
        if generation != current_generation {
            metrics = Metrics::default();
            history = crate::history::History::default();
            generation = current_generation;
        }
        if capture_started.is_some() {
            let mut s = shared.lock().unwrap();
            if s.state == State::Capturing { s.duration_secs = capture_started.unwrap().elapsed().as_secs_f64(); }
        }
        let message = rx.recv_timeout(Duration::from_millis(20));
        let end = matches!(
            message,
            Ok(Message::End) | Err(RecvTimeoutError::Disconnected)
        );
        let result = (|| -> Result<()> {
            match message {
                Ok(Message::Ready(info)) => { capture_started = Some(Instant::now()); jobs
                    .send(WriteJob::Ready(info))
                    .context("database writer disconnected")?; }
                Ok(Message::Batch(batch)) => {
                    if shared.lock().unwrap().buffered_bytes
                        >= (config.buffer_size_bytes + largest_block) * 2
                        && !cancel.load(Ordering::Relaxed)
                    {
                        shared.lock().unwrap().storage_fault = true;
                        fault(
                            &shared,
                            &cancel,
                            "both recording buffers full; capture stopped (backpressure)".into(),
                        );
                    }
                    if batch.gaps > 0 {
                        history.break_line();
                    }
                    let mut previous = None;
                    for (index, m) in &batch.samples {
                        if previous.is_some_and(|p| *index != p + 1)
                            || (previous.is_none() && *index > 0)
                        {
                            history.break_line();
                        }
                        previous = Some(*index);
                        history.observe(m);
                        metrics.observe_values(m);
                    }
                    if batch.invalid > 0 {
                        history.break_line();
                    }
                    if let Some((_, m)) = batch.samples.last() {
                        metrics.latest = Some(m.clone());
                    }
                    let recorded = sampler.process(batch);
                    largest_block = largest_block.max(recorded.memory());
                    add(recorded, &mut pending, &mut bytes);
                }
                Ok(Message::Snapshot(reply)) => {
                    if let Some(r) = sampler.finish() {
                        add(
                            RecordedBatch {
                                frame: None,
                                records: vec![r],
                            },
                            &mut pending,
                            &mut bytes,
                        );
                    }
                    seal(&mut pending, &mut bytes)?;
                    jobs.send(WriteJob::Snapshot(reply))
                        .context("database writer disconnected")?;
                }
                Ok(Message::End) | Err(RecvTimeoutError::Disconnected) => {
                    if let Some(r) = sampler.finish() {
                        add(
                            RecordedBatch {
                                frame: None,
                                records: vec![r],
                            },
                            &mut pending,
                            &mut bytes,
                        );
                    }
                    seal(&mut pending, &mut bytes)?;
                    jobs.send(WriteJob::End)
                        .context("database writer disconnected")?;
                }
                Err(RecvTimeoutError::Timeout) => {}
            }
            if bytes >= config.buffer_size_bytes && !shared.lock().unwrap().storage_fault {
                seal(&mut pending, &mut bytes)?;
            }
            Ok(())
        })();
        if let Err(e) = result {
            fault(&shared, &cancel, format!("processing: {e:#}"));
            break;
        }
        if publish.elapsed() >= Duration::from_millis(100) || end {
            let (m, h) = (Arc::new(metrics.clone()), Arc::new(history.clone()));
            let mut s = shared.lock().unwrap();
            if s.display_generation == generation {
                s.metrics = m;
                s.history = h;
            }
            publish = Instant::now();
        }
        if end {
            break;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{mock::MockSource, SessionInfo};
    #[test]
    fn disk_lock_does_not_block_live_snapshots_and_small_cache_waits_for_stop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("locked.db");
        let conn = crate::storage::open_database(path.to_str().unwrap()).unwrap();
        let mut capture = Runtime::start(Box::new(MockSource), path.to_str().unwrap().into());
        let deadline = Instant::now() + Duration::from_secs(5);
        while capture.shared.lock().unwrap().session_id.is_none()
            || capture.shared.lock().unwrap().accepted < 3
        {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(capture.shared.lock().unwrap().saved, 0);
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let barrier = capture.barrier();
        let download = thread::spawn(move || barrier.snapshot());
        thread::sleep(Duration::from_millis(1200));
        let s = capture.shared.lock().unwrap().clone();
        assert!(s.metrics.count >= 5);
        assert!(!s.history.buckets().is_empty());
        assert_eq!(s.saved, 0);
        conn.execute_batch("ROLLBACK").unwrap();
        drop(download.join().unwrap().unwrap());
        capture.ensure_saved().unwrap();
        let s = capture.shared.lock().unwrap();
        assert_eq!(s.accepted, s.saved_source);
        assert_eq!(s.buffered_bytes, 0);
        let saved = s.saved;
        drop(s);
        capture.reset_metrics();
        let s = capture.shared.lock().unwrap();
        assert_eq!(s.metrics.count, 0);
        assert!(s.history.buckets().is_empty());
        assert_eq!(s.saved, saved);
    }
    struct PacedFrames(Arc<AtomicBool>);
    impl DataSource for PacedFrames {
        fn run(self: Box<Self>, sink: &Sink) -> Result<()> {
            sink.ready("synthetic".into(), "usb-cc", vec![])?;
            while !self.0.load(Ordering::Acquire) && !sink.stopped() {
                thread::sleep(Duration::from_millis(5));
            }
            for i in 0..100 {
                if sink.stopped() {
                    break;
                }
                sink.batch(crate::recording::tests::batch(i * 800, 800))?;
                thread::sleep(Duration::from_millis(20));
            }
            Ok(())
        }
    }
    #[test]
    fn capacity_triggers_write_and_two_full_buffers_stop_without_losing_accepted_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("capacity.db");
        let gate = Arc::new(AtomicBool::new(false));
        let config = Config {
            buffer_size_bytes: 256_000,
            ..Config::default()
        };
        let mut capture = Runtime::start_config(
            Box::new(PacedFrames(gate.clone())),
            path.to_str().unwrap().into(),
            config,
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while capture.shared.lock().unwrap().session_id.is_none() {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(5));
        }
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        gate.store(true, Ordering::Release);
        while !capture.cancel.load(Ordering::Relaxed) {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(5));
        }
        let state = capture.shared.lock().unwrap().clone();
        assert!(state.writing_bytes >= config.buffer_size_bytes);
        assert_eq!(state.saved, 0);
        assert!(state.error.unwrap().contains("both recording buffers full"));
        let block = Resampler::new(10000)
            .process(crate::recording::tests::batch(0, 800))
            .memory();
        assert!(state.buffered_bytes <= 2 * config.buffer_size_bytes + 10 * block);
        conn.execute_batch("ROLLBACK").unwrap();
        capture.ensure_saved().unwrap();
        let state = capture.shared.lock().unwrap();
        assert_eq!(state.accepted, state.saved_source);
        assert_eq!(state.dropped, 0);
        assert_eq!(state.buffered_bytes, 0);
    }
    #[test]
    fn barrier_flushes_partial_record_and_pins_snapshot_while_capture_continues() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("barrier.db");
        let mut capture = Runtime::start_config(
            Box::new(MockSource),
            path.to_str().unwrap().into(),
            Config {
                sample_rate_hz: 1,
                ..Config::default()
            },
        );
        while capture.shared.lock().unwrap().accepted < 3 {
            thread::sleep(Duration::from_millis(10));
        }
        let snapshot = capture.barrier().snapshot().unwrap();
        let count: u64 = snapshot
            .query_row("SELECT sum(source_count) FROM measurements", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(count >= 3);
        thread::sleep(Duration::from_millis(300));
        assert!(capture.shared.lock().unwrap().accepted > count);
        capture.ensure_saved().unwrap();
        assert_eq!(
            snapshot
                .query_row("SELECT sum(source_count) FROM measurements", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            count
        );
        let conn = crate::network::read_database(&path).unwrap();
        let summary: (u64, u64, String) = conn
            .query_row(
                "SELECT saved_count,saved_source_count,outcome FROM sessions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert!(summary.0 < summary.1);
        assert_eq!(summary.2, "complete");
    }
    #[test]
    fn failed_memory_buffer_is_retained_and_retry_does_not_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retry.db");
        let conn = crate::storage::open_database(path.to_str().unwrap()).unwrap();
        conn.execute_batch("CREATE TRIGGER reject_sample BEFORE INSERT ON measurements BEGIN SELECT RAISE(FAIL,'temporary disk failure'); END;").unwrap();
        let mut capture = Runtime::start(Box::new(MockSource), path.to_str().unwrap().into());
        while capture.shared.lock().unwrap().accepted < 3 {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(capture.barrier().snapshot().is_err());
        assert!(capture.ensure_saved().is_err());
        assert!(capture.shared.lock().unwrap().buffered_bytes > 0);
        conn.execute_batch("DROP TRIGGER reject_sample").unwrap();
        capture.ensure_saved().unwrap();
        capture.ensure_saved().unwrap();
        let s = capture.shared.lock().unwrap();
        assert_eq!(s.accepted, s.saved_source);
        assert_eq!(s.buffered_bytes, 0);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM measurements", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            s.saved
        );
    }
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
            name: None,
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
            name: None,
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
