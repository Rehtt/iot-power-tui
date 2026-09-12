use crate::{
    domain::Measurement,
    runtime::{Shared, State},
};
use anyhow::{bail, ensure, Result};
use chrono::{DateTime, Utc};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{SyncSender, TrySendError},
    Arc, Mutex,
};

pub mod mock;
pub mod replay;
pub mod serial;
pub mod usb;

pub struct Batch {
    pub frame: Vec<u8>,
    pub packet_id: Option<u32>,
    pub received: DateTime<Utc>,
    pub samples: Vec<(u32, Measurement)>,
    pub gaps: u64,
    pub invalid: u64,
}
pub struct SessionInfo {
    pub device: String,
    pub transport: &'static str,
    pub calibration: Vec<u8>,
    pub received: DateTime<Utc>,
}
pub enum Message {
    Ready(SessionInfo),
    Batch(Batch),
}
pub struct Sink {
    pub tx: SyncSender<Message>,
    pub cancel: Arc<AtomicBool>,
    pub shared: Arc<Mutex<Shared>>,
}
impl Sink {
    pub fn stopped(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }
    pub fn state(&self, state: State) {
        let mut s = self.shared.lock().unwrap();
        if s.error.is_none() {
            s.state = state;
        }
    }
    pub fn ready(
        &self,
        device: String,
        transport: &'static str,
        calibration: Vec<u8>,
    ) -> Result<()> {
        self.shared.lock().unwrap().device = Some(device.clone());
        self.send(Message::Ready(SessionInfo {
            device,
            transport,
            calibration,
            received: Utc::now(),
        }))?;
        self.state(State::Capturing);
        Ok(())
    }
    fn send(&self, message: Message) -> Result<()> {
        match self.tx.try_send(message) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(message)) => {
                if let Message::Batch(batch) = message {
                    self.shared.lock().unwrap().dropped += batch.samples.len() as u64;
                }
                bail!("acquisition queue full; capture stopped (data loss)")
            }
            Err(TrySendError::Disconnected(_)) => bail!("database writer disconnected"),
        }
    }
    pub fn batch(&self, batch: Batch) -> Result<()> {
        let n = batch.samples.len() as u64;
        {
            let mut s = self.shared.lock().unwrap();
            s.received += n;
            s.gaps += batch.gaps;
            s.invalid += batch.invalid;
        }
        self.send(Message::Batch(batch))?;
        self.shared.lock().unwrap().accepted += n;
        Ok(())
    }
    pub fn measurement(&self, m: Measurement) -> Result<()> {
        ensure!(
            [m.voltage_v, m.current_a, m.power_w, m.energy_wh]
                .iter()
                .all(|v| v.is_finite()),
            "nonfinite input measurement"
        );
        let mut message = Message::Batch(Batch {
            frame: Vec::new(),
            packet_id: None,
            received: Utc::now(),
            samples: vec![(0, m)],
            gaps: 0,
            invalid: 0,
        });
        while !self.stopped() {
            match self.tx.try_send(message) {
                Ok(()) => {
                    let mut s = self.shared.lock().unwrap();
                    s.received += 1;
                    s.accepted += 1;
                    return Ok(());
                }
                Err(TrySendError::Full(returned)) => {
                    message = returned;
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                Err(TrySendError::Disconnected(_)) => bail!("database writer disconnected"),
            }
        }
        Ok(())
    }
}
pub trait DataSource: Send + 'static {
    fn run(self: Box<Self>, sink: &Sink) -> Result<()>;
}
