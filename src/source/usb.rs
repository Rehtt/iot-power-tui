use crate::{
    protocol::{status_query, Calibration, Decoder, Framer},
    runtime::State,
    source::{DataSource, Sink},
};
use anyhow::{bail, ensure, Context, Result};
use chrono::Utc;
use rusb::{Device, DeviceHandle, GlobalContext};
use std::time::{Duration, Instant};

const VID: u16 = 0x1209;
const PID: u16 = 0x7301;
const TIMEOUT: Duration = Duration::from_millis(250);

pub fn devices() -> Result<Vec<Device<GlobalContext>>> {
    let mut matching = Vec::new();
    for device in rusb::devices().context("enumerate USB devices")?.iter() {
        let d = device.device_descriptor()?;
        if d.vendor_id() == VID && d.product_id() == PID {
            matching.push(device);
        }
    }
    Ok(matching)
}
pub fn list_devices() -> Result<()> {
    let devices = devices()?;
    if devices.is_empty() {
        println!("No IoT Power CC (1209:7301) found");
    }
    for device in devices {
        let serial = device
            .open()
            .and_then(|h| h.read_serial_number_string_ascii(&device.device_descriptor()?));
        println!(
            "{:03}/{:03} 1209:7301 {}",
            device.bus_number(),
            device.address(),
            match serial {
                Ok(s) => s,
                Err(e) => format!("serial unavailable: {e} (check USB permissions)"),
            }
        );
    }
    Ok(())
}
struct Claimed(DeviceHandle<GlobalContext>);
impl Drop for Claimed {
    fn drop(&mut self) {
        let _ = self.0.release_interface(0);
    }
}
trait Transport {
    fn query(&mut self) -> Result<()>;
    fn read(&mut self, bytes: &mut [u8]) -> Result<usize>;
}
impl Transport for Claimed {
    fn query(&mut self) -> Result<()> {
        let n = self
            .0
            .write_bulk(0x02, &status_query(), TIMEOUT)
            .context("query CC calibration")?;
        ensure!(n == 64, "short CC status query write");
        Ok(())
    }
    fn read(&mut self, bytes: &mut [u8]) -> Result<usize> {
        match self.0.read_bulk(0x81, bytes, TIMEOUT) {
            Ok(n) => Ok(n),
            Err(rusb::Error::Timeout) => Ok(0),
            Err(e) => Err(e).context("read CC USB (device disconnected?)"),
        }
    }
}
pub struct UsbSource {
    pub serial: Option<String>,
}
impl DataSource for UsbSource {
    fn run(self: Box<Self>, sink: &Sink) -> Result<()> {
        let devices = devices()?;
        ensure!(!devices.is_empty(), "no IoT Power CC (1209:7301) connected");
        ensure!(
            devices.len() == 1 || self.serial.is_some(),
            "multiple CC devices; select --device SERIAL"
        );
        let mut selected = None;
        for device in devices {
            let handle = device.open().with_context(|| {
                format!(
                    "open USB {:03}/{:03}; grant CC USB access using the README udev rule",
                    device.bus_number(),
                    device.address()
                )
            })?;
            let serial = handle
                .read_serial_number_string_ascii(&device.device_descriptor()?)
                .context("read CC serial number")?;
            if self.serial.as_ref().is_none_or(|s| s == &serial) {
                selected = Some((handle, serial));
                break;
            }
        }
        let (handle, serial) = selected.context("requested CC serial number not found")?;
        handle
            .claim_interface(0)
            .context("claim CC USB interface 0")?;
        let mut transport = Claimed(handle);
        capture(
            &mut transport,
            sink,
            serial,
            Duration::from_secs(2),
            Duration::from_secs(3),
        )
    }
}
fn capture(
    transport: &mut impl Transport,
    sink: &Sink,
    serial: String,
    retry: Duration,
    idle: Duration,
) -> Result<()> {
    sink.state(State::Calibrating);
    let mut parser = Framer::default();
    let mut decoder = None;
    let mut attempts = 0;
    let mut last_query = Instant::now() - retry;
    let mut last_sample = Instant::now();
    let mut buffer = [0; 16384];
    while !sink.stopped() {
        if decoder.is_none() && last_query.elapsed() >= retry {
            ensure!(attempts < 3, "CC calibration timed out after 3 queries");
            transport.query()?;
            attempts += 1;
            last_query = Instant::now();
        }
        let n = transport.read(&mut buffer)?;
        for frame in parser.push(&buffer[..n]) {
            ensure!(frame[5] == 4, "USB frame is not device class CC (4)");
            let received = Utc::now();
            if frame[4] == 4 {
                let calibration = Calibration::parse(&frame)?;
                if decoder.is_none() {
                    sink.ready(serial.clone(), "usb-cc", frame)?;
                    decoder = Some(Decoder::new(calibration, serial.clone()));
                    last_sample = Instant::now();
                }
            } else if let Some(decoder) = decoder.as_mut() {
                let batch = decoder.decode(frame, received)?;
                let invalid = batch.invalid;
                sink.batch(batch)?;
                last_sample = Instant::now();
                if invalid > 0 {
                    bail!("CC packet contained {invalid} invalid samples; raw packet retained");
                }
            }
        }
        sink.shared.lock().unwrap().discarded_bytes = parser.discarded;
        if decoder.is_some() {
            ensure!(last_sample.elapsed() < idle, "CC sample stream timed out");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::tests::{packet, status},
        runtime::Shared,
        source::Message,
    };
    use std::{
        collections::VecDeque,
        sync::{atomic::AtomicBool, mpsc::sync_channel, Arc, Mutex},
    };
    struct Fake {
        data: VecDeque<Vec<u8>>,
        queries: usize,
        disconnect: bool,
    }
    impl Transport for Fake {
        fn query(&mut self) -> Result<()> {
            self.queries += 1;
            Ok(())
        }
        fn read(&mut self, bytes: &mut [u8]) -> Result<usize> {
            if let Some(frame) = self.data.pop_front() {
                bytes[..frame.len()].copy_from_slice(&frame);
                Ok(frame.len())
            } else if self.disconnect {
                bail!("synthetic disconnect")
            } else {
                Ok(0)
            }
        }
    }
    #[test]
    fn initialization_timeout_is_bounded_and_creates_no_session() {
        let (tx, rx) = sync_channel(8);
        let sink = Sink {
            tx,
            cancel: Arc::new(AtomicBool::new(false)),
            shared: Arc::new(Mutex::new(Shared::default())),
        };
        let mut fake = Fake {
            data: VecDeque::new(),
            queries: 0,
            disconnect: false,
        };
        assert!(capture(
            &mut fake,
            &sink,
            "test".into(),
            Duration::ZERO,
            Duration::from_secs(1)
        )
        .is_err());
        assert_eq!(fake.queries, 3);
        assert!(rx.try_recv().is_err());
    }
    #[test]
    fn calibration_precedes_samples_and_disconnect_surfaces() {
        let (tx, rx) = sync_channel(8);
        let sink = Sink {
            tx,
            cancel: Arc::new(AtomicBool::new(false)),
            shared: Arc::new(Mutex::new(Shared::default())),
        };
        let mut fake = Fake {
            data: VecDeque::from(vec![packet(0), status(), packet(1)]),
            queries: 0,
            disconnect: true,
        };
        let error = capture(
            &mut fake,
            &sink,
            "test".into(),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(error.to_string().contains("disconnect"));
        assert!(matches!(rx.try_recv().unwrap(), Message::Ready(_)));
        assert!(matches!(rx.try_recv().unwrap(), Message::Batch(_)));
        assert_eq!(sink.shared.lock().unwrap().accepted, 800);
    }
    #[test]
    fn invalid_sample_retains_raw_packet_then_faults() {
        let (tx, rx) = sync_channel(8);
        let sink = Sink {
            tx,
            cancel: Arc::new(AtomicBool::new(false)),
            shared: Arc::new(Mutex::new(Shared::default())),
        };
        let mut raw = packet(1);
        raw[8..10].copy_from_slice(&[0, 0]);
        let mut fake = Fake {
            data: VecDeque::from(vec![status(), raw.clone()]),
            queries: 0,
            disconnect: true,
        };
        let e = capture(
            &mut fake,
            &sink,
            "test".into(),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(e.to_string().contains("invalid samples"));
        assert!(matches!(rx.try_recv().unwrap(), Message::Ready(_)));
        let Message::Batch(batch) = rx.try_recv().unwrap() else {
            panic!("expected raw sample packet")
        };
        assert_eq!(batch.frame, raw);
        assert_eq!(batch.samples.len(), 799);
        assert_eq!(batch.samples[0].0, 1);
    }
    #[test]
    fn cancellation_sends_no_query_and_idle_stream_faults() {
        let (tx, _rx) = sync_channel(8);
        let sink = Sink {
            tx,
            cancel: Arc::new(AtomicBool::new(true)),
            shared: Arc::new(Mutex::new(Shared::default())),
        };
        let mut fake = Fake {
            data: VecDeque::from(vec![status()]),
            queries: 0,
            disconnect: false,
        };
        capture(
            &mut fake,
            &sink,
            "test".into(),
            Duration::from_secs(2),
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(fake.queries, 0);
        sink.cancel
            .store(false, std::sync::atomic::Ordering::Relaxed);
        let e = capture(
            &mut fake,
            &sink,
            "test".into(),
            Duration::from_secs(2),
            Duration::ZERO,
        )
        .unwrap_err();
        assert!(e.to_string().contains("sample stream timed out"));
    }
}
