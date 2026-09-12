use crate::{
    domain::Measurement,
    source::{DataSource, Sink},
};
use anyhow::{Context, Result};
use std::{io::Read, time::Duration};
pub struct SerialSource {
    pub port: String,
    pub baud: u32,
}
impl DataSource for SerialSource {
    fn run(self: Box<Self>, sink: &Sink) -> Result<()> {
        let mut port = serialport::new(&self.port, self.baud)
            .timeout(Duration::from_millis(100))
            .open()
            .with_context(|| format!("open serial {}", self.port))?;
        let mut pending = Vec::new();
        let mut bytes = [0; 4096];
        let mut ready = false;
        while !sink.stopped() {
            let n = match port.read(&mut bytes) {
                Ok(0) => anyhow::bail!("serial device disconnected"),
                Ok(n) => n,
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::TimedOut | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    continue
                }
                Err(e) => return Err(e).context("read serial JSONL"),
            };
            for &b in &bytes[..n] {
                pending.push(b);
                anyhow::ensure!(
                    pending.len() <= 1_048_576,
                    "serial JSONL record exceeds 1 MiB"
                );
                if b == b'\n' {
                    let mut m: Measurement =
                        serde_json::from_slice(&pending).context("decode serial JSONL")?;
                    m.raw = std::mem::take(&mut pending);
                    if !ready {
                        sink.ready(m.device_id.clone(), "serial-jsonl", Vec::new())?;
                        ready = true;
                    }
                    sink.measurement(m)?;
                }
            }
        }
        Ok(())
    }
}
