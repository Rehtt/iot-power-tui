use crate::{
    domain::Measurement,
    source::{DataSource, Sink},
};
use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
};
pub struct ReplaySource {
    pub path: String,
}
impl DataSource for ReplaySource {
    fn run(self: Box<Self>, sink: &Sink) -> Result<()> {
        let mut reader = BufReader::new(
            File::open(&self.path).with_context(|| format!("open replay {}", self.path))?,
        );
        let mut ready = false;
        while !sink.stopped() {
            let mut line = String::new();
            // Bound each JSONL record; never accumulate an unbounded device/file line.
            let n = std::io::Read::by_ref(&mut reader)
                .take(1_048_577)
                .read_line(&mut line)?;
            anyhow::ensure!(n <= 1_048_576, "JSONL record exceeds 1 MiB");
            if n == 0 {
                break;
            }
            let m: Measurement = serde_json::from_str(&line).context("decode replay JSONL")?;
            if !ready {
                sink.ready(m.device_id.clone(), "replay", Vec::new())?;
                ready = true;
            }
            if sink.stopped() {
                break;
            }
            sink.measurement(m)?;
        }
        Ok(())
    }
}
