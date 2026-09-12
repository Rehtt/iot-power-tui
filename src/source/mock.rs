use crate::{
    domain::Measurement,
    source::{DataSource, Sink},
};
use anyhow::Result;
use chrono::Utc;
use std::time::Duration;
pub struct MockSource;
impl DataSource for MockSource {
    fn run(self: Box<Self>, sink: &Sink) -> Result<()> {
        sink.ready("MOCK-CC".into(), "mock", Vec::new())?;
        let mut t = 0.0_f64;
        let mut energy = 0.0;
        while !sink.stopped() {
            t += 0.1;
            let voltage_v = 5.0 + t.sin() * 0.05;
            let current_a = 0.25 + t.cos() * 0.03;
            let power_w = voltage_v * current_a;
            energy += power_w * 0.1 / 3600.0;
            sink.measurement(Measurement {
                timestamp: Utc::now(),
                device_id: "MOCK-CC".into(),
                voltage_v,
                current_a,
                power_w,
                energy_wh: energy,
                status: "mock".into(),
                raw: Vec::new(),
            })?;
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }
}
