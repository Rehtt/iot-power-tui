use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Measurement {
    pub timestamp: DateTime<Utc>,
    pub device_id: String,
    pub voltage_v: f64,
    pub current_a: f64,
    pub power_w: f64,
    pub energy_wh: f64,
    pub status: String,
    pub raw: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct Metrics {
    pub latest: Option<Measurement>,
    pub count: u64,
    pub average_voltage: f64,
    pub average_current: f64,
    pub average_power: f64,
    pub peak_power: f64,
}

impl Metrics {
    #[cfg(test)]
    pub fn observe(&mut self, m: Measurement) {
        self.observe_values(&m);
        self.latest = Some(m);
    }
    pub fn observe_values(&mut self, m: &Measurement) {
        self.count += 1;
        let n = self.count as f64;
        self.average_voltage += (m.voltage_v - self.average_voltage) / n;
        self.average_current += (m.current_a - self.average_current) / n;
        self.average_power += (m.power_w - self.average_power) / n;
        self.peak_power = if self.count == 1 {
            m.power_w
        } else {
            self.peak_power.max(m.power_w)
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metrics_mean_and_negative_peak_are_correct() {
        let mut metrics = Metrics::default();
        for power in [-2.0, -1.0] {
            metrics.observe(Measurement {
                timestamp: Utc::now(),
                device_id: "test".into(),
                voltage_v: 1.0,
                current_a: power,
                power_w: power,
                energy_wh: 0.0,
                status: "test".into(),
                raw: vec![],
            });
        }
        assert_eq!(metrics.count, 2);
        assert_eq!(metrics.average_power, -1.5);
        assert_eq!(metrics.peak_power, -1.0);
    }
}
