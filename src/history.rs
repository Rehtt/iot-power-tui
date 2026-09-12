use crate::domain::Measurement;
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug)]
pub struct Stats {
    pub count: u64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
}
impl Stats {
    fn new(value: f64) -> Self {
        Self {
            count: 1,
            mean: value,
            min: value,
            max: value,
        }
    }
    fn merge(&mut self, other: Self) {
        let count = self.count + other.count;
        self.mean += (other.mean - self.mean) * other.count as f64 / count as f64;
        self.count = count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }
}
#[derive(Clone, Debug)]
pub struct Bucket {
    pub time: i64,
    pub values: [Stats; 3],
    pub segment: u64,
}
#[derive(Clone, Default)]
pub struct History {
    buckets: VecDeque<Bucket>,
    previous_us: Option<i64>,
    segment: u64,
    broken: bool,
}
#[derive(Default, Debug)]
pub struct Series {
    pub mean: Vec<(f64, f64)>,
    pub min: Vec<(f64, f64)>,
    pub max: Vec<(f64, f64)>,
}
impl History {
    pub fn break_line(&mut self) {
        self.broken = true;
    }
    pub fn observe(&mut self, m: &Measurement) {
        let us = m.timestamp.timestamp_micros();
        let time = us.div_euclid(100_000);
        let backwards = self.previous_us.is_some_and(|p| us < p);
        // A backwards clock starts a new visible epoch; old data remains in SQLite.
        if backwards {
            self.buckets.clear();
        }
        if backwards || self.broken || self.buckets.back().is_some_and(|b| time > b.time + 1) {
            self.segment = self.segment.wrapping_add(1);
        }
        self.broken = false;
        self.previous_us = Some(us);
        let values = [m.voltage_v, m.current_a, m.power_w].map(Stats::new);
        if let Some(last) = self
            .buckets
            .back_mut()
            .filter(|b| b.time == time && b.segment == self.segment)
        {
            for (a, b) in last.values.iter_mut().zip(values) {
                a.merge(b);
            }
        } else {
            self.buckets.push_back(Bucket {
                time,
                values,
                segment: self.segment,
            });
        }
        while self.buckets.front().is_some_and(|b| time - b.time >= 600) || self.buckets.len() > 600
        {
            self.buckets.pop_front();
        }
    }
    pub fn series(&self, metric: usize, seconds: u32, columns: usize) -> Vec<Series> {
        let Some(latest) = self.buckets.back() else {
            return Vec::new();
        };
        let width = (seconds as i64 * 10 + columns.max(1) as i64 - 1) / columns.max(1) as i64;
        let mut aggregated: Vec<(i64, u64, Stats)> = Vec::new();
        for b in self
            .buckets
            .iter()
            .filter(|b| latest.time - b.time < seconds as i64 * 10)
        {
            let column = (b.time - (latest.time - seconds as i64 * 10 + 1)) / width.max(1);
            if let Some(last) = aggregated
                .last_mut()
                .filter(|last| last.0 == column && last.1 == b.segment)
            {
                last.2.merge(b.values[metric]);
            } else {
                aggregated.push((column, b.segment, b.values[metric]));
            }
        }
        let mut out = Vec::new();
        let mut previous = None;
        for (column, segment, stats) in aggregated {
            if previous != Some(segment) {
                out.push(Series::default());
                previous = Some(segment);
            }
            let x = ((column + 1) * width) as f64 / 10.0 - seconds as f64;
            let line = out.last_mut().expect("segment created");
            line.mean.push((x.min(0.0), stats.mean));
            line.min.push((x.min(0.0), stats.min));
            line.max.push((x.min(0.0), stats.max));
        }
        out
    }
}
pub fn axis(series: &[Series], metric: usize) -> (f64, &'static str, [f64; 2]) {
    let mut low = f64::INFINITY;
    let mut high = f64::NEG_INFINITY;
    for s in series {
        for &(_, v) in s.min.iter().chain(&s.max) {
            low = low.min(v);
            high = high.max(v);
        }
    }
    if !low.is_finite() {
        low = 0.0;
        high = 0.0;
    }
    let magnitude = low.abs().max(high.abs());
    let (scale, unit) = match metric {
        0 if magnitude > 0.0 && magnitude < 1.0 => (1000.0, "mV"),
        0 => (1.0, "V"),
        1 if magnitude > 0.0 && magnitude < 0.001 => (1e6, "µA"),
        1 if magnitude > 0.0 && magnitude < 1.0 => (1e3, "mA"),
        1 => (1.0, "A"),
        _ if magnitude > 0.0 && magnitude < 0.001 => (1e6, "µW"),
        _ if magnitude > 0.0 && magnitude < 1.0 => (1e3, "mW"),
        _ => (1.0, "W"),
    };
    low *= scale;
    high *= scale;
    let padding = ((high - low) * 0.08)
        .max(high.abs().max(low.abs()) * 0.01)
        .max(0.001);
    (scale, unit, [low - padding, high + padding])
}
#[cfg(test)]
mod tests {
    use super::*;
    fn sample(us: i64, v: f64) -> Measurement {
        Measurement {
            timestamp: chrono::DateTime::from_timestamp_micros(us).unwrap(),
            device_id: "synthetic".into(),
            voltage_v: v,
            current_a: v,
            power_w: v,
            energy_wh: 0.0,
            status: "test".into(),
            raw: vec![],
        }
    }
    #[test]
    fn retains_spikes_and_weighted_mean_when_reducing_columns() {
        let mut h = History::default();
        for i in 0..1000 {
            h.observe(&sample(i * 100, if i == 1 { 100.0 } else { 0.0 }));
        }
        h.observe(&sample(100_000, 10.0));
        let series = h.series(1, 10, 1);
        assert_eq!(series[0].max[0].1, 100.0);
        assert_eq!(series[0].min[0].1, 0.0);
        assert!((series[0].mean[0].1 - 110.0 / 1001.0).abs() < 1e-12);
    }
    #[test]
    fn bounds_history_and_breaks_gaps_and_backward_time() {
        let mut h = History::default();
        for t in 0..1000 {
            h.observe(&sample(t * 100_000, 1.0));
        }
        assert_eq!(h.buckets.len(), 600);
        h.break_line();
        h.observe(&sample(99_900_100, 2.0));
        assert_eq!(h.series(0, 30, 1).len(), 2);
        h.observe(&sample(100_200_000, 2.0));
        assert_eq!(h.series(0, 30, 100).len(), 3);
        h.observe(&sample(0, 2.0));
        assert_eq!(h.buckets.len(), 1);
    }
    #[test]
    fn axis_handles_empty_constant_negative_and_units() {
        for value in [0.0, -0.0001, 3.0] {
            let mut h = History::default();
            h.observe(&sample(0, value));
            let (scale, _, bounds) = axis(&h.series(1, 30, 80), 1);
            assert!(bounds[0] < value * scale && value * scale < bounds[1]);
        }
        assert_eq!(axis(&[], 0).1, "V");
        let mut h = History::default();
        h.observe(&sample(0, -0.0001));
        assert_eq!(axis(&h.series(1, 30, 80), 1).1, "µA");
    }
}
