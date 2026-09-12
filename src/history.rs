use crate::domain::Measurement;
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
    pub fn buckets(&self) -> Vec<Bucket> {
        self.buckets.iter().cloned().collect()
    }
    pub fn from_buckets(buckets: Vec<Bucket>) -> Self {
        Self {
            buckets: buckets.into_iter().take(600).collect(),
            ..Self::default()
        }
    }
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
        let width = width.max(1);
        let start = latest.time - seconds as i64 * 10 + 1;
        let mut aggregated: Vec<(i64, u64, Stats)> = Vec::new();
        for b in &self.buckets {
            // Anchor groups to measurement time, not the moving window. Otherwise
            // every scroll reassigns old samples and changes their mean/extrema.
            let column = b.time.div_euclid(width);
            // Drop the whole group at the left edge instead of recomputing it
            // from a shrinking subset as samples leave the visible window.
            if column * width < start {
                continue;
            }
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
            let x = ((column + 1) * width - 1 - latest.time) as f64 / 10.0;
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
    fn absolute_points(h: &History, seconds: u32, columns: usize) -> Vec<(i64, [f64; 3])> {
        let latest = h.buckets.back().unwrap().time;
        h.series(1, seconds, columns)
            .into_iter()
            .flat_map(|line| {
                line.mean
                    .into_iter()
                    .zip(line.min)
                    .zip(line.max)
                    .map(move |((mean, min), max)| {
                        (
                            latest + (mean.0 * 10.0).round() as i64,
                            [mean.1, min.1, max.1],
                        )
                    })
            })
            .collect()
    }
    #[test]
    fn completed_groups_keep_values_and_timestamps_while_the_window_scrolls() {
        for seconds in [10, 30, 60] {
            for columns in [17, 53, 120] {
                let mut h = History::default();
                for t in 0..610 {
                    h.observe(&sample(t * 100_000, (t % 13) as f64));
                    // Unequal counts and narrow spikes must remain unchanged too.
                    if t % 7 == 0 {
                        h.observe(&sample(t * 100_000 + 1, -50.0));
                    }
                }
                let baseline = absolute_points(&h, seconds, columns);
                let width = (i64::from(seconds) * 10 + columns as i64 - 1) / columns as i64;
                let mut compared = 0;
                for t in 610..650 {
                    h.observe(&sample(t * 100_000, (t % 13) as f64));
                    // The client receives a new full snapshot on every poll.
                    let remote = History::from_buckets(h.buckets());
                    let points = absolute_points(&remote, seconds, columns);
                    for &(time, values) in &baseline {
                        if time < 609 && time - width + 1 >= t - i64::from(seconds) * 10 + 1 {
                            assert_eq!(
                                points.iter().find(|p| p.0 == time),
                                Some(&(time, values)),
                                "{seconds}s, {columns} columns, now={t}, group ending={time}"
                            );
                            compared += 1;
                        }
                    }
                }
                assert!(compared > 0);
            }
        }
    }
    #[test]
    fn scrolling_removes_a_clipped_left_group_instead_of_recomputing_it() {
        let mut h = History::default();
        for t in 0..200 {
            h.observe(&sample(t * 100_000, if t == 100 { 100.0 } else { 0.0 }));
        }
        let before = absolute_points(&h, 10, 25);
        assert_eq!(before[0].0, 103);
        assert!((before[0].1[0] - 25.0).abs() < 1e-12);
        assert_eq!(before[0].1[1..], [0.0, 100.0]);
        h.observe(&sample(20_000_000, 0.0));
        let after = absolute_points(&h, 10, 25);
        assert_eq!(after[0].0, 107);
        assert_eq!(after[..after.len() - 1], before[1..]);
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
