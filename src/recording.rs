//! Recording policy is independent of full-rate live statistics.
use crate::{domain::Measurement, source::Batch};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::mem::size_of;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub buffer_size_bytes: usize,
    pub sample_rate_hz: u32,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            buffer_size_bytes: 10_000_000,
            sample_rate_hz: 10_000,
        }
    }
}
impl Config {
    pub fn validate(self) -> Result<Self> {
        ensure!(
            (1..=10_000).contains(&self.sample_rate_hz),
            "record rate must be 1..10000 Hz"
        );
        ensure!(
            (64_000..=256_000_000).contains(&self.buffer_size_bytes),
            "buffer size must be 64K..256M bytes per buffer"
        );
        Ok(self)
    }
}
pub fn parse_size(text: &str) -> Result<usize, String> {
    let text = text.trim();
    let end = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let n: usize = text[..end].parse().map_err(|_| "invalid buffer size")?;
    let factor = match text[end..].trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" => 1000,
        "m" | "mb" => 1_000_000,
        "kib" => 1024,
        "mib" => 1024 * 1024,
        _ => return Err("use bytes, K, M, KiB or MiB".into()),
    };
    n.checked_mul(factor)
        .ok_or_else(|| "buffer size overflow".into())
}
#[derive(Clone, Copy, Debug)]
pub struct FrameRef {
    pub sequence: u64,
    pub index: u32,
}
#[derive(Clone, Debug)]
pub struct Aggregate {
    pub count: u64,
    pub end: DateTime<Utc>,
    pub last: Option<FrameRef>,
    pub min: [f64; 3],
    pub max: [f64; 3],
    pub partial: bool,
}
#[derive(Clone, Debug)]
pub struct Record {
    pub measurement: Measurement,
    pub first: Option<FrameRef>,
    pub aggregate: Option<Box<Aggregate>>,
}
impl Record {
    pub fn count(&self) -> u64 {
        self.aggregate.as_ref().map_or(1, |a| a.count)
    }
    pub fn memory(&self) -> usize {
        let m = &self.measurement;
        m.device_id.capacity()
            + m.status.capacity()
            + m.raw.capacity()
            + self
                .aggregate
                .as_ref()
                .map_or(0, |_| size_of::<Aggregate>())
    }
}
#[derive(Debug)]
pub struct Frame {
    pub sequence: u64,
    pub packet_id: Option<u32>,
    pub received: DateTime<Utc>,
    pub raw: Vec<u8>,
    pub gaps: u64,
    pub invalid: u64,
}
#[derive(Default, Debug)]
pub struct RecordedBatch {
    pub frame: Option<Frame>,
    pub records: Vec<Record>,
}
impl RecordedBatch {
    pub fn memory(&self) -> usize {
        size_of::<Self>()
            + self.frame.as_ref().map_or(0, |f| f.raw.capacity())
            + self.records.capacity() * size_of::<Record>()
            + self.records.iter().map(Record::memory).sum::<usize>()
    }
}
pub struct Resampler {
    rate: u32,
    sequence: u64,
    origin: Option<i64>,
    previous: Option<i64>,
    bucket: Option<i128>,
    current: Option<Record>,
    cut: bool,
}
impl Resampler {
    pub fn new(rate: u32) -> Self {
        Self {
            rate,
            sequence: 0,
            origin: None,
            previous: None,
            bucket: None,
            current: None,
            cut: false,
        }
    }
    #[cfg(test)]
    pub fn with_sequence(sequence: u64) -> Self {
        let mut s = Self::new(10000);
        s.sequence = sequence;
        s
    }
    pub fn finish(&mut self) -> Option<Record> {
        self.cut = true;
        let mut record = self.current.take()?;
        if let Some(a) = &mut record.aggregate {
            // A CC sample covers its nominal 100 us interval. Do not label a
            // completely filled final bucket partial solely because capture ends.
            let complete = a.last.is_some()
                && self
                    .origin
                    .zip(self.bucket)
                    .is_some_and(|(origin, bucket)| {
                        (a.end.timestamp_micros() as i128 + 100 - origin as i128)
                            * self.rate as i128
                            >= (bucket + 1) * 1_000_000
                    });
            a.partial |= !complete;
        }
        Some(record)
    }
    pub fn process(&mut self, batch: Batch) -> RecordedBatch {
        let seq = self.sequence;
        self.sequence += 1;
        let has_frame = !batch.frame.is_empty();
        let mut out = RecordedBatch {
            frame: has_frame.then_some(Frame {
                sequence: seq,
                packet_id: batch.packet_id,
                received: batch.received,
                raw: batch.frame,
                gaps: batch.gaps,
                invalid: batch.invalid,
            }),
            records: Vec::with_capacity(if self.rate == 10000 {
                batch.samples.len()
            } else {
                batch.samples.len() / (10000 / self.rate).max(1) as usize + 2
            }),
        };
        if batch.gaps > 0 {
            if let Some(r) = self.finish() {
                out.records.push(r);
            }
        }
        let mut previous_index = None;
        for (index, m) in batch.samples {
            let reference = has_frame.then_some(FrameRef {
                sequence: seq,
                index,
            });
            let us = m.timestamp.timestamp_micros();
            let broken = previous_index.is_some_and(|p| index != p + 1)
                || (previous_index.is_none() && index > 0)
                || self
                    .previous
                    .is_some_and(|p| us < p || (has_frame && us - p > 100));
            previous_index = Some(index);
            if broken {
                if let Some(r) = self.finish() {
                    out.records.push(r);
                }
                if self.previous.is_some_and(|p| us < p) {
                    self.origin = None;
                }
            }
            self.previous = Some(us);
            if self.rate == 10000 {
                out.records.push(Record {
                    measurement: m,
                    first: reference,
                    aggregate: None,
                });
                continue;
            }
            let origin = *self.origin.get_or_insert(us);
            let bucket = ((us as i128 - origin as i128) * self.rate as i128).div_euclid(1_000_000);
            if self.bucket != Some(bucket) {
                if let Some(r) = self.current.take() {
                    out.records.push(r);
                }
                self.bucket = Some(bucket);
                self.cut = broken;
            }
            let values = [m.voltage_v, m.current_a, m.power_w];
            if let Some(r) = &mut self.current {
                let a = r.aggregate.as_mut().unwrap();
                a.count += 1;
                a.end = m.timestamp;
                a.last = reference;
                for (i, v) in values.into_iter().enumerate() {
                    a.min[i] = a.min[i].min(v);
                    a.max[i] = a.max[i].max(v);
                }
                let n = a.count as f64;
                r.measurement.voltage_v += (m.voltage_v - r.measurement.voltage_v) / n;
                r.measurement.current_a += (m.current_a - r.measurement.current_a) / n;
                r.measurement.power_w += (m.power_w - r.measurement.power_w) / n;
                r.measurement.energy_wh = m.energy_wh;
            } else {
                self.current = Some(Record {
                    first: reference,
                    aggregate: Some(Box::new(Aggregate {
                        count: 1,
                        end: m.timestamp,
                        last: reference,
                        min: values,
                        max: values,
                        partial: self.cut,
                    })),
                    measurement: m,
                });
            }
        }
        if batch.invalid > 0 {
            if let Some(r) = self.finish() {
                out.records.push(r);
            }
        }
        out
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    pub fn batch(start: usize, count: usize) -> Batch {
        Batch {
            frame: vec![0; 3212],
            packet_id: Some((start / 800) as u32),
            received: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            gaps: 0,
            invalid: 0,
            samples: (start..start + count)
                .map(|i| {
                    (
                        (i - start) as u32,
                        Measurement {
                            timestamp: DateTime::from_timestamp_micros(
                                1_700_000_000_000_000 + i as i64 * 100,
                            )
                            .unwrap(),
                            device_id: "synthetic".into(),
                            voltage_v: if i == 3 { 100.0 } else { 2.0 },
                            current_a: if i == 3 { 0.5 } else { 1.0 },
                            power_w: if i == 3 { 50.0 } else { 2.0 },
                            energy_wh: i as f64 / 1e8,
                            status: "estimated_timestamp".into(),
                            raw: vec![],
                        },
                    )
                })
                .collect(),
        }
    }
    #[test]
    fn rates_preserve_counts_ranges_spikes_and_cumulative_energy() {
        for rate in [10000, 1000, 333, 1] {
            let mut r = Resampler::new(rate);
            let mut records = vec![];
            let mut frames = 0;
            for start in (0..10000).step_by(800) {
                let b = r.process(batch(start, (10000 - start).min(800)));
                frames += usize::from(b.frame.is_some());
                records.extend(b.records);
            }
            records.extend(r.finish());
            assert_eq!(frames, 13);
            assert_eq!(records.len(), rate as usize);
            assert_eq!(records.iter().map(Record::count).sum::<u64>(), 10000);
            assert_eq!(records.last().unwrap().measurement.energy_wh, 9999.0 / 1e8);
            if rate < 10000 {
                assert!(!records.last().unwrap().aggregate.as_ref().unwrap().partial);
                let first = &records[0];
                let a = first.aggregate.as_ref().unwrap();
                assert_eq!(a.max, [100.0, 1.0, 50.0]);
                assert!(
                    (first.measurement.power_w
                        - (2.0 * (a.count - 1) as f64 + 50.0) / a.count as f64)
                        .abs()
                        < 1e-10
                );
                assert_eq!(first.first.unwrap().sequence, 0);
                assert_eq!(
                    records
                        .last()
                        .unwrap()
                        .aggregate
                        .as_ref()
                        .unwrap()
                        .last
                        .unwrap()
                        .sequence,
                    12
                );
            }
        }
    }
    #[test]
    fn gaps_barriers_backward_time_and_invalid_indices_split_intervals() {
        let mut r = Resampler::new(1);
        assert!(r.process(batch(0, 2)).records.is_empty());
        let mut b = batch(800, 2);
        b.gaps = 1;
        let out = r.process(b);
        assert_eq!(out.records.len(), 1);
        assert!(out.records[0].aggregate.as_ref().unwrap().partial);
        let barrier = r.finish().unwrap();
        assert_eq!(barrier.count(), 2);
        let mut b = batch(0, 3);
        b.samples.remove(1);
        b.invalid = 1;
        let out = r.process(b);
        assert_eq!(out.records.len(), 2);
        assert!(out.records.iter().all(|r| r.count() == 1));
    }
    #[test]
    fn configuration_units_and_bounds_are_checked() {
        assert_eq!(parse_size("10M").unwrap(), 10_000_000);
        assert_eq!(parse_size("10MiB").unwrap(), 10_485_760);
        for text in ["-1M", "1.5M", "99999999999999999999M", "3GB"] {
            assert!(parse_size(text).is_err());
        }
        for rate in [0, 10001] {
            assert!(Config {
                sample_rate_hz: rate,
                ..Config::default()
            }
            .validate()
            .is_err());
        }
    }
}
