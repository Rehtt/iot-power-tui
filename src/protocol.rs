//! CC framing and calibration. No device control commands are implemented here.
use anyhow::{bail, ensure, Result};
use chrono::{DateTime, TimeDelta, Utc};

use crate::{domain::Measurement, source::Batch};

pub const SAMPLES: usize = 800;
pub const SAMPLE_FRAME_LEN: usize = 3212;
pub const SAMPLE_US: i64 = 100;

pub fn status_query() -> [u8; 64] {
    let mut bytes = [0; 64];
    bytes[..4].copy_from_slice(&[0x55, 0xaa, 0x40, 0x04]);
    bytes[62..].copy_from_slice(&[0x0a, 0x0d]);
    bytes
}

#[derive(Default)]
pub struct Framer {
    buffer: Vec<u8>,
    pub discarded: u64,
}
impl Framer {
    pub fn push(&mut self, data: &[u8]) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        // Process incrementally so even an arbitrarily large input cannot grow the buffer.
        for &byte in data {
            self.buffer.push(byte);
            loop {
                if self.buffer.len() < 2 {
                    break;
                }
                if self.buffer[..2] != [0xaa, 0x55] {
                    self.buffer.remove(0);
                    self.discarded += 1;
                    continue;
                }
                if self.buffer.len() < 6 {
                    break;
                }
                let length = u16::from_le_bytes([self.buffer[2], self.buffer[3]]) as usize;
                if !matches!((length, self.buffer[4]), (64, 4) | (SAMPLE_FRAME_LEN, 1)) {
                    self.buffer.remove(0);
                    self.discarded += 1;
                    continue;
                }
                if self.buffer.len() < length {
                    break;
                }
                if self.buffer[length - 2..length] != [0x0a, 0x0d] {
                    self.buffer.remove(0);
                    self.discarded += 1;
                    continue;
                }
                frames.push(std::mem::take(&mut self.buffer));
                break;
            }
        }
        frames
    }
}

#[derive(Clone, Debug)]
pub struct Calibration(pub [f64; 8]);
impl Calibration {
    pub fn parse(frame: &[u8]) -> Result<Self> {
        validate(frame, 64, 4)?;
        let mut values = [0.0; 8];
        for (i, value) in values.iter_mut().enumerate() {
            *value = f32::from_le_bytes(frame[6 + i * 4..10 + i * 4].try_into()?) as f64;
            ensure!(
                value.is_finite(),
                "nonfinite CC calibration coefficient {i}"
            );
        }
        for i in [0, 2, 4, 6] {
            ensure!(values[i] > 0.0, "invalid CC calibration gain {i}");
        }
        Ok(Self(values))
    }
    fn convert(&self, voltage: u16, current: u16) -> Result<(f64, f64)> {
        let range = (current >> 14) as usize;
        ensure!((1..=3).contains(&range), "invalid CC current range {range}");
        let v = voltage as f64 * self.0[0] * 100.0 + self.0[1];
        let a = ((current & 0x0fff) as f64 * self.0[range * 2] * 100.0 - v + self.0[range * 2 + 1])
            / 1_000_000.0;
        ensure!(
            v.is_finite() && a.is_finite() && (v * a).is_finite(),
            "nonfinite CC measurement"
        );
        Ok((v, a))
    }
}
fn validate(frame: &[u8], length: usize, kind: u8) -> Result<()> {
    ensure!(frame.len() == length, "wrong CC frame length");
    ensure!(
        frame[..2] == [0xaa, 0x55] && frame[length - 2..] == [10, 13],
        "invalid CC frame markers"
    );
    ensure!(
        u16::from_le_bytes([frame[2], frame[3]]) as usize == length,
        "invalid declared CC length"
    );
    ensure!(
        frame[4] == kind && frame[5] == 4,
        "unsupported CC frame type/class"
    );
    Ok(())
}

pub struct Decoder {
    calibration: Calibration,
    device: String,
    previous: Option<u32>,
    anchor: Option<DateTime<Utc>>,
    packet_offset: u64,
    energy_wh: f64,
}
impl Decoder {
    pub fn new(calibration: Calibration, device: String) -> Self {
        Self {
            calibration,
            device,
            previous: None,
            anchor: None,
            packet_offset: 0,
            energy_wh: 0.0,
        }
    }
    pub fn decode(&mut self, frame: Vec<u8>, received: DateTime<Utc>) -> Result<Batch> {
        validate(&frame, SAMPLE_FRAME_LEN, 1)?;
        let id = u32::from_le_bytes(frame[3206..3210].try_into()?);
        let delta = self.previous.map(|p| id.wrapping_sub(p)).unwrap_or(1);
        if delta == 0 || delta > i32::MAX as u32 {
            bail!("duplicate/backward CC packet ID {id}");
        }
        let gaps = if self.previous.is_some() {
            (delta - 1) as u64
        } else {
            0
        };
        if self.previous.is_some() {
            self.packet_offset += delta as u64;
        }
        self.previous = Some(id);
        let anchor = *self
            .anchor
            .get_or_insert(received - TimeDelta::microseconds((SAMPLES as i64 - 1) * SAMPLE_US));
        let mut samples = Vec::with_capacity(SAMPLES);
        let mut invalid = 0;
        for index in 0..SAMPLES {
            let offset = 6 + index * 4;
            let v = u16::from_le_bytes(frame[offset..offset + 2].try_into()?);
            let a = u16::from_le_bytes(frame[offset + 2..offset + 4].try_into()?);
            match self.calibration.convert(v, a) {
                Ok((voltage_v, current_a)) => {
                    let power_w = voltage_v * current_a;
                    self.energy_wh += power_w * 0.0001 / 3600.0;
                    let elapsed =
                        (self.packet_offset * SAMPLES as u64 + index as u64) as i64 * SAMPLE_US;
                    samples.push((
                        index as u32,
                        Measurement {
                            timestamp: anchor + TimeDelta::microseconds(elapsed),
                            device_id: self.device.clone(),
                            voltage_v,
                            current_a,
                            power_w,
                            energy_wh: self.energy_wh,
                            status: "estimated_timestamp".into(),
                            raw: Vec::new(),
                        },
                    ));
                }
                Err(_) => invalid += 1,
            }
        }
        Ok(Batch {
            frame,
            packet_id: Some(id),
            received,
            samples,
            gaps,
            invalid,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    pub fn status() -> Vec<u8> {
        let mut f = vec![0; 64];
        f[..6].copy_from_slice(&[0xaa, 0x55, 64, 0, 4, 4]);
        for (i, v) in [0.001_f32, 0.0, 1.0, 0.0, 2.0, 0.0, 3.0, 0.0]
            .iter()
            .enumerate()
        {
            f[6 + i * 4..10 + i * 4].copy_from_slice(&v.to_le_bytes());
        }
        f[62..].copy_from_slice(&[10, 13]);
        f
    }
    pub fn packet(id: u32) -> Vec<u8> {
        let mut f = vec![0; SAMPLE_FRAME_LEN];
        f[..6].copy_from_slice(&[0xaa, 0x55, 0x8c, 0x0c, 1, 4]);
        for i in 0..SAMPLES {
            f[6 + i * 4..8 + i * 4].copy_from_slice(&50_u16.to_le_bytes());
            f[8 + i * 4..10 + i * 4].copy_from_slice(&0x400a_u16.to_le_bytes());
        }
        f[3206..3210].copy_from_slice(&id.to_le_bytes());
        f[3210..].copy_from_slice(&[10, 13]);
        f
    }
    #[test]
    fn frames_fragmented_concatenated_and_resynchronized() {
        let mut parser = Framer::default();
        let mut data = vec![0xaa, 0x55, 0xff, 0xff, 1, 4, 23];
        let mut corrupt = status();
        corrupt[63] = 0;
        data.extend(corrupt);
        data.extend(status());
        data.extend(packet(2));
        let mut frames = Vec::new();
        for bytes in data.chunks(7) {
            frames.extend(parser.push(bytes));
        }
        assert_eq!(frames, vec![status(), packet(2)]);
        assert!(parser.discarded > 0);
        parser.push(&vec![0xaa; 100_000]);
        assert!(parser.buffer.len() < SAMPLE_FRAME_LEN);
    }
    #[test]
    fn all_ranges_units_mask_and_invalid_calibration() {
        let c = Calibration::parse(&status()).unwrap();
        for r in 1..=3 {
            let (v, a) = c.convert(50, (r << 14) | 0x300a).unwrap();
            assert!((v - 5.0).abs() < 1e-6);
            assert!((a - (1000.0 * r as f64 - 5.0) / 1e6).abs() < 1e-12);
        }
        assert!(c.convert(0, 0).is_err());
        assert!(c.convert(50, 0x4000).unwrap().1 < 0.0);
        let mut bad = status();
        bad[6..10].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(Calibration::parse(&bad).is_err());
        bad = status();
        bad[5] = 1;
        assert!(Calibration::parse(&bad).is_err());
    }
    #[test]
    fn packet_wrap_gaps_energy_and_invalid_sample_indices() {
        let mut decoder = Decoder::new(Calibration::parse(&status()).unwrap(), "synthetic".into());
        let now = Utc::now();
        let first = decoder.decode(packet(u32::MAX), now).unwrap();
        let second = decoder.decode(packet(0), now).unwrap();
        assert_eq!(second.gaps, 0);
        let mut raw = packet(3);
        raw[8..10].copy_from_slice(&0_u16.to_le_bytes());
        let third = decoder.decode(raw, now).unwrap();
        assert_eq!(third.gaps, 2);
        assert_eq!(third.invalid, 1);
        assert_eq!(third.samples[0].0, 1);
        assert_eq!(
            (third.samples[0].1.timestamp - first.samples[0].1.timestamp).num_microseconds(),
            Some(320100)
        );
        let last = &third.samples.last().unwrap().1;
        assert!((last.energy_wh - last.power_w * 2399.0 * 0.0001 / 3600.0).abs() < 1e-15);
        assert!(decoder.decode(packet(3), now).is_err());
    }
}
