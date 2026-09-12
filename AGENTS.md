# Repository Guidelines

## Project Structure & Module Organization

This Rust terminal application collects IoT Power CC measurements.

- `src/main.rs`: CLI, application loop, worker coordination, and terminal restoration.
- `src/domain.rs`: measurements and display metrics.
- `src/history.rs` and `src/ui.rs`: bounded chart aggregation, layouts, and exit dialog.
- `src/workspace.rs`: per-run staging, save/import, and explicit discard.
- `src/protocol.rs`: CC framing, calibration, timestamps, and decoding.
- `src/source/`: native USB, mock, replay, and serial JSONL adapters.
- `src/runtime.rs`: cancellation, bounded queues, capture state, and database worker.
- `src/storage.rs`: SQLite migration, batch transactions, and session finalization.
- `docs/cc-protocol.md`: protocol evidence and formulas.
- Keep tests beside modules; place integration fixtures under `tests/fixtures/`. Use synthetic frames only.

## Build, Test, and Development Commands

Run from the repository root:

```bash
cargo check                         # Type-check
cargo test                          # Run tests
cargo fmt -- --check                 # Check formatting
cargo clippy --all-targets -- -D warnings
cargo run -- --list-devices          # List CC devices
cargo run -- --usb                   # Native CC capture
cargo run -- --mock                  # Generated measurements
cargo run -- --replay samples.jsonl  # Replay measurements
cargo run -- --port /dev/ttyACM0      # Serial JSONL, not CC USB
```

On network failure, retry with `http://192.168.31.100:7890` as the HTTP(S) proxy.

## Coding Style & Naming Conventions

Use stable Rust and rustfmt defaults: four-space indentation and trailing commas. Use `snake_case` for functions/modules, `PascalCase` for types, and explicit units such as `voltage_v` and `energy_wh`. Separate transport, decoding, persistence, and UI. Return contextual `anyhow::Result` errors for recoverable failures.

## Testing Guidelines

Prefer deterministic tests without hardware. Cover framing, calibration ranges, unit conversion, packet gaps/wrap, SQLite migrations, transaction rollback, cancellation, backpressure, and session lifecycle. Name tests after behavior, such as `source_failure_drains_accepted_packets_and_marks_incomplete`. Run all four checks above before submitting. Record actual hardware results separately; synthetic arithmetic tests do not establish device accuracy.

## Commit & Pull Request Guidelines

No established Git history exists. Use focused, imperative conventional-style subjects, such as `feat: add CC USB source`. PRs should explain user-visible behavior, validation commands, hardware assumptions, and protocol/database migrations. Link applicable issues; include sample TUI output for layout changes.

## Security & Configuration Tips

Never commit captured device data, credentials, or runtime databases. `data/`, `work/`, `target/`, and `.iot-power-pending/` are ignored. CC USB access uses the targeted udev rule in README; `dialout` applies to serial input. Validate incoming lengths, calibration values, and sample ranges. Do not add device-output or firmware commands to acquisition code.
