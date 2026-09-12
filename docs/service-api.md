# Service API v1

Start `iot-power-tui --service [--addr 0.0.0.0:8080] [--db PATH]`.
The API is read-only HTTP for trusted LANs, without authentication or TLS.
All values use V, A, W and Wh; chart bucket times are UTC epoch 100 ms units.

## Live status

`GET /api/v1/status` returns a cached JSON object, refreshed approximately 10 Hz.
Clients poll approximately 5 Hz. No database history scans or raw frames occur in this endpoint.

| Field | Meaning |
| --- | --- |
| `version` | API version, currently `1` |
| `instance` | Unique identity for this service process |
| `session_id` | Current session's database ID, or null before initialization |
| `state` | `Connecting`, `Calibrating`, `Capturing`, `Stopped`, `Fault` |
| `device` | Device identifier or null |
| `received`, `accepted`, `saved` | Current session counters; saved means committed to the service database |
| `gaps`, `invalid`, `dropped` | Missing packets, invalid samples, dropped samples |
| `error` | Capture fault description or null |
| `rate` | Recent received samples/second |
| `updated_at` | Snapshot generation UTC RFC 3339 time |
| `latest` | Null or timestamp, voltage_v, current_a, power_w, energy_wh |
| `count`, `averages`, `peak_power` | Display sample count, means in voltage/current/power order, peak W |
| `buckets` | At most 600 aggregate buckets, covering at most 60 seconds |

Each bucket contains `time`, `segment`, and `values` in voltage/current/power order.
Each value contains `count`, `mean`, `min`, `max`. Merge with sample-count weights;
preserve min/max, and never join different segments. Server aggregation already breaks
packet/time gaps and resets on backwards measurement time. Replace the full client
history on each response; process/session identity changes must not join old curves.
A successful HTTP response does not mean the device is capturing: inspect `state`.

## Sessions

`GET /api/v1/sessions[?before=ID]` returns up to 50 sessions in descending ID order.
For the next page, use the last returned ID as `before`; an empty array is the end.
Each entry has `id`, `device`, `started_at`, `ended_at`, `saved`, and `outcome`.
Active session `ended_at` is null, and counters reflect committed batches; full
received/accepted/error accounting is finalized when capture ends. An unclean
process exit also leaves null `ended_at` and `incomplete`; this is not proof that
the session is still capturing. Compare its ID to live status.

## Download

`GET /api/v1/sessions/{id}/download` streams `application/vnd.sqlite3`.
The server opens a separate read-only SQLite transaction and copies only the
selected session, its frames and measurements into a temporary SQLite file.
Each copy batch has at most 4096 rows; IDs and references remain intact. A WAL
read snapshot keeps the exported rows consistent while capture continues.

The result uses the regular schema version 1 plus:

```sql
CREATE TABLE export_metadata(
  exported_at TEXT NOT NULL,
  source_session_id INTEGER NOT NULL,
  partial INTEGER NOT NULL
);
```

`partial=1` means the source session had no end time at snapshot creation. It
includes only committed rows and retains `incomplete`, never fabricating a clean
session end. `exported_at` records export generation time, not sample time.
Downloads do not merge into another database or modify original history.

Only one export/stream is allowed at once: competing requests receive **409**.
Unknown sessions or database failures currently receive **500** with a generic
error; details are logged at the service. Invalid path/query types receive **400**.
There are no range/resume, mutation, device control, or waveform-query endpoints.
Temporary exports are removed when the response finishes or is dropped. Clients
write a unique `.part`, validate SQLite and foreign keys, then atomically finalize
without overwriting another file. Interrupted local downloads are removed.

## Lifecycle and resource limits

The HTTP listener binds before capture starts. USB reconnect backoff is
2/4/8/16/30 seconds; successful samples reset it. Each reconnect starts a new
calibrated session. Storage faults require a service restart after repair.
Replay EOF leaves the API available. SIGINT/SIGTERM cancels capture, drains the
accepted queue, finalizes the session and closes connections. Large snapshots
can retain WAL pages until export finishes; allow sufficient server disk space.
The database is never automatically pruned.
