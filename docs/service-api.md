# Service API v1

Start `iot-power-tui --service [--addr 0.0.0.0:8080] [--db PATH]`.
The API provides read access and recording configuration over trusted-LAN HTTP, without authentication or TLS.
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
| `received`, `accepted` | Input sample counters in the current session |
| `records`, `saved`, `saved_source` | Generated record rows, committed rows, and input samples represented by committed rows |
| `buffered_bytes`, `writing_bytes` | Total pending bytes and its sealed/writing subset; not process RSS |
| `config` | Current session recording settings (`buffer_size_bytes`, `sample_rate_hz`) |
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
For an active session, the server inserts a FIFO input barrier, closes the current
partial aggregation interval and flushes all preceding records. The writer pins a
read-only WAL transaction before processing later data. Capture continues; post-barrier
records are excluded. Draining/faulted sessions with unsaved data return an error
instead of exporting stale disk contents.

The server copies only the
selected session, its frames and measurements into a temporary SQLite file.
On Linux, export uses a dedicated thread with niceness at least 10, giving
acquisition and recording priority. Each SQL copy batch has at most 256 rows; SQLite streams them directly, avoiding
per-value Rust allocations, and yields to capacity writes on the capture path.
The `measurement_details` view resolves session-inherited device/status metadata.
IDs and both endpoint frame references remain intact. A WAL
read snapshot keeps the exported rows consistent while capture continues.

The result uses the regular schema version 2 plus:

```sql
CREATE TABLE export_metadata(
  exported_at TEXT NOT NULL,
  source_session_id INTEGER NOT NULL,
  partial INTEGER NOT NULL
);
```

`partial=1` means the source session had no end time at snapshot creation. It
includes rows flushed through the input barrier and retains `incomplete`, never fabricating a clean
session end. `exported_at` records export generation time, not sample time.
Downloads do not merge into another database or modify original history.

Only one export/stream is allowed at once: competing requests receive **409**.
Unknown sessions or database failures currently receive **500** with a generic
error; details are logged at the service. Invalid path/query types receive **400**.
There are no range/resume, device-output control, or waveform-query endpoints.
Temporary exports are removed when the response finishes or is dropped. Clients
write a unique `.part`, validate SQLite and foreign keys, then atomically finalize
without overwriting another file. Interrupted local downloads are removed.

## Lifecycle and resource limits

The HTTP listener binds before capture starts. USB reconnect backoff is
2/4/8/16/30 seconds; successful samples reset it. Each reconnect starts a new
calibrated session. Storage faults stop capture and retain uncommitted memory.
After repair, reapply configuration to retry the drain before a new session.
Replay EOF leaves the API available. SIGINT/SIGTERM cancels capture, drains the
accepted queue, finalizes the session and closes connections. If the final flush
fails, the process retains buffers and retries every two seconds; repair storage
and allow it to finish. Forcing termination loses those memory buffers. Large snapshots
can retain WAL pages until export finishes; allow sufficient server disk space.
The database is never automatically pruned.

## Runtime configuration

`GET /api/v1/config` returns:

```json
{"buffer_size_bytes":10000000,"sample_rate_hz":10000,"revision":0,"state":"idle","pending":null,"error":null}
```

`PUT /api/v1/config` accepts all three fields:

```json
{"buffer_size_bytes":10000000,"sample_rate_hz":333,"revision":0}
```

Bytes must be 64000–256000000; rate must be an integer 1–10000. Invalid
configuration returns **400** (malformed JSON/types may return **422**).
A stale revision or another application in progress returns **409**. An identical
idle configuration returns **200** without restarting capture. Changes return
**202** with `state="applying"` and `pending` settings; poll GET for completion.
The service drains/ends the old session in the background before starting the new
one. Live status remains available. On success `revision` increments, `state`
becomes `idle`, and `pending` clears. Failure leaves `state="failed"`, an error,
the old configuration/revision and retained buffers; repair storage and PUT again
with the current revision to retry. No new capture starts after a failed drain.
Settings are process-local and are not persisted as the next launch defaults.

The hardware remains at 10 kHz; this config only changes software recording.
No authentication is provided: any host able to reach the service can change
recording settings. Deploy on the same trusted LAN as existing client access.

## Compact live representation

New Clients request `GET /api/v1/status` with
`Accept: application/vnd.iot-power.live-v1`. This avoids serializing thousands of
chart floats to decimal on ARMv6. Without that exact Accept header, JSON remains
unchanged; new Clients also accept JSON from older services.

The binary response starts with eight ASCII bytes `IPLIVE01`, followed by a
little-endian u32 JSON-header length and u16 bucket count. The header is the usual
Live JSON object with an empty `buckets` array (maximum 16384 bytes). Each of at most
600 buckets then occupies 112 bytes: i64 `time`, u64 `segment`, then voltage/current/
power statistics, each u64 `count` followed by f64 `mean`, `min`, `max`. All words
are little-endian and floats retain their IEEE-754 bits. Exact payload length,
finite statistics and bounds are checked before allocation/rendering.
