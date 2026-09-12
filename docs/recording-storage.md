# Recording and storage

## Pipeline and memory

CC input remains 10,000 samples/s. Acquisition decodes packets into an eight-message
bounded ingress queue. A separate processor owns full-rate statistics, energy
observations and 100 ms chart buckets; it publishes immutable display snapshots
about every 100 ms. UI rendering and HTTP snapshots do not take database locks.
Display reset changes only these statistics/history, not recording or energy.

Recording uses two ownership-transferred buffers, each defaulting to 10,000,000
bytes. Accounting includes `RecordedBatch`, allocated record-vector capacity,
measurement strings/raw bytes, USB frame capacity and boxed aggregate metadata.
The resampler's one current aggregate, bounded ingress, deque/allocator bookkeeping,
SQLite caches (8 MiB per capture writer) and display snapshots are additional memory; 20 MB is not an RSS cap.
One complete input block can exceed a threshold; the already accepted ingress tail
is retained on overflow. Built-in USB blocks contain at most 800 samples / 3212 raw
bytes; JSONL lines are bounded to 1 MiB. No unlimited spill queue is used.

Capacity transfers a buffer without cloning its data. Transactions consume at most
32 blocks and INSERT statements at most 64 records; counts advance after commit.
Capture commits keep SQLite synchronous durability and fsync the WAL. Automatic
WAL checkpoints are disabled on capture connections; session finalization performs
a passive checkpoint. This avoids copying pages back to the SD database on every
buffer write. Keep the `.db-wal` with the database while capture is running; use the
API for a standalone snapshot. Long sessions therefore require extra WAL disk space.
SQLite allocator status counters are disabled at build time to reduce mutex overhead;
this does not disable thread safety or durability.

There is **no timed measurement flush**. Session metadata may be written earlier.
Stop, normal exit, configuration application and active download explicitly drain
partial buffers. On a write failure the transaction rolls back and its records stay
in memory; previously committed blocks are removed, so retry cannot duplicate them.
A full second buffer stops capture with an explicit backlog error. Forced termination
or power loss can lose all uncommitted memory. Local save refuses to report success
until accepted input is represented on disk. Service shutdown retains failed buffers
and retries the final flush every two seconds until storage is repaired.

## Software recording rate

`sample_rate_hz` is an integer 1–10000, with 10000 as the default. The default saves
each input sample without allocating duplicate extrema. Lower rates assign timestamps
to session-relative intervals using `floor((timestamp_us-origin_us)*rate/1000000)`;
integer arithmetic supports 333 Hz without rounding drift across packets. Other
sources retain their timestamps and are never upsampled.

Completed CC intervals are not marked partial solely because capture stops exactly
at their end.

An aggregate stores the arithmetic mean of each input voltage, current and power,
plus independent min/max. Power is averaged directly, not multiplied from mean V/A.
Its energy is the last input's cumulative Wh, integrated by the original source at
full input rate. Gaps, invalid sample indices, backwards time, barriers and session
end close intervals; no synthetic samples fill gaps. Early closure sets `partial=1`.
A backwards clock resets the time origin. Native USB raw frames and calibration
remain intact regardless of recording rate.

## Schema version 2

Existing version 0/1 databases migrate transactionally without deleting history.
Old records default to `source_count=1`; old `saved_source_count` is backfilled from
`saved_count`. Version 2 adds:

| Table | Added columns |
| --- | --- |
| `sessions` | `sample_rate_hz`, `buffer_size_bytes`, `saved_source_count` |
| `frames` | `capture_sequence` (session-local ordering, distinct from wrapping device packet ID) |
| `measurements` | `source_count`, `end_ts`, `end_frame_id`, `end_sample_index`, `partial`, `voltage_min/max`, `current_min/max`, `power_min/max` |

For full-rate USB rows, repeated `device_id` and the fixed
`estimated_timestamp` status are inherited from the session instead of duplicated
on every row. Use `SELECT * FROM measurement_details` to resolve them. This view
retains the measurement column names and also reads older explicit values. Raw frames
and per-sample numeric values are unchanged; aggregate rows retain explicit metadata.

`ts`, `frame_id`, `sample_index` identify the first input; the new endpoint columns
identify the last. Intermediate raw frames lie between those session-local capture
sequences. For native singleton records, endpoint/extrema columns are NULL; use the
original timestamp/frame/value instead. Aggregates may contain only one input after
a cut, so inspect endpoint columns rather than assuming `source_count>1`.

`accepted_count` counts input samples, `saved_count` counts committed rows, and
`saved_source_count` counts the input samples those rows represent. Normal downsampling
does not increase drops or mark a session incomplete. Completed sessions require
accepted input to match represented input, with no gaps, invalid samples or faults.
Session counters during capture describe committed data; live status also includes
pending input and records. All physical units remain V, A, W and Wh.

Temporary capture imports use one transaction, map session IDs and both endpoint
frame IDs, and retain old target history. Active-session export flushes through a
FIFO input barrier and pins a WAL snapshot before later writes. SQL copies at most
256 rows per batch into the export, yielding to capacity writes while capture is
active; the source is read-only. Linux exports run on a dedicated thread with
niceness at least 10, preserving acquisition/writer CPU priority without changing
the shared async worker pool. Post-barrier samples
continue recording but are excluded. Exported active sessions remain incomplete
with `export_metadata.partial=1`. Downloading cannot silently omit failed pending data.
