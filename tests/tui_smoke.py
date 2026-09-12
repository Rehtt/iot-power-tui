"""Unix PTY tests: cargo build && python3 tests/tui_smoke.py."""
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import sqlite3
import struct
import subprocess
import tempfile
import termios
import time

ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / "target/debug/iot-power-tui"


def run_tui(arguments, actions, timeout=15):
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 140, 0, 0))
    before = termios.tcgetattr(slave)
    process = subprocess.Popen(
        [str(BINARY), *arguments], stdin=slave, stdout=slave, stderr=slave,
        env={**os.environ, "TERM": "xterm-256color"},
    )
    started = time.monotonic()
    output = bytearray()
    try:
        while process.poll() is None:
            if select.select([master], [], [], 0.02)[0]:
                output.extend(os.read(master, 65536))
            elapsed = time.monotonic() - started
            while actions and elapsed >= actions[0][0]:
                action = actions.pop(0)[1]
                if callable(action):
                    action(master, output)
                else:
                    os.write(master, action)
            if elapsed > timeout:
                raise AssertionError("TUI did not exit within timeout")
        while select.select([master], [], [], 0)[0]:
            output.extend(os.read(master, 65536))
        assert before == termios.tcgetattr(slave), "Terminal settings not restored"
        assert b"\x1b[?1049l" in output, "Alternate screen not restored"
        return process.returncode, output
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        os.close(master)
        os.close(slave)


def rows(database):
    with sqlite3.connect(database) as connection:
        return connection.execute(
            "SELECT accepted_count,saved_count,outcome,ended_at FROM sessions"
        ).fetchall()


def pending(parent):
    return list((parent / ".iot-power-pending").glob("capture-*/capture.db"))


def sample_count(database):
    with sqlite3.connect(database) as connection:
        return connection.execute("SELECT count(*) FROM measurements").fetchone()[0]


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        target = root / "final.db"
        observed = []

        def check_unsaved(_, output):
            assert not target.exists(), "Destination changed before save confirmation"
            captures = pending(root)
            assert len(captures) == 1
            observed.append(sample_count(captures[0]))

        code, output = run_tui(
            ["--mock", "--db", str(target)],
            [(0.3, b"1]3[2r"), (0.7, b"q"), (1.0, check_unsaved),
             (1.4, check_unsaved), (1.5, b"\x1b"), (1.8, b"s"), (2.2, b"s"),
             (2.7, b"\x03"), (3.0, b"\r")],
        )
        assert code == 0, output[-3000:]
        assert observed[1] > observed[0], "Capture stopped while exit dialog was open"
        sessions = rows(target)
        assert len(sessions) == 2, sessions
        assert all(a == s and a > 0 and o == "complete" and e for a, s, o, e in sessions)
        assert not pending(root)
        print("PASS charts/confirm/cancel/continued capture/restart/save/Ctrl+C")

        original_count = sample_count(target)
        code, output = run_tui(["--mock", "--db", str(target)], [(0.6, b"q"), (0.9, b"n")])
        assert code == 0, output[-2000:]
        assert sample_count(target) == original_count and rows(target) == sessions
        assert not pending(root)
        print("PASS discard preserves historical sessions")

        blocked = root / "blocked.db"
        blocked.mkdir()

        def unblock(_, output):
            assert len(pending(root)) == 1 and sample_count(pending(root)[0]) > 0
            blocked.rmdir()

        code, output = run_tui(
            ["--mock", "--db", str(blocked)],
            [(0.6, b"q"), (0.9, b"y"), (1.7, unblock), (2.0, b"\r")],
        )
        assert code == 0 and blocked.is_file(), output[-3000:]
        assert len(rows(blocked)) == 1 and not pending(root)
        assert rows(blocked)[0][0] == sample_count(blocked)
        print("PASS failed save preserves cache and retries without duplication")

        replay = root / "fault.jsonl"
        measurement = {"timestamp": "2026-01-01T00:00:00Z", "device_id": "synthetic",
                       "voltage_v": 5, "current_a": 0.1, "power_w": 0.5,
                       "energy_wh": 0, "status": "test", "raw": []}
        replay.write_text(json.dumps(measurement) + "\ninvalid json\n")
        fault_target = root / "fault.db"
        code, output = run_tui(
            ["--replay", str(replay), "--db", str(fault_target)],
            [(0.7, b"q"), (1.0, b"y")],
        )
        assert code == 1 and rows(fault_target)[0][2] == "incomplete", output[-3000:]
        print("PASS faulted capture can be saved with incomplete status")

        empty = root / "empty.jsonl"
        empty.touch()
        empty_target = root / "empty.db"
        code, output = run_tui(
            ["--replay", str(empty), "--db", str(empty_target)], [(0.5, b"q")],
        )
        assert code == 0 and not empty_target.exists() and not pending(root)
        print("PASS empty capture exits without save dialog")

    result = subprocess.run([str(BINARY)], capture_output=True, check=False)
    assert result.returncode == 2
    print("PASS explicit source validation")


if __name__ == "__main__":
    main()
