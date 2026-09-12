"""HTTP + PTY service/client regression. Run after cargo build (Unix)."""
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
import tui_smoke

BINARY = tui_smoke.BINARY


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        db = root / 'service.db'
        addr = f'127.0.0.1:{free_port()}'
        base = f'http://{addr}/api/v1'
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def status():
            with opener.open(base + '/status', timeout=3) as response:
                return json.load(response)

        def start():
            process = subprocess.Popen([str(BINARY), '--service', '--mock', '--addr', addr, '--db', str(db)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    if status()['accepted'] > 0:
                        return process
                except OSError:
                    pass
                time.sleep(.1)
            process.kill()
            raise AssertionError(process.communicate())

        service = start()
        try:
            first = status()
            with concurrent.futures.ThreadPoolExecutor(3) as pool:
                results = list(pool.map(lambda _: status(), range(3)))
            assert all(s['instance'] == first['instance'] for s in results)
            code, output = tui_smoke.run_tui(['--client', '--addr', addr, '--download-dir', str(root / 'downloads')], [(0.5, b'1]3[2'), (.8, b'h'), (1.1, b'd'), (2.5, b'\x1b'), (2.8, b'q')])
            assert code == 0, output[-2000:]
            assert service.poll() is None
            assert status()['saved'] > first['saved']
            downloads = list((root / 'downloads').glob('*.db'))
            assert len(downloads) == 1, downloads
            assert not list((root / 'downloads').glob('*.part'))
            with sqlite3.connect(downloads[0]) as conn:
                assert conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                assert conn.execute('SELECT partial FROM export_metadata').fetchone()[0] == 1
                n = conn.execute('SELECT count(*) FROM measurements').fetchone()[0]
                assert n > 0
                assert conn.execute('SELECT saved_count FROM sessions').fetchone()[0] == n
            print('PASS concurrent clients, live refresh, session browser, active download, client exit independence')

            def restart(_, output):
                nonlocal service
                service.send_signal(signal.SIGTERM)
                service.communicate(timeout=8)
                time.sleep(.5)
                service = start()

            code, output = tui_smoke.run_tui(['--client', '--addr', addr], [(.7, restart), (2.5, b'\x03')])
            assert code == 0, output[-2000:]
            assert status()['instance'] != first['instance']
            print('PASS service restart, client reconnect and Ctrl+C terminal restoration')
        finally:
            service.send_signal(signal.SIGTERM)
            stdout, stderr = service.communicate(timeout=8)
            assert service.returncode == 0, stderr
        with sqlite3.connect(db) as conn:
            rows = conn.execute('SELECT accepted_count,saved_count,outcome,ended_at FROM sessions').fetchall()
            assert len(rows) == 2
            assert all(a == s and s > 0 and o == 'complete' and e for a, s, o, e in rows), rows
        print('PASS SIGTERM drains accepted data and preserves historical sessions')

        replay = root / 'synthetic.jsonl'
        replay.write_text(json.dumps(dict(timestamp='2026-01-01T00:00:00Z', device_id='synthetic', voltage_v=5, current_a=.1, power_w=.5, energy_wh=0, status='test', raw=[])) + '\n')
        service = subprocess.Popen([str(BINARY), '--service', '--replay', str(replay), '--addr', addr, '--db', str(db)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    current = status()
                    if current['state'] == 'Stopped' and current['saved'] == 1:
                        break
                except OSError:
                    pass
                time.sleep(.1)
            else:
                raise AssertionError('Replay did not finish')
            time.sleep(.5)
            assert status()['session_id'] == current['session_id']
            with opener.open(base + '/sessions', timeout=3) as response:
                assert len(json.load(response)) == 3
            print('PASS replay EOF keeps history API available without restarting capture')
        finally:
            service.send_signal(signal.SIGTERM)
            service.communicate(timeout=8)
        with socket.socket() as occupied:
            occupied.bind(('127.0.0.1', 0))
            occupied.listen()
            target = root / 'must-not-exist.db'
            result = subprocess.run([str(BINARY), '--service', '--mock', '--addr', f'127.0.0.1:{occupied.getsockname()[1]}', '--db', str(target)], capture_output=True, timeout=5)
            assert result.returncode != 0 and not target.exists()
        print('PASS failed bind does not start capture or create a database')


if __name__ == '__main__':
    main()
