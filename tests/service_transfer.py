"""Synthetic large export, busy/backpressure, failed destination and cancellation tests."""
import http.client
import http.server
import json
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

import tui_smoke
from service_smoke import free_port


def main():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        db = root / 'capture.db'
        addr = f'127.0.0.1:{free_port()}'
        base = f'http://{addr}/api/v1'

        def status():
            with opener.open(base + '/status', timeout=3) as response:
                return json.load(response)

        def start():
            proc = subprocess.Popen([str(tui_smoke.BINARY), '--service', '--mock', '--addr', addr, '--db', str(db)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    if status()['accepted']:
                        return proc
                except OSError:
                    pass
                time.sleep(.1)
            proc.kill()
            raise AssertionError(proc.communicate())

        proc = start()
        proc.send_signal(signal.SIGTERM)
        proc.communicate(timeout=8)
        # This is generated data, never a real capture fixture.
        with sqlite3.connect(db) as conn:
            conn.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<60000) INSERT INTO measurements(session_id,ts,device_id,voltage_v,current_a,power_w,energy_wh,status,raw) SELECT 1,'2026-01-01T00:00:00Z','synthetic',5,0.1,0.5,0,'test',zeroblob(1024) FROM n")
            count = conn.execute('SELECT count(*) FROM measurements WHERE session_id=1').fetchone()[0]
            conn.execute('UPDATE sessions SET accepted_count=?,received_count=?,saved_count=? WHERE id=1', (count, count, count))
        proc = start()
        held = None
        try:
            before = status()['accepted']
            held = http.client.HTTPConnection(addr, timeout=20)
            held.connect()
            held.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            held.request('GET', '/api/v1/sessions/1/download')
            response = held.getresponse()
            assert response.status == 200
            try:
                opener.open(base + '/sessions/1/download', timeout=3)
                raise AssertionError('Concurrent export should be busy')
            except urllib.error.HTTPError as error:
                assert error.code == 409
            latencies = []
            for _ in range(5):
                at = time.monotonic()
                live = status()
                latencies.append(time.monotonic() - at)
                time.sleep(.2)
            assert live['accepted'] > before
            assert max(latencies) < 2, latencies
            rss_kb = int(next(line.split()[1] for line in Path(f'/proc/{proc.pid}/status').read_text().splitlines() if line.startswith('VmRSS:')))
            assert rss_kb < 100_000, rss_kb
            response.close()
            held.close()
            held = None
            deadline = time.monotonic() + 10
            while True:
                try:
                    with opener.open(base + '/sessions/1/download', timeout=20) as download:
                        with (root / 'complete.db').open('wb') as target:
                            shutil.copyfileobj(download, target, length=65536)
                    break
                except urllib.error.HTTPError as error:
                    assert error.code == 409 and time.monotonic() < deadline
                    time.sleep(.1)
            with sqlite3.connect(root / 'complete.db') as conn:
                assert conn.execute('SELECT count(*) FROM measurements').fetchone()[0] == count
                assert conn.execute('SELECT partial FROM export_metadata').fetchone()[0] == 0
                assert conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
            print(f'PASS large selected-session export, busy response, cancellation releases slot, status responsive; RSS={rss_kb} KiB')
            blocked = root / 'blocked'
            blocked.write_text('a file, not a directory')
            code, output = tui_smoke.run_tui(['--client', '--addr', addr, '--download-dir', str(blocked)], [(.5, b'd'), (1.5, b'q')])
            assert code == 0, output[-2000:]
            assert blocked.read_text() == 'a file, not a directory'
            print('PASS failed download destination leaves service and destination intact')
            snapshot = status()
        finally:
            if held:
                held.close()
            proc.send_signal(signal.SIGTERM)
            proc.communicate(timeout=8)

        # Slow HTTP peer makes cancellation deterministic, including stalled reads.
        class SlowPeer(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                if self.path.endswith('/status'):
                    body = json.dumps(snapshot).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_response(200)
                    self.send_header('Content-Length', '100000000')
                    self.end_headers()
                    try:
                        self.wfile.write(b'SQLite format 3\x00')
                        self.wfile.flush()
                        time.sleep(8)
                    except (BrokenPipeError, ConnectionResetError):
                        pass

        peer = http.server.ThreadingHTTPServer(('127.0.0.1', 0), SlowPeer)
        thread = threading.Thread(target=peer.serve_forever, daemon=True)
        thread.start()
        try:
            downloads = root / 'interrupted'
            started = time.monotonic()
            code, output = tui_smoke.run_tui(['--client', '--addr', f'127.0.0.1:{peer.server_port}', '--download-dir', str(downloads)], [(.6, b'd'), (1.2, b'\x03')], timeout=5)
            assert code == 0 and time.monotonic() - started < 4, output[-2000:]
            assert not list(downloads.glob('*')), list(downloads.glob('*'))
            print('PASS stalled download cancelled promptly; partial file removed; terminal restored')
        finally:
            peer.shutdown()
            peer.server_close()


if __name__ == '__main__':
    main()
