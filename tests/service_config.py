"""Settings, drain failures/retry, aggregation and Client/local editor PTY tests."""
import json
import signal
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
import tui_smoke
from service_smoke import free_port


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        target = root / 'local.db'
        code, output = tui_smoke.run_tui(['--mock', '--db', str(target)],
            [(.4, b'c'), (.6, b'\x1b'), (.8, b'c'), (1., b'10M\t1\r'),
             (2.4, b'q'), (2.6, b'y')])
        assert code == 0, output[-2000:]
        with sqlite3.connect(target) as c:
            sessions = c.execute('SELECT sample_rate_hz,accepted_count,saved_count,saved_source_count,outcome FROM sessions ORDER BY id').fetchall()
            assert [s[0] for s in sessions] == [10000, 1], sessions
            assert all(a == represented and outcome == 'complete' for _, a, _, represented, outcome in sessions)
            assert sessions[1][1] > sessions[1][2]
        print('PASS local settings cancel/apply/drain/new session/aggregate save')

        addr = '127.0.0.1:%s' % free_port()
        base = 'http://%s/api/v1' % addr
        db = root / 'service.db'
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        def request(path, data=None):
            req = urllib.request.Request(base+path, data=None if data is None else json.dumps(data).encode(), headers={'Content-Type': 'application/json'}, method='GET' if data is None else 'PUT')
            with opener.open(req, timeout=10) as response: return json.load(response)
        def wait(predicate):
            deadline = time.monotonic()+10
            while time.monotonic() < deadline:
                try:
                    result = predicate()
                    if result: return result
                except OSError: pass
                time.sleep(.05)
            raise AssertionError('Timed out waiting for service')
        def apply(rate, revision):
            return request('/config', dict(buffer_size_bytes=10000000, sample_rate_hz=rate, revision=revision))
        service = subprocess.Popen([str(tui_smoke.BINARY), '--service', '--mock', '--addr', addr, '--db', str(db)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            wait(lambda: request('/status')['accepted'] >= 3)
            original = request('/status')['session_id']
            assert apply(10000, 0)['state'] == 'idle'
            time.sleep(.2)
            assert request('/status')['session_id'] == original
            with sqlite3.connect(db) as c:
                c.execute("CREATE TRIGGER fail_write BEFORE INSERT ON measurements BEGIN SELECT RAISE(FAIL,'test write failure'); END")
            apply(1, 0)
            wait(lambda: request('/config')['state'] == 'failed')
            assert request('/status')['session_id'] == original
            assert request('/status')['buffered_bytes'] > 0
            with sqlite3.connect(db) as c: c.execute('DROP TRIGGER fail_write')
            apply(1, 0)
            wait(lambda: request('/config')['revision'] == 1)
            wait(lambda: request('/status')['session_id'] != original and request('/status')['accepted'] >= 3)
            try:
                apply(333, 0)
                raise AssertionError('Stale version accepted')
            except urllib.error.HTTPError as e: assert e.code == 409
            print('PASS idempotency, failed drain retains cache, retry and stale client conflict')
            code, output = tui_smoke.run_tui(['--client', '--addr', addr],
                [(.4, b'c'), (.9, b'\x1b'), (1.1, b'c'), (1.6, b'10M\t333\r'), (3.2, b'q')])
            assert code == 0, output[-2000:]
            cfg = request('/config')
            assert cfg['sample_rate_hz'] == 333 and cfg['revision'] == 2, cfg
            current = request('/status')
            with opener.open(base+'/sessions/%s/download' % current['session_id'], timeout=10) as response:
                exported = root / 'active.db'
                exported.write_bytes(response.read())
            with sqlite3.connect(exported) as c:
                n = c.execute('SELECT sum(source_count) FROM measurements').fetchone()[0]
                assert n >= current['accepted']
                assert c.execute('SELECT saved_source_count FROM sessions').fetchone()[0] == n
                assert not c.execute('PRAGMA foreign_key_check').fetchall()
            time.sleep(.3)
            assert request('/status')['accepted'] > n
            print('PASS Client settings/cancel/apply/live polling and active download barrier')
        finally:
            service.send_signal(signal.SIGTERM)
            _, stderr = service.communicate(timeout=10)
            assert service.returncode == 0, stderr
        with sqlite3.connect(db) as c:
            assert all(a == represented for a, represented in c.execute('SELECT accepted_count,saved_source_count FROM sessions'))
            assert c.execute('SELECT count(*) FROM measurements').fetchone()[0] == c.execute('SELECT sum(saved_count) FROM sessions').fetchone()[0]


if __name__ == '__main__':
    main()
