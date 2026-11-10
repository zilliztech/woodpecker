#!/usr/bin/env python3
"""Browser acceptance of the real WAL backend using bounded HTTP fixtures.

Fixture observations are synthetic; the production UI has no simulated states.
No cluster, model credentials, model calls, or npm dependencies are required.
"""
import argparse
import http.server
import json
from pathlib import Path
import queue
import re
import signal
import subprocess
import tempfile
import threading
import time
import urllib.parse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wpdiag', default='bin/wpdiag')
    parser.add_argument('--output', default='/tmp/wpdiag-webui-evidence/wal')
    args = parser.parse_args()
    binary = str(Path(args.wpdiag).resolve())
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    catalog = json.loads((Path(__file__).parents[3] / 'diagnostics/evidence/catalog.json').read_text())
    hist = {m for e in catalog['evidence'] if e['kind'] in ('p99', 'errors') for m in e['metrics']}
    state = {'requests': 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            state['requests'] += 1
            if self.path.startswith('/slow/'):
                time.sleep(0.1)
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get('query', [''])[0]
            interval = re.search(r'\[(\d+)ms\] @ ([0-9.]+)$', query)
            end = float(interval[2])
            start = end - int(interval[1]) / 1000
            labels = {'cluster': 'fixture', 'instance': 'wp:9091', 'node_id': 'node-1'}
            for key, value in re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', query):
                if key != '__name__':
                    labels[key] = json.loads('"' + value + '"')
            match = re.search(r'__name__=~"(.*?)"', query)
            name = match[1].split('(')[0].removesuffix('_') if match else json.loads(re.search(r'__name__=("(?:[^"\\]|\\.)*")', query)[1])
            is_hist = name in hist and bool(match)
            base = name.removesuffix('_count') if not is_hist else name
            # A real raw matrix, with consistent cumulative buckets/count/sum.
            rows = []
            n = int((end - start) // 10)
            if is_hist:
                rows = [(name + '_count', {}, 100), (name + '_sum', {}, 50000)]
                rows += [(name + '_bucket', {'le': str(bound)}, 100 if bound >= 500 else 0) for bound in (10, 100, 1000)]
                rows.append((name + '_bucket', {'le': '+Inf'}, 100))
            else:
                value = 100 if ('requests' in name or 'bytes' in name or base in hist) else 0
                rows = [(name, {}, value)]
            result = []
            for metric, extra, step in rows:
                values = [[start + 1 + i * 10, str(i * step)] for i in range(n)]
                if metric.endswith('_segment') or metric.endswith('_entry'):
                    values = [[start + 1 + i * 10, '1'] for i in range(n)]
                result.append({'metric': dict(labels, __name__=metric, **extra), 'values': values})
            body = json.dumps({'status': 'success', 'data': {'resultType': 'matrix', 'result': result}}).encode()
            self.send_response(200)
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    backend = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=backend.serve_forever, daemon=True).start()
    proc = None
    try:
        with tempfile.TemporaryDirectory(prefix='wpdiag-wal-') as folder:
            root = Path(folder)
            config = {'schema_version': 'wpdiag.config/v1', 'sources': {'prometheus': {'connections': {
                'fixture': {'settings': {'endpoint': f'http://127.0.0.1:{backend.server_port}/slow', 'deployment': 'fixture', 'match_labels': {'cluster': 'fixture'}}}}}},
                'workbench': {'targets': {'fixture': {'label': 'Fixture / log 1', 'prometheus': 'fixture', 'log_namespace': 'fixture-tenant', 'log_id': '1', 'node_id': 'node-1'}}}}
            path = root / 'wpdiag.yaml'
            path.write_text(json.dumps(config))
            proc = subprocess.Popen([binary, 'serve', '--config', str(path), '--listen', '127.0.0.1:0'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            lines = queue.Queue()
            threading.Thread(target=lambda: [lines.put(line) for line in proc.stdout], daemon=True).start()
            deadline = time.monotonic() + 10
            url = None
            while time.monotonic() < deadline:
                try:
                    line = lines.get(timeout=0.2)
                    match = re.search(r'WebUI: (http://127\.0\.0\.1:\d+)', line)
                    if match:
                        url = match[1]
                        break
                except queue.Empty:
                    if proc.poll() is not None:
                        raise RuntimeError(proc.stderr.read())
            assert url, 'No workbench startup URL'
            result = subprocess.run(['node', str(Path(__file__).with_name('wal_browser.mjs')), url, str(output)], capture_output=True, text=True, timeout=150)
            (output / 'browser.stdout').write_text(result.stdout)
            (output / 'browser.stderr').write_text(result.stderr)
            assert result.returncode == 0, result.stderr + result.stdout
            print(json.dumps({'passed': True, 'checks': json.loads(result.stdout), 'source_requests': state['requests']}, indent=2))
    finally:
        if proc:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        backend.shutdown()
        backend.server_close()


if __name__ == '__main__':
    main()
