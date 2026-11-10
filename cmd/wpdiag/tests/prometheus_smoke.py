#!/usr/bin/env python3
"""Run WP-only atomic Metrics checks against an isolated real Prometheus TSDB."""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid


def docker(*args):
    return subprocess.check_output(['docker', *args], text=True, stderr=subprocess.PIPE).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wp', default='bin/wp')
    parser.add_argument('--image', default='quay.io/prometheus/prometheus:v3.0.1')
    args = parser.parse_args()
    binary = str(Path(args.wp).resolve())
    name = 'wp-metrics-smoke-' + uuid.uuid4().hex[:10]
    begin = int(time.time()) - 3600
    end = begin + 300
    metric = 'woodpecker_server_logstore_operation_latency'
    results = []
    with tempfile.TemporaryDirectory(prefix='wp-metrics-smoke-') as folder:
        root = Path(folder)
        lines = []
        for case, latency in [('slow', 3000), ('healthy', 50), ('idle', 0), ('sparse', 3000)]:
            for index in range(31):
                if case == 'sparse' and index not in (0, 30):
                    continue
                stamp = begin + index * 10
                count = 0 if case == 'idle' else index * 200
                labels = {'cluster': 'fixture', 'case': case, 'instance': 'wp:9091', 'node_id': 'node-1',
                          'log_ns': 'tenant/wal', 'log_id': '7', 'operation': 'get_batch_entries', 'status': 'success'}
                rows = [(metric + '_count', {}, count), (metric + '_sum', {}, count * latency)]
                rows += [(metric + '_bucket', {'le': str(upper)}, count if upper >= latency else 0) for upper in [100, 256, 2048, 4096]]
                rows.append((metric + '_bucket', {'le': '+Inf'}, count))
                for family, extra, value in rows:
                    identity = ','.join(k + '=' + json.dumps(v) for k, v in sorted(dict(labels, **extra).items()))
                    lines.append(f'{family}{{{identity}}} {value} {stamp}')
        (root / 'samples.openmetrics').write_text('\n'.join(lines) + '\n# EOF\n')
        (root / 'prometheus.yaml').write_text('global:\n  scrape_interval: 10s\nscrape_configs: []\n')
        identity = f'{os.getuid()}:{os.getgid()}'
        docker('run', '--rm', '--user', identity, '-v', str(root) + ':/work', '--entrypoint', '/bin/promtool',
               args.image, 'tsdb', 'create-blocks-from', 'openmetrics', '/work/samples.openmetrics', '/work/data')
        started = False
        try:
            docker('run', '--rm', '-d', '--name', name, '--user', identity, '-p', '127.0.0.1::9090',
                   '-v', str(root) + ':/work', args.image, '--config.file=/work/prometheus.yaml', '--storage.tsdb.path=/work/data')
            started = True
            endpoint = 'http://' + docker('port', name, '9090/tcp').splitlines()[0]
            deadline = time.monotonic() + 30
            while True:
                try:
                    with urllib.request.urlopen(endpoint + '/-/ready', timeout=1):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(.1)
            for case, expected in [('slow', 'abnormal'), ('healthy', 'clear'), ('idle', 'unknown'), ('sparse', 'unknown')]:
                path = root / 'cli.yaml'
                path.write_text(json.dumps({'current-context': 'fixture', 'contexts': {'fixture': {
                    'endpoint': 'http://unused.invalid:9091', 'observability': {'prometheus': {
                        'endpoint': endpoint, 'deployment': 'fixture', 'match_labels': {'cluster': 'fixture', 'case': case}}}}}}))
                stamp = lambda n: datetime.datetime.fromtimestamp(n, datetime.timezone.utc).isoformat()
                proc = subprocess.run([binary, 'diag', 'metrics', 'check', 'server-read-latency', '--config', str(path),
                    '--from', stamp(begin), '--to', stamp(end), '--details', 'all', '-o', 'json'], capture_output=True, text=True, timeout=20)
                assert proc.returncode == 0, (case, proc.stderr, proc.stdout[:1500])
                report = json.loads(proc.stdout)
                assert len(report['checks']) == 1
                check = report['checks'][0]
                assert check['status'] == expected, (case, check)
                assert check['execution'] == 'completed', (case, check)
                assert report['summary']['trace']['status'] == 'not_selected'
                results.append({'case': case, 'status': check['status']})
            print(json.dumps({'passed': True, 'backend': 'Prometheus', 'image': args.image, 'cases': results}))
        finally:
            if started:
                subprocess.run(['docker', 'stop', '--time', '1', name], capture_output=True, timeout=15)


if __name__ == '__main__':
    main()
