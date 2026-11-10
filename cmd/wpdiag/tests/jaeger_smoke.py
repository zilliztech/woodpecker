#!/usr/bin/env python3
"""Run WP-only atomic Trace checks against an isolated Jaeger 1.53 instance."""
import argparse
import datetime
import json
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
    parser.add_argument('--image', default='jaegertracing/all-in-one:1.53.0')
    args = parser.parse_args()
    binary = str(Path(args.wp).resolve())
    name = 'wp-trace-smoke-' + uuid.uuid4().hex[:10]
    started = False
    try:
        docker('run', '-d', '--rm', '--name', name, '-e', 'COLLECTOR_OTLP_ENABLED=true', '-e', 'SPAN_STORAGE_TYPE=memory',
               '-p', '127.0.0.1::16686', '-p', '127.0.0.1::4318', args.image)
        started = True
        ports = json.loads(docker('inspect', name))[0]['NetworkSettings']['Ports']
        query = 'http://127.0.0.1:' + ports['16686/tcp'][0]['HostPort']
        ingest = 'http://127.0.0.1:' + ports['4318/tcp'][0]['HostPort']
        deadline = time.monotonic() + 30
        while True:
            try:
                with urllib.request.urlopen(query + '/api/services', timeout=1):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(.1)
        begin = time.time_ns() - 20_000_000_000
        trace_id = uuid.uuid4().hex
        span = {'traceId': trace_id, 'spanId': '123456789abcdef0', 'name': 'AddEntry', 'kind': 1,
                'startTimeUnixNano': str(begin), 'endTimeUnixNano': str(begin + 2_000_000_000),
                'status': {'code': 2}, 'attributes': [{'key': 'error', 'value': {'boolValue': True}}]}
        packet = {'resourceSpans': [{'resource': {'attributes': [{'key': 'service.name', 'value': {'stringValue': 'wp'}}]},
                  'scopeSpans': [{'scope': {'name': 'LogStore'}, 'spans': [span]}]}]}
        request = urllib.request.Request(ingest + '/v1/traces', data=json.dumps(packet).encode(), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=5) as response:
            result = json.load(response)
        assert not result.get('partialSuccess', {}).get('rejectedSpans'), result
        deadline = time.monotonic() + 10
        while True:
            try:
                with urllib.request.urlopen(query + '/api/traces/' + trace_id, timeout=1) as response:
                    if json.load(response).get('data'):
                        break
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise RuntimeError('Published trace was not retained')
            time.sleep(.1)
        stamp = lambda n: datetime.datetime.fromtimestamp(n, datetime.timezone.utc).isoformat()
        results = []
        with tempfile.TemporaryDirectory(prefix='wp-trace-smoke-') as folder:
            path = Path(folder) / 'cli.yaml'
            path.write_text(json.dumps({'current-context': 'fixture', 'contexts': {'fixture': {
                'endpoint': 'http://unused.invalid:9091', 'observability': {'trace': {
                    'endpoint': query, 'deployment': 'fixture', 'services': {'wp': 'wp_server'}, 'search': {'service': 'wp'}}}}}}))
            for key in ['server-append-duration', 'server-append-errors']:
                for exact in [False, True]:
                    argv = [binary, 'diag', 'trace', 'check', key, '--config', str(path),
                            '--from', stamp(begin / 1e9 - 1), '--to', stamp(time.time() - 1), '--details', 'all', '-o', 'json']
                    if exact:
                        argv += ['--trace-id', trace_id]
                    proc = subprocess.run(argv, capture_output=True, text=True, timeout=20)
                    assert proc.returncode == 0, (key, proc.stderr, proc.stdout[:2000])
                    report = json.loads(proc.stdout)
                    assert len(report['checks']) == 1
                    check = report['checks'][0]
                    assert check['status'] == 'abnormal', (key, exact, check)
                    assert trace_id in check['trace_ids']
                    assert report['summary']['metrics']['status'] == 'not_selected'
                    results.append({'check': key, 'exact_trace': exact, 'status': check['status']})
        print(json.dumps({'passed': True, 'backend': 'Jaeger', 'image': args.image, 'cases': results}))
    finally:
        if started:
            subprocess.run(['docker', 'stop', '--time', '1', name], capture_output=True, timeout=15)


if __name__ == '__main__':
    main()
