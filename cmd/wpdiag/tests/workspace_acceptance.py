#!/usr/bin/env python3
"""CLI + wp webui acceptance against bounded Metrics/Trace/Admin fixtures.

No cluster, model account, model request or npm package is used.
"""
import argparse
import copy
import http.server
import json
import os
from pathlib import Path
import queue
import re
import subprocess
import tempfile
import threading
import time
import urllib.parse


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--wp', default='bin/wp')
    p.add_argument('--cli-only', action='store_true')
    p.add_argument('--output', default='/tmp/wpdiag-webui-evidence/workspace')
    args = p.parse_args()
    binary = str(Path(args.wp).resolve())
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    catalog = json.loads((Path(__file__).parents[3] / 'diagnostics/evidence/catalog.json').read_text())
    hist = {m for e in catalog['evidence'] if e['kind'] in ('p99', 'errors') for m in e['metrics']}
    state = {'metrics': 0, 'trace': 0, 'admin': 0, 'mutations': 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            if parsed.path.startswith(('/failed/', '/unauthorized/')):
                state['metrics'] += 1
                self.send_error(401 if parsed.path.startswith('/unauthorized/') else 503)
                return
            if parsed.path.endswith('/api/v1/query') and query.get('query') == ['vector(1)']:
                state['metrics'] += 1
                data = {'status': 'success', 'data': {'resultType': 'vector', 'result': [{'metric': {}, 'value': [time.time(), '1']}]}}
            elif parsed.path.endswith('/api/services'):
                state['trace'] += 1
                data = {'data': ['wp']}
            elif parsed.path.startswith('/empty/'):
                state['metrics'] += 1
                data = {'status': 'success', 'data': {'resultType': 'matrix', 'result': []}}
            elif parsed.path.endswith('/api/traces'):
                state['trace'] += 1
                start = int(query.get('start', [str(int((time.time()-60)*1e6))])[0])
                trace_id = '1' * 32
                data = {'data': [{'traceID': trace_id, 'spans': [{
                    'traceID': trace_id, 'spanID': '2'*16, 'operationName': 'AddEntry',
                    'processID': 'p', 'startTime': start+1000000, 'duration': 2000000,
                    'tags': [{'key': 'error', 'type': 'bool', 'value': True}]}],
                    'processes': {'p': {'serviceName': 'wp', 'tags': [
                        {'key': 'otel.scope.name', 'type': 'string', 'value': 'LogStore'},
                        {'key': 'service.instance.id', 'type': 'string', 'value': 'node-1'}]}}}]}
                minimum = query.get('minDuration', [''])[0]
                units = {'ns': 1e-9, 'us': 1e-6, 'µs': 1e-6, 'ms': 1e-3, 's': 1, 'm': 60, 'h': 3600}
                seconds = sum(float(n) * units[u] for n, u in re.findall(r'([0-9.]+)(ns|us|µs|ms|s|m|h)', minimum))
                tags = json.loads(query.get('tags', ['{}'])[0])
                if query.get('operation', ['AddEntry'])[0] != 'AddEntry' or seconds > 2 or tags.get('otel.status_code'):
                    data = {'data': []}

            elif parsed.path == '/admin/runtime/ops':
                state['admin'] += 1
                data = [{'op_id': 'fixture-op', 'op_type': 'file.flush', 'log_id': 7, 'segment_id': 9, 'query': parsed.query}]
            elif parsed.path == '/admin/node/status':
                state['admin'] += 1
                data = {'state': 'active'}
            elif parsed.path == '/admin/logstore/segments':
                state['admin'] += 1
                data = {'segments': [{'log_id': 7, 'segment_id': 9, 'flush_queue_depth': 2, 'flush_queue_capacity': 16}]}
            elif parsed.path == '/admin/memberlist':
                state['admin'] += 1
                data = {'members': [{'id': 'node-1', 'state': 0, 'service_addr': 'node-1:18080', 'region': 'fixture', 'az': 'zone-a'}]}
            else:
                state['metrics'] += 1
                time.sleep(0.025)
                expr = query.get('query', [''])[0]
                interval = re.search(r'\[(\d+)ms\] @ ([0-9.]+)$', expr)
                if not interval:
                    self.send_error(400)
                    return
                end = float(interval[2])
                start = end-int(interval[1])/1000
                labels = {'cluster': 'fixture', 'instance': 'wp:9091', 'node_id': 'node-1'}
                for key, value in re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', expr):
                    if key != '__name__':
                        labels[key] = json.loads('"'+value+'"')
                if 'status=~"error|error_get_processor"' in expr:
                    labels['status'] = 'error_get_processor'
                match = re.search(r'__name__=~"(.*?)"', expr)
                name = match[1].split('(')[0].removesuffix('_') if match else json.loads(re.search(r'__name__=("(?:[^"\\]|\\.)*")', expr)[1])
                n = int((end-start)//10)
                rows = []
                if name in hist and match:
                    rows = [(name+'_count', {}, 200), (name+'_sum', {}, 120000)]
                    rows += [(name+'_bucket', {'le': str(bound)}, 200 if bound >= 600 else 0) for bound in (100, 500, 1000)]
                    rows.append((name+'_bucket', {'le': '+Inf'}, 200))
                else:
                    rows = [(name, {}, 10)]
                result = []
                for metric, extra, step in rows:
                    values = [[start+1+i*10, str(i*step)] for i in range(n)]
                    if 'pending_append' in metric:
                        values = [[start+1+i*10, str(12+i*2)] for i in range(n)]
                    if metric.endswith('_segment') or metric.endswith('_entry'):
                        values = [[start+1+i*10, '1'] for i in range(n)]
                    for suffix, value in [('disk_usage_ratio', .92), ('watermark_soft', .8), ('watermark_hard', .9)]:
                        if metric.endswith(suffix):
                            values = [[start+1+i*10, str(value)] for i in range(n)]
                    if 'system_' in metric:
                        values = [[start+1+i*10, '.1'] for i in range(n)]
                    result.append({'metric': dict(labels, __name__=metric, **extra), 'values': values})
                data = {'status': 'success', 'data': {'resultType': 'matrix', 'result': result}}
            body = json.dumps(data).encode()
            self.send_response(200)
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_POST(self):
            state['mutations'] += 1
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{}')

    class FixtureServer(http.server.ThreadingHTTPServer):
        request_queue_size = 128
        daemon_threads = True

    backend = FixtureServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=backend.serve_forever, daemon=True).start()
    proc = None
    try:
        with tempfile.TemporaryDirectory(prefix='wp-workspace-') as folder:
            root = Path(folder)
            endpoint = f'http://127.0.0.1:{backend.server_port}'
            config = {'current-context': 'fixture-a', 'contexts': {}, 'diagnostics': {
                'sources': {'metrics': {'connections': {'local': {'settings': {'endpoint': endpoint, 'deployment': 'fixture', 'match_labels': {'cluster': 'fixture'}}}}},
                            'trace': {'connections': {'local': {'settings': {'endpoint': endpoint, 'deployment': 'fixture', 'services': {'wp': 'wp_server'}, 'search': {'service': 'wp'}}}}}},
                'targets': {}}, 'agent': {'enabled': False}}
            config['diagnostics']['sources']['trace']['connections']['tagged'] = copy.deepcopy(config['diagnostics']['sources']['trace']['connections']['local'])
            config['diagnostics']['sources']['trace']['connections']['tagged']['settings'].update({'identity_tags': {'log_ns':'wp.log_ns','log_id':'wp.log_id','node_id':'wp.node_id','reader_name':'wp.reader_name'}, 'request_id_tag':'request.id'})
            for name in ('fixture-a', 'fixture-b'):
                config['contexts'][name] = {'endpoint': endpoint, 'node_admin_urls': {'node-1': endpoint}, 'k8s': {'kube_context': name+'-kube', 'namespace': name+'-ns', 'cluster': name+'-wp'}, 'observability': {
                    'prometheus': config['diagnostics']['sources']['metrics']['connections']['local'],
                    'trace': config['diagnostics']['sources']['trace']['connections']['local'],
                    'grafana': endpoint+'/grafana/'+name}, 'agent': {'enabled': False}}
            del config['diagnostics']
            del config['agent']
            os.environ['WOODPECKER_RUN_CACHE_DIR'] = str(root/'input-candidates')
            # Successful input fixture: one independent key, no tenant association.
            cache=root/'input-candidates';cache.mkdir()
            seed_values={'metadata.etcd':['http://previous-etcd:2379']}
            for field in ('metadata.prefix','metadata.bucket','metadata.root_path','log_name','segment_id','reader_name','path','request_id','trace_id'):
                seed_values[field]=[f'previous-{i}' for i in range(5)]
            seed_values['metadata.root_path']=['wal/'+('long-path/'*80)+str(i) for i in range(5)]
            (cache/'run-00000000000000000001.json').write_text(json.dumps({'schema_version':'wp.input-candidates/v1','recorded_at':'2026-11-01T00:00:00Z','values':seed_values}))
            path = root/'cli.yaml'
            source_comments='# Preserve source comments and ordering\n'+''.join(f'# source line {i}\n' for i in range(260))+'# LONG_SOURCE_LINE '+('x'*400)+'\n'
            path.write_text(source_comments+json.dumps(config,indent=2)+'\n# SOURCE END\n')
            before = path.read_bytes()
            # The terminal and machine output are both usable without a server.
            cli = subprocess.run([binary, 'diag', '--config', str(path), 'scenario', 'slow-writes', '--window', '5m', '--node-id', 'node-1', '--log-namespace', 'tenant', '--log-id', '1', '--details', 'all', '--output', 'json'], capture_output=True, text=True, timeout=45)
            if cli.returncode != 0:
                (output / "cli-failure.json").write_text(cli.stdout)
            assert cli.returncode == 0, cli.stderr+cli.stdout[:500]
            result = json.loads(cli.stdout)
            assert result['schema_version'] == 'wp.diag/v1'
            assert result['summary']['metrics']['abnormal'] > 0
            assert result['summary']['trace']['matched_traces'] == 1
            (output/'cli-scenario.json').write_text(cli.stdout)
            print('CLI scenario passed', flush=True)
            if args.cli_only:
                assert state['mutations'] == 0
                return
            proc = subprocess.Popen([binary, 'webui', '--config', str(path), '--listen', '127.0.0.1:0'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            lines = queue.Queue()
            threading.Thread(target=lambda: [lines.put(line) for line in proc.stdout], daemon=True).start()
            line = lines.get(timeout=20)
            match = re.search(r'http://127.0.0.1:\d+', line)
            assert match, line
            for script in ['operations_browser.mjs', 'parameter_help_browser.mjs', 'command_picker_browser.mjs', 'diagnostic_validation_browser.mjs']:
                case_output = output / Path(script).stem
                case_output.mkdir(parents=True, exist_ok=True)
                case = subprocess.run(['node', str(Path(__file__).with_name(script)), match[0], str(case_output)], capture_output=True, text=True, timeout=150)
                assert case.returncode == 0, script + '\n' + case.stderr + case.stdout
                print(script + ' passed', flush=True)
            browser = subprocess.run(['node', str(Path(__file__).with_name('workspace_inputs_browser.mjs')), match[0], str(output)], capture_output=True, text=True, timeout=150)
            assert browser.returncode == 0, browser.stderr+browser.stdout
            assert path.read_bytes() == before, 'Browser sessions must not rewrite cli.yaml'
            variants = []
            proc.terminate()
            proc.wait(timeout=10)
            # An installed local fake emits Codex's stream format. No real model
            # credentials or provider requests are used for history/follow-up tests.
            packets = root/'agent-packets.ndjson'
            fake = root/'fake-codex'
            fake.write_text('#!/usr/bin/env python3\nimport sys,json\n'
                            'if sys.argv[1:3] == ["login","status"]: sys.exit(0)\n'
                            'p=json.load(sys.stdin)\n'
                            f'with open({str(packets)!r},"a") as out: out.write(json.dumps(p)+"\\n")\n'
                            'text="## Finding\\n"+("Refined assessment" if p.get("previous_analysis") else "Initial assessment")+": filesystem usage is high; impact is unconfirmed. [S06]\\n## Evidence\\n- Usage exceeds the watermark. [S06]\\n## Next steps\\n1. Correlate write attempts with the incident interval. [W04]"\n'
                            'for e in [{"type":"turn.started"},{"type":"item.completed","item":{"type":"agent_message","text":text}},{"type":"turn.completed"}]: print(json.dumps(e),flush=True)\n')
            fake.chmod(0o700)
            agent_config = copy.deepcopy(config)
            for context in agent_config['contexts'].values():
                context['agent'] = {'enabled': True, 'provider': 'codex', 'auth': 'cli', 'executable': str(fake)}
            agent_path = root/'agent.cli.yaml'
            agent_path.write_text(json.dumps(agent_config))
            proc = subprocess.Popen([binary, 'webui', '--config', str(agent_path), '--listen', '127.0.0.1:0'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            lines = queue.Queue()
            threading.Thread(target=lambda: [lines.put(line) for line in proc.stdout], daemon=True).start()
            match = re.search(r'http://127.0.0.1:\d+', lines.get(timeout=20))
            assert match
            analysis_browser = subprocess.run(['node', str(Path(__file__).with_name('workspace_analysis_browser.mjs')), match[0], str(output)], capture_output=True, text=True, timeout=120)
            assert analysis_browser.returncode == 0, analysis_browser.stderr+analysis_browser.stdout
            submitted = [json.loads(line) for line in packets.read_text().splitlines()]
            assert len(submitted) == 2
            assert 'previous_analysis' not in submitted[0]
            assert submitted[1]['report_sha256'] == submitted[0]['report_sha256']
            assert 'Initial assessment' in submitted[1]['previous_analysis']['text']
            assert submitted[1]['question'] == 'What would confirm the suspected impact?'
            assert agent_path.read_text() == json.dumps(agent_config)
            proc.terminate()
            proc.wait(timeout=10)
            # Empty/source-only environments and metadata requirements are exercised
            # in Go workspace tests; browser cases here cover the shared form/cache.
            assert state['mutations'] == 0, 'CLI confirmation was bypassed'
            receipt = {'passed': True, 'browser': json.loads(browser.stdout), 'analysis_browser': json.loads(analysis_browser.stdout), 'fake_agent_calls': 2, 'configuration_variants': variants, 'source_requests': state, 'model_calls': 0}
            (output/'result.json').write_text(json.dumps(receipt, indent=2))
            print(json.dumps(receipt, indent=2))
    finally:
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        backend.shutdown()


if __name__ == '__main__':
    main()
