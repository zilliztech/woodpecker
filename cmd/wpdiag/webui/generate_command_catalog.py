#!/usr/bin/env python3
"""Build the WebUI command directory using local wp --help and handbook topics.

Only help is invoked. No operation, remote query or configuration change is run.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import sys
import subprocess

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='./bin/wp')
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location('handbook', ROOT/'docs/operations/generate_cli.py')
    handbook = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(handbook)
    topics = json.loads((ROOT/'docs/operations/cli-categories.json').read_text())
    examples = json.loads((ROOT/'docs/operations/cli-examples.json').read_text())
    changes = {'ctx use', 'node decommission', 'node cancel-decommission', 'k8s scale',
               'logging set-level', 'logstore fence', 'logstore fence-quorum', 'logstore force-flush',
               'logstore compact', 'log truncate', 'log skip-range add', 'log skip-range remove',
               'meta log patch', 'meta segment patch', 'marking confirm'}
    views = {'cluster info': 'cluster', 'meta log show': 'metadata',
             'meta segment list': 'metadata', 'meta quorum list': 'metadata', 'meta check': 'metadata'}
    required_flags = json.loads(subprocess.check_output(['go', 'run', './cmd/wpcli/catalog'], cwd=ROOT, text=True))
    operations = []
    for name, help_text in handbook.discover(str(Path(args.binary).resolve())):
        group = name.split()[0]
        category = next(key for key, topic in topics.items() if group in topic['groups'])
        usage = help_text.split('Usage:\n')[1].split('\n\n')[0].strip()
        description = help_text.split('\n\nUsage:')[0].strip()
        flags = []
        seen = set()
        sections = [(False, help_text.split('\nFlags:\n')[1].split('\nGlobal Flags:')[0] if '\nFlags:\n' in help_text else ''),
                    (True, help_text.split('\nGlobal Flags:\n')[1].split('\nUse ')[0] if '\nGlobal Flags:\n' in help_text else '')]
        for inherited, section in sections:
            for line in section.splitlines():
                match = re.match(r'^\s+(?:-\w,\s+)?--([\w-]+)(?: (\w+))?\s{2,}(.+)$', line)
                if match and match[1] not in ('help',) and match[1] not in seen:
                    seen.add(match[1])
                    default = re.search(r'\(default (.*?)\)', match[3])
                    flags.append({'name': match[1], 'type': match[2] or 'bool', 'description': match[3],
                                  'global': inherited, 'default': default[1].strip('"') if default else '',
                                  'required': match[1] in required_flags.get(name, [])})
        # Token matches, rather than examples, own parameter names.
        arguments = [{'name': m.group(1), 'optional': m.group(0).startswith('[')}
                     for m in re.finditer(r'[<\[]([^<>\[\]]+)[>\]]', usage) if m.group(1) != 'flags']
        connection = 'service'
        if category in ('local', 'shell'): connection = 'local'
        elif category == 'telemetry': connection = 'telemetry'
        elif category == 'etcd': connection = 'etcd'
        elif category in ('meta', 'marking') or name == 'log readers': connection = 'metadata'
        elif category == 'k8s': connection = 'kubernetes'
        elif category in ('log', 'segment') or name in ('logstore lac', 'logstore fence-quorum'): connection = 'mixed'
        if name == 'log truncate' or name.startswith('log skip-range '): connection = 'metadata'
        if name == 'diag catalog': connection = 'local'
        visual = 'diagnostics' if group == 'diag' else views.get(name, '')
        operations.append({'name': name, 'category': category, 'usage': usage, 'description': description,
                           'arguments': arguments, 'flags': flags, 'connection': connection,
                           'effect': 'Changes state / preview' if name in changes else 'Read / collect',
                           'availability': 'unavailable' if name in ('node restart', 'k8s doctor') else 'visual' if visual else 'cli',
                           'execution': 'terminal' if name in ('shell', 'webui') else 'unavailable' if name in ('node restart', 'k8s doctor') else 'process',
                           'visual': visual, 'example': examples.get(name, ['', '', '', ''])[0],
                           'interpretation': examples.get(name, ['', '', '', ''])[3],
                           'reference': 'https://zilliztech.github.io/woodpecker/docs.html#operations-cli-'+('diagnosis' if category == 'telemetry' else category)})
    result = {'schema_version': 'wp.commands/v1', 'categories': [dict(id=k, **v) for k, v in topics.items()], 'commands': operations}
    path = ROOT/'cmd/wpdiag/webui/assets/commands.json'
    path.write_text(json.dumps(result, indent=2)+'\n')
    print(f'Generated {len(operations)} commands in {len(topics)} handbook categories')


if __name__ == '__main__':
    main()
