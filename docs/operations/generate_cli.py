#!/usr/bin/env python3
"""Regenerate the static CLI handbook from local wp help and authored examples.
Run from any directory: python3 docs/operations/generate_cli.py --binary ./bin/wp
No command is executed except --help.
"""
import argparse
import html
import json
import pathlib
import re
import subprocess
HERE = pathlib.Path(__file__).resolve().parent
CATEGORY_CONFIG = json.loads((HERE / 'cli-categories.json').read_text())
GROUPS = {key: (item['title'], item['groups']) for (key, item) in CATEGORY_CONFIG.items()}

def esc(s):
    return html.escape(s)

def block(s):
    return '<pre><code>' + esc(s) + '</code></pre>'

def anchor(name):
    return name.replace(' ', '-')

def flags(text, compact=False):
    rows = []
    for line in text.splitlines():
        if not line.lstrip().startswith('-'):
            continue
        cells = re.split('\\s{2,}', line.strip(), maxsplit=1)
        label = cells[0]
        desc = cells[1] if len(cells) > 1 else ''
        if compact and ('--etcd' in label or '--meta-prefix' in label or '--help' in label):
            continue
        rows.append('<tr><td><code>' + esc(label) + '</code></td><td>' + esc(desc) + '</td></tr>')
    return '<table><thead><tr><th>Parameter / type</th><th>Use and default</th></tr></thead><tbody>' + ''.join(rows) + '</tbody></table>' if rows else '<p>No additional operation-specific flags. Connection settings come from CLI setup.</p>'

def discover(binary, path=()):
    text = subprocess.check_output([binary, *path, '--help'], text=True)
    if 'Available Commands:\n' in text:
        children = re.findall('^  ([\\w-]+)\\s+', text.split('Available Commands:\n')[1].split('\nFlags:')[0], re.M)
        return [item for child in children if child != 'help' for item in discover(binary, (*path, child))]
    return [(' '.join(path), text)]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', default='./bin/wp')
    args = parser.parse_args()
    rows = discover(str(pathlib.Path(args.binary).resolve()))
    examples = json.loads((HERE / 'cli-examples.json').read_text())
    missing = {n for (n, _) in rows} - set(examples)
    if missing:
        raise SystemExit('Missing authored command examples: ' + str(missing))
    all_roots = [root for (_, group_roots) in GROUPS.values() for root in group_roots]
    assert len(all_roots) == len(set(all_roots)), 'A command group appears in multiple topics'
    assert set(all_roots) == {name.split()[0] for (name, _) in rows}, 'Topic configuration does not cover all command groups'
    nav = ''
    sections = ''
    quick_tables = {}
    for (category, (title, roots)) in GROUPS.items():
        selected = [(n, h) for (n, h) in rows if n.split()[0] in roots]
        nav += '<h3>' + title + '</h3>'
        sections += '<section id="topic-' + category + '"><h2>' + title + '</h2>'
        quick_rows = []
        previous_root = None
        for (name, help_text) in selected:
            ident = anchor(name)
            nav += '<a class="command-link" href="#' + ident + '">wp ' + name + '</a>'
            (example, ok, bad, meaning) = examples[name]
            purpose = help_text.split('\n\nUsage:')[0].strip()
            usage = help_text.split('Usage:\n')[1].split('\n\n')[0].strip()
            local = help_text.split('\nFlags:\n')[1].split('\nGlobal Flags:')[0] if '\nFlags:\n' in help_text else ''
            root_name = name.split()[0]
            if root_name != previous_root:
                label = 'Command group' if any((n.startswith(root_name + ' ') for (n, _) in rows)) else 'Command'
                quick_rows.append('<tr class="ops-group-heading"><th colspan="3">' + label + ': <code>wp ' + esc(root_name) + '</code></th></tr>')
                previous_root = root_name
            required = []
            positional_meanings = {'node': 'Discovered node ID', 'node-or-pod': 'Actual pod name, or a node ordinal resolved with --wp-cluster', 'logName': 'Logical log name in metadata', 'segmentId': 'Numeric segment ID within that log', 'logId': 'Numeric log identity', 'key': 'Exact raw etcd key', 'name': 'Local CLI context name', 'metric-name': 'Prometheus metric family to watch'}
            for arg in re.findall('<([^>]+)>', re.sub('\\[[^\\]]*\\]', '', usage).split('|', 1)[0]):
                required.append('<code>&lt;' + esc(arg) + '&gt;</code>: ' + esc(positional_meanings.get(arg, 'Required positional argument')))
            for line in local.splitlines():
                if not re.search('\\([^)]*\\brequired(?:[;)])', line):
                    continue
                cells = re.split('\\s{2,}', line.strip(), maxsplit=1)
                if len(cells) == 2:
                    required.append('<code>' + esc(cells[0]) + '</code>: ' + esc(cells[1].replace('(required)', '').strip()))
            if name in ['log skip-range add', 'log skip-range remove']:
                required.append('<code>--from-entry / --to-entry</code>: inclusive entry range')
            if name == 'log skip-range add':
                required.append('<code>--reason</code>: accepted-loss reason; <code>-y</code> commits the reviewed declaration')
            if name == 'metrics report':
                required.append('<code>--scenario</code>: analysis scenario, or <code>--list</code> to enumerate scenarios')
            if name in ['segment inspect', 'segment probe', 'log scan']:
                required.append('For explicit storage: paired <code>--bucket / --root-path</code>')
            quick_rows.append('<tr class="ops-quick-row"><td><a href="#' + ident + '"><code>wp ' + esc(name) + '</code></a></td><td>' + esc(purpose.splitlines()[0]) + '</td><td>' + ('<br>'.join(required) if required else 'No required command-specific arguments; uses selected context/defaults.') + '</td></tr>')
            help_without_global = help_text.split('\nGlobal Flags:')[0].strip()
            preparation = ''
            connections = '<p class="small">Etcd connection flags: see <a href="cli.html#etcd-connection">CLI setup</a>.</p>' if '--etcd' in local else ''
            sections += '<article class="command" id="' + ident + '"><h2>wp ' + name + '</h2><p>' + esc(purpose.splitlines()[0]) + '</p>' + block(usage) + '<h3>Parameters to know</h3>' + flags(local, compact=True) + connections + '<h3>Demo</h3>' + preparation + block(example) + '<h3>Output and interpretation</h3>' + block(ok) + '<p>' + esc(meaning) + '</p><details class="ops-help"><summary>Finding or error example</summary>' + block(bad) + '</details><details class="ops-help"><summary>Complete command help</summary>' + block(help_without_global) + '</details></article>'
        sections += '</section>'
        quick_tables[category] = '<section class="ops-quick-section" id="quick-' + category + '"><h2>' + title + ' · quick lookup</h2><table class="ops-quick-table"><thead><tr><th>Operation</th><th>Purpose</th><th>Required / important parameters</th></tr></thead><tbody>' + ''.join(quick_rows) + '</tbody></table></section>'
    root = subprocess.check_output([str(pathlib.Path(args.binary).resolve()), '--help'], text=True)
    shared = root.split('\nFlags:\n')[1].split('\nUse ')[0]
    introduction = (HERE / '_overview.html').read_text()
    terminology = f'{len(all_roots)} top-level commands/groups · {len(rows)} executable operations'
    topics = '<nav class="ops-topic-grid" aria-label="CLI topics">' + ''.join(('<a href="#' + key + '"><strong>' + esc(item['title']) + '</strong><span>' + esc(' / '.join(item['groups'])) + '</span></a>' for (key, item) in CATEGORY_CONFIG.items())) + '</nav>'
    introduction = introduction.replace('{{SUMMARY}}', terminology).replace('{{TOPICS}}', topics).replace('{{SHARED_FLAGS}}', flags(shared))
    lookup = '<section id="command-lookup"><h2>All operations at a glance</h2><p>Search by command, purpose or parameter, then click an operation to open its topic at the matching detail. Connection parameters are explained in CLI setup.</p><label for="quick-search">Search operations</label><input id="quick-search" type="search" placeholder="inspect, --op-id, profile…"></section>' + ''.join(quick_tables.values())
    for stale in HERE.glob('_cli-*.html'):
        if stale.stem[len('_cli-'):] not in GROUPS:
            stale.unlink()
    for category in GROUPS:
        match = re.search('<section id="topic-' + category + '">.*?</section>', sections, re.S)
        category_content = match.group(0)
        if category in ['local', 'etcd']:
            intro_file = '_setup.html' if category == 'local' else '_etcd.html'
            category_content = category_content.replace('</h2>', '</h2>' + (HERE / intro_file).read_text(), 1)
        (HERE / ('_cli-' + category + '.html')).write_text(re.sub('>\\s*<', '>\n<', category_content) + '\n')
    overview_end = introduction.index('</section>') + len('</section>')
    (HERE / '_cli.html').write_text(re.sub('>\\s*<', '>\n<', introduction[:overview_end] + lookup + introduction[overview_end:]).rstrip() + '\n')
    import sys
    sys.dont_write_bytecode = True
    import embed_operations
    embed_operations.build()
    print('Generated', len(rows), 'executable operation entries in', len(GROUPS), 'topics')
if __name__ == '__main__':
    main()
