"""Embed Operations reference categories into the native Docs shell."""
from pathlib import Path
import re
import json
import html
HERE = Path(__file__).resolve().parent
DOCS = HERE.parent / 'docs.html'
START = '<!-- BEGIN OPERATIONS CONTENT -->'
END = '<!-- END OPERATIONS CONTENT -->'
CATEGORY_CONFIG = json.loads((HERE / 'cli-categories.json').read_text())
CATEGORIES = {key: item['title'] for (key, item) in CATEGORY_CONFIG.items()}
COMMAND_ROUTES = {}
SCENARIOS = {'read': 'Read Stalls', 'write': 'Write Stalls', 'corruption': 'Data Corruption Diagnosis & Recovery'}
SCENARIO_ALIASES = {'readers': 'read/readers', 'writes': 'write/submission', 'scan': 'corruption/scan', 'skip-range': 'corruption/skip-range', 'compaction': 'corruption/compaction', 'metadata': 'corruption/metadata', 'corruption-lab': 'corruption/locate'}

def scenario_link(section=''):
    if section in SCENARIOS:
        return '#operations-scenarios-' + section
    if section in SCENARIO_ALIASES:
        (topic, anchor) = SCENARIO_ALIASES[section].split('/')
        return '#operations-scenarios-' + topic + '/' + anchor
    return '#operations-scenarios' + ('/' + section if section else '')

def codeblocks(content):

    def render(match):
        body = match.group(1)
        if body.strip().startswith('<code>'):
            body = body.strip()[6:-7]
        return '<div class="code-container"><div class="code-header"><span>Example / output</span><button class="code-copy" onclick="copyCode(this)">Copy</button></div><div class="code-body"><pre><code>' + body + '</code></pre></div></div>'
    return re.sub('<pre>(.*?)</pre>', render, content, flags=re.S)

def cli_link(section=''):
    section = {'environment': 'cluster', 'runtime': 'ops', 'data': 'log', 'metadata': 'meta'}.get(section, section)
    if section in ['start', 'cli-yaml', 'external-access', 'shell-functions', 'etcd-connection', 'first-run']:
        return '#operations-cli-local/' + section
    if section in COMMAND_ROUTES:
        return '#operations-cli-' + COMMAND_ROUTES[section] + '/' + section
    if section in CATEGORIES:
        return '#operations-cli-' + section
    return '#operations-cli' + ('/' + section if section else '')

def fragment(kind):
    route = 'operations-' + kind
    prefix = 'ops-' + kind + '-'
    file = '_scenario-' + kind[len('scenarios-'):] + '.html' if kind.startswith('scenarios-') else '_scenarios.html' if kind == 'scenarios' else '_' + kind + '.html'
    content = (HERE / file).read_text().replace('{{WORKFLOW_CONNECTION}}', (HERE / '_workflow-connection.html').read_text())
    if kind.startswith('cli-'):
        category = kind[4:]
        description = html.escape('Configure wp and run your first commands from outside Kubernetes.' if category == 'local' else CATEGORY_CONFIG[category]['purpose'])
        groups = ' / '.join(('wp ' + name for name in CATEGORY_CONFIG[category]['groups']))
        content = re.sub('<h2>.*?</h2>', '', content, count=1, flags=re.S)
        content = '<p class="small"><a href="cli.html">CLI Handbook overview</a> / ' + CATEGORIES[category] + '</p><h1>' + CATEGORIES[category] + '</h1><p class="page-desc">' + description + '</p>' + content
    content = re.sub('<article class="command" id="([^"]+)"', lambda m: '<article class="command" data-command-anchor="' + m[1] + '" id="' + m[1] + '"', content)
    ids = re.findall('id="([^"]+)"', content)
    if kind.startswith('cli-'):
        entries = re.findall('<article class="command"[^>]*id="([^"]+)"[^>]*>\\s*<h2>(.*?)</h2>', content, re.S)
        navigation = [(a, re.sub('<[^>]*>', '', label)) for (a, label) in entries]
        navigation = [(a, re.sub('<[^>]*>', '', label)) for (a, label) in re.findall('<h2 id="([^"]+)">(.*?)</h2>', content, re.S)] + navigation
    elif kind == 'cli':
        navigation = [('overview', 'Overview'), ('command-lookup', 'Quick lookup')] + [('quick-' + c, t) for (c, t) in CATEGORIES.items()] + [('setup', 'Connection & naming'), ('shared', 'Shared parameters'), ('repair-format', 'Patch format'), ('metrics-limits', 'Metrics limits'), ('exit-codes', 'Exit codes')]
    else:
        navigation = [(a, re.sub('<[^>]*>', '', label)) for (a, label) in re.findall('<h[23][^>]*id="([^"]+)"[^>]*>(.*?)</h[23]>', content, re.S)]
    navigation = [(a, label) for (a, label) in navigation if a in ids]
    rail = '<aside class="ops-right-nav" aria-label="On this page"><details class="ops-rail" open><summary>On this page</summary><nav>' + ''.join(('<a href="#' + route + '/' + a + '">' + label + '</a>' for (a, label) in navigation)) + '</nav></details></aside>'
    content = re.sub('id="([^"]+)"', lambda m: 'id="' + prefix + m[1] + '"', content)
    content = content.replace('for="quick-search"', 'for="' + prefix + 'quick-search"')

    def link(m):
        url = m[1]
        if url.startswith('#'):
            url = cli_link(url[1:]) if kind.startswith('cli') else '#' + route + '/' + url[1:]
        elif url.startswith('cli.html'):
            url = cli_link(url.split('#', 1)[1] if '#' in url else '')
        elif url.startswith('scenarios.html'):
            url = scenario_link(url.split('#', 1)[1] if '#' in url else '')
        elif url.startswith('../'):
            url = url[3:]
        return 'href="' + url + '"'
    content = re.sub('href="([^"]+)"', link, content)
    content = re.sub('<table(?: class="([^"]*)")?>', lambda m: '<table class="doc-table ' + (m[1] or '') + '">', content)
    content = codeblocks(content)
    content = re.sub('(<table class="doc-table[^"]*">.*?</table>)', lambda m: '<div class="ops-table-wrap">' + m[1] + '</div>', content, flags=re.S)
    return '<div class="doc-page ops-page" id="page-' + route + '">\n' + rail + '\n' + content + '\n</div>\n'

def redirect(file, kind, legacy=False):
    if legacy:
        js = "const part=location.hash.slice(1);location.replace('docs.html#'+(part==='commands'?'operations-cli/overview':'operations-scenarios'+(part?'/'+part:'')));"
        target = 'docs.html#operations-scenarios'
    else:
        js = "const part=location.hash.slice(1);location.replace('../docs.html#operations-" + kind + "'+(part?'/'+part:''));"
        target = '../docs.html#operations-' + kind
    file.write_text('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Woodpecker Operations</title></head><body><p>This document is part of <a href="' + target + '">Woodpecker Docs · Operations</a>.</p><script>' + js + '</script></body></html>\n')

def build():
    for category in CATEGORIES:
        content = (HERE / ('_cli-' + category + '.html')).read_text()
        for name in re.findall('<article class="command" id="([^"]+)"', content):
            COMMAND_ROUTES[name] = category
    page = DOCS.read_text()
    links = ''.join(('<a class="sidebar-link" data-page="operations-cli-' + key + '" href="#operations-cli-' + key + '" onclick="showPage(\'operations-cli-' + key + '\',this)">' + html.escape(item['title']) + '</a>\n' for (key, item) in CATEGORY_CONFIG.items()))
    page = re.sub('(<div class="sidebar-subitems" aria-label="CLI Handbook categories">).*?</div>', lambda m: m[1] + '\n' + links + '</div>', page, count=1, flags=re.S)
    workflow_links = ''.join(('<a class="sidebar-link" data-page="operations-scenarios-' + key + '" href="#operations-scenarios-' + key + '" onclick="showPage(\'operations-scenarios-' + key + '\',this)">' + html.escape(title) + '</a>\n' for (key, title) in SCENARIOS.items()))
    workflow_sidebar = '<div class="sidebar-subitems" aria-label="Troubleshooting scenarios">' + workflow_links + '</div>'
    marker = '<!-- INCIDENT SCENARIO NAV -->'
    if marker in page:
        page = re.sub(marker + '.*?<!-- END INCIDENT SCENARIO NAV -->', lambda m: marker + workflow_sidebar + '<!-- END INCIDENT SCENARIO NAV -->', page, count=1, flags=re.S)
    else:
        parent = '<a class="sidebar-link" data-page="operations-scenarios" href="#operations-scenarios" onclick="showPage(\'operations-scenarios\',this)">Troubleshooting</a>'
        page = page.replace(parent, parent + '\n' + marker + workflow_sidebar + '<!-- END INCIDENT SCENARIO NAV -->', 1)
    content = START + '\n' + fragment('cli') + ''.join((fragment('cli-' + c) for c in CATEGORIES)) + fragment('scenarios') + ''.join((fragment('scenarios-' + c) for c in SCENARIOS)) + END
    if START in page:
        page = page[:page.index(START)] + content + page[page.index(END) + len(END):]
    else:
        page = page.replace('        <!-- ═══════ OVERVIEW ═══════ -->', content + '\n\n        <!-- ═══════ OVERVIEW ═══════ -->', 1)
    DOCS.write_text(page)
    redirect(HERE / 'cli.html', 'cli')
    redirect(HERE / 'scenarios.html', 'scenarios')
    redirect(HERE.parent / 'operations.html', 'scenarios', True)
if __name__ == '__main__':
    build()
