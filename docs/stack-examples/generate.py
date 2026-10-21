#!/usr/bin/env python3
"""Embed the stack examples in the existing Docs shell without changing other pages."""
from pathlib import Path
import html
import re

HERE = Path(__file__).resolve().parent
DOCS = HERE.parent / "docs.html"
STACK_REPO = "https://github.com/zilliztech/woodpecker/tree/master/deployments/milvus-full-stack-helm"
PAGES = (
    ("stack-examples", "Overview & goals", "_overview.html"),
    ("stack-examples-helm", "Helm walkthrough", "_helm.html"),
    ("stack-examples-observability", "Monitoring & alerts", "_observability.html"),
    ("stack-examples-wpcli", "wpcli operations", "_wpcli.html"),
    ("stack-examples-e2e", "E2E testing & optimization", "_e2e.html"),
)


def region(page, name, content, anchor):
    start, end = f"<!-- BEGIN STACK EXAMPLES {name} -->", f"<!-- END STACK EXAMPLES {name} -->"
    rendered = start + "\n" + content + "\n" + end
    if start in page:
        if page.count(start) != 1 or page.count(end) != 1:
            raise ValueError("Stack examples markers must occur exactly once: " + name)
        before, rest = page.split(start, 1)
        _, after = rest.split(end, 1)
        return before + rendered + after
    if page.count(anchor) != 1:
        raise ValueError("Cannot find unique Docs insertion point: " + name)
    return page.replace(anchor, rendered + "\n" + anchor, 1)


def fragment(index):
    route, _, filename = PAGES[index]
    content = (HERE / filename).read_text().replace("{{STACK_REPO}}", STACK_REPO)
    headings = re.findall(r'<h2 id="([^"]+)">(.*?)</h2>', content)
    navigation = '<nav class="stack-onpage" aria-label="On this page">' + "".join(
        f'<a href="#{route}/{anchor}">{title}</a>' for anchor, title in headings) + "</nav>"
    content = re.sub(r'id="([^"]+)"', lambda match: f'id="{route}-{match[1]}"', content)
    content = re.sub(r"<pre><code>(.*?)</code></pre>", lambda match:
                     '<div class="code-container"><div class="code-header"><span>Commands</span>'
                     '<button class="code-copy" onclick="copyCode(this)">Copy</button></div>'
                     '<div class="code-body"><pre><code>' + match[1] + '</code></pre></div></div>', content, flags=re.S)
    content = re.sub(r"<table>(.*?)</table>", lambda match:
                     '<div class="stack-table"><table class="doc-table">' + match[1] + '</table></div>', content, flags=re.S)
    # Keep the page title/description first, followed by section navigation.
    content = content.replace('</p>', '</p>\n' + navigation, 1)
    adjacent = []
    for offset, label in ((-1, "Previous"), (1, "Next")):
        other = index + offset
        if 0 <= other < len(PAGES):
            target, title, _ = PAGES[other]
            adjacent.append(f'<a href="#{target}"><div class="pg-label">{label}</div>'
                            f'<div class="pg-title">{html.escape(title)}</div></a>')
        else:
            adjacent.append("<div></div>")
    content += '\n<div class="doc-pagination">' + "".join(adjacent) + '</div>'
    return f'<div class="doc-page stack-examples-page" id="page-{route}">\n{content}\n</div>'


def build():
    page = DOCS.read_text()
    links = "\n".join(f'<a class="sidebar-link" data-page="{route}" href="#{route}" '
                      f'onclick="showPage(\'{route}\',this)">{html.escape(title)}</a>'
                      for route, title, _ in PAGES)
    sidebar = ('<div class="sidebar-section"><div class="sidebar-heading">Milvus–Woodpecker Stack Examples</div>\n'
               + links + '\n</div>\n<div class="sidebar-divider"></div>')
    page = region(page, "NAV", sidebar,
                  '    <div class="sidebar-section">\n        <div class="sidebar-heading">Operations</div>')
    page = region(page, "CONTENT", "\n".join(fragment(i) for i in range(len(PAGES))),
                  '        <!-- ═══════ OVERVIEW ═══════ -->')
    DOCS.write_text(page)
    print("Embedded five Milvus–Woodpecker Stack Examples pages in " + str(DOCS))


if __name__ == "__main__":
    build()
