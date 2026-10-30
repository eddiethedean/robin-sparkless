"""Check first-party docs links/anchors, fence syntax, configuration and release metadata."""
from __future__ import annotations

import ast
import json
from pathlib import Path
import re
import subprocess
import tomllib
from urllib.parse import unquote

import markdown

ROOT = Path(__file__).resolve().parents[1]
FENCE = re.compile(r'^```(\w*)[^\n]*\n(.*?)^```\s*$', re.M | re.S)
LINK = re.compile(r'(?<!!)\[[^\]]+\]\(([^\s)]+)\)')


def main() -> None:
    tracked = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard'], cwd=ROOT, text=True).splitlines()
    files = sorted({ROOT / p for p in tracked if p.endswith(('.md', '.rst')) and not p.startswith(('upstream_', 'vendor/'))})
    errors = []
    anchors = {}
    for path in files:
        text = path.read_text()
        rendered = markdown.markdown(text, extensions=['toc', 'attr_list', 'pymdownx.superfences'])
        anchors[path.resolve()] = set(re.findall(r'\bid=["\']([^"\']+)', rendered))
    for path in files:
        text = path.read_text()
        for fence in FENCE.finditer(text):
            language, body = fence.group(1, 2)
            line = text.count('\n', 0, fence.start()) + 1
            try:
                if language == 'python':
                    ast.parse(body)
                elif language == 'toml':
                    tomllib.loads(body)
            except (SyntaxError, tomllib.TOMLDecodeError) as error:
                errors.append(f'{path.relative_to(ROOT)}:{line}: {language} syntax: {error}')
        prose = FENCE.sub('', text)
        for match in LINK.finditer(prose):
            target = unquote(match.group(1).strip('<>'))
            if re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:', target) or target.startswith('//'):
                continue
            relative, _, anchor = target.partition('#')
            dest = (path.parent / relative).resolve() if relative else path.resolve()
            if not dest.exists():
                errors.append(f'{path.relative_to(ROOT)}: missing target {target}')
            elif anchor and dest in anchors and anchor not in anchors[dest]:
                errors.append(f'{path.relative_to(ROOT)}: missing anchor {target}')
    release = json.loads((ROOT / 'docs/reference/release.json').read_text())
    project = tomllib.loads((ROOT / 'python/pyproject.toml').read_text())['project']
    if release['version'] != project['version']:
        errors.append('docs release version differs from Python package version')
    if release['status'] not in {'candidate', 'published'}:
        errors.append('docs release status must be candidate or published')
    for path in [ROOT/'README.md', ROOT/'python/README.md']:
        if len(path.read_text().split()) > 350:
            errors.append(f'{path.relative_to(ROOT)} exceeds the 350-word README budget; move detail to docs')
    if errors:
        raise SystemExit('\n'.join(errors))
    print(f'Checked {len(files)} documentation files: links, anchors, Python/TOML fences, version and README budgets')


if __name__ == '__main__':
    main()
