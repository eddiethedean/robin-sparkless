"""Show documentation version/availability on every site page."""

import json
from html import escape
from pathlib import Path


def on_page_markdown(markdown, page, config, files):
    release = json.loads((Path(config.docs_dir) / "reference/release.json").read_text())
    version = escape(release["version"])
    status = escape(release["status"])
    reviewed = escape(release["reviewed"])
    note = (
        '<div class="sparkless-release">'
        f'<span class="sparkless-release__version">Docs {version}</span>'
        f'<span class="sparkless-release__status">{status}</span>'
        f'<time datetime="{reviewed}">Reviewed {reviewed}</time>'
        "</div>\n\n"
    )
    return note + markdown
