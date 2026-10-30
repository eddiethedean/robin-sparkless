"""Show documentation version/availability on every site page."""
import json
from pathlib import Path


def on_page_markdown(markdown, page, config, files):
    release = json.loads((Path(config.docs_dir) / "reference/release.json").read_text())
    note = (f"<p class=\"admonition-title\">Documentation: {release['version']} "
            f"({release['status']}); reviewed {release['reviewed']}. "
            "Match your installed package to the versioned support contract.</p>\n\n")
    return note + markdown
