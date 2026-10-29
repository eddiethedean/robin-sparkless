"""Build the Spark Connect 4.0.5 token parameter without exposing it in argv."""

from __future__ import annotations

from urllib.parse import quote


def url_with_token(url: str, token: str) -> str:
    base, separator, parameters = url.partition("/;")
    existing = parameters.split(";") if separator and parameters else []
    existing = [value for value in existing if not value.startswith("token=")]
    existing.append("token=" + quote(token, safe=""))
    return base + "/;" + ";".join(existing)
