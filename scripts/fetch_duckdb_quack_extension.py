"""Fetch the DuckDB 2.0.0 Quack extension for one supported server-wheel target."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
from pathlib import Path
import urllib.request


DUCKDB_VERSION = "2.0.0"
PLATFORMS = ("linux_amd64", "osx_arm64")
COMPRESSED_SHA256 = {
    "linux_amd64": "f9a5c0d2e0eef0f7546aba4579ab80d75d167cdf48830459faf67378ab2000fe",
    "osx_arm64": "d6779fb4adf9835e31f3b34cfdb1444cec1dd3af8f5ca99071271a91bae52cfc",
}
MAX_COMPRESSED_BYTES = 32 * 1024 * 1024
MAX_EXTENSION_BYTES = 128 * 1024 * 1024


def fetch_extension(platform: str, output: Path) -> tuple[int, str, str]:
    if platform not in PLATFORMS:
        raise ValueError(f"unsupported DuckDB Quack platform: {platform}")
    url = (
        "https://extensions.duckdb.org/v{}/{}/quack.duckdb_extension.gz".format(
            DUCKDB_VERSION, platform
        )
    )
    request = urllib.request.Request(
        url, headers={"User-Agent": "DuckDB/2.0.0"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        compressed = response.read(MAX_COMPRESSED_BYTES + 1)
    if len(compressed) > MAX_COMPRESSED_BYTES:
        raise ValueError("compressed DuckDB Quack extension exceeds 32 MiB")
    compressed_digest = hashlib.sha256(compressed).hexdigest()
    if compressed_digest != COMPRESSED_SHA256[platform]:
        raise ValueError(
            "DuckDB Quack extension archive hash does not match the pinned {} artifact".format(
                platform
            )
        )
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
        extension = stream.read(MAX_EXTENSION_BYTES + 1)
    if not extension or len(extension) > MAX_EXTENSION_BYTES:
        raise ValueError("DuckDB Quack extension is empty or exceeds 128 MiB")
    if output.name != "quack.duckdb_extension":
        raise ValueError("output file must be named quack.duckdb_extension")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(extension)
    return len(extension), compressed_digest, hashlib.sha256(extension).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", choices=PLATFORMS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    size, archive_digest, digest = fetch_extension(args.platform, args.output)
    print(
        "DuckDB Quack extension {} {} bytes={} archive_sha256={} extension_sha256={}".format(
            DUCKDB_VERSION, args.platform, size, archive_digest, digest
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
