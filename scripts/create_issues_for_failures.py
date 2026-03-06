#!/usr/bin/env python3
"""Create one GitHub issue per file that has failing tests. Output file -> issue_number."""

import re
import subprocess
import sys
from collections import defaultdict

UNSKIP_NOTE = "\n\nThe tests referenced in this issue are currently **skipped** in the codebase (pytest mark with this issue number). When working on fixing sparkless to pass them, **unskip** the tests."


def main():
    with open("/tmp/failing_tests.txt") as f:
        lines = [line.strip() for line in f if line.strip()]

    by_file = defaultdict(list)
    for line in lines:
        filepath, _, rest = line.partition("::")
        by_file[filepath].append(rest if rest else line)

    mapping = {}
    for filepath in sorted(by_file.keys()):
        tests = by_file[filepath]
        title = f"Fix failing tests: {filepath}"
        body = "Failing tests:\n\n- " + "\n- ".join(tests) + UNSKIP_NOTE
        # gh has body length limits; truncate test list if needed
        if len(body) > 60000:
            body = (
                "Failing tests (truncated):\n\n- "
                + "\n- ".join(tests[:50])
                + f"\n\n... and {len(tests) - 50} more."
                + UNSKIP_NOTE
            )
        try:
            out = subprocess.run(
                ["gh", "issue", "create", "--title", title, "--body", body],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if out.returncode != 0:
                print(out.stderr, file=sys.stderr)
                sys.exit(1)
            # e.g. https://github.com/eddiethedean/robin-sparkless/issues/1144
            url = out.stdout.strip()
            n = re.search(r"/issues/(\d+)", url)
            if n:
                num = int(n.group(1))
                mapping[filepath] = num
                print(f"{filepath}\t{num}", flush=True)
        except Exception as e:
            print(f"Error creating issue for {filepath}: {e}", file=sys.stderr)
            raise

    with open("/tmp/file_to_issue.txt", "w") as f:
        for k, v in sorted(mapping.items()):
            f.write(f"{k}\t{v}\n")
    print(f"Wrote {len(mapping)} mappings to /tmp/file_to_issue.txt", file=sys.stderr)


if __name__ == "__main__":
    main()
