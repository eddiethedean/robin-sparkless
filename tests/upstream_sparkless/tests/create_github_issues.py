#!/usr/bin/env python3
"""
Script to create GitHub issues for all bugs documented in BUG_LOG.md
"""

import subprocess
import re
from pathlib import Path


def parse_bug_log():
    """Parse BUG_LOG.md and extract all bug information."""
    bug_log_path = Path(__file__).parent / "BUG_LOG.md"
    content = bug_log_path.read_text()

    bugs = []
    current_bug = None

    lines = content.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]

        # Check for bug header: ### BUG-XXX:
        bug_match = re.match(r"^### (BUG-\d+): (.+)$", line)
        if bug_match:
            # Save previous bug if exists
            if current_bug:
                bugs.append(current_bug)

            # Start new bug
            bug_id, title = bug_match.groups()
            current_bug = {
                "id": bug_id,
                "title": title,
                "status": "Open",
                "severity": None,
                "discovered": None,
                "files": [],
                "description": [],
                "error": [],
                "reproduction": [],
                "impact": [],
                "workaround": None,
                "related_code": [],
                "related_issues": [],
                "affected_tests": [],
                "notes": [],
            }

        # Parse bug details
        if current_bug:
            if line.startswith("**Status**:"):
                current_bug["status"] = line.split(":", 1)[1].strip()
            elif line.startswith("**Severity**:"):
                current_bug["severity"] = line.split(":", 1)[1].strip()
            elif line.startswith("**Discovered**:"):
                current_bug["discovered"] = line.split(":", 1)[1].strip()
            elif line.startswith("**Files**:"):
                files_str = line.split(":", 1)[1].strip()
                current_bug["files"] = [
                    f.strip().strip("`") for f in files_str.split(",")
                ]
            elif line.startswith("**Description**:"):
                i += 1
                while (
                    i < len(lines)
                    and not lines[i].startswith("**")
                    and not lines[i].startswith("---")
                ):
                    if lines[i].strip():
                        current_bug["description"].append(lines[i].strip())
                    i += 1
                i -= 1  # Back up one line
            elif line.startswith("**Error**:"):
                i += 1
                in_code_block = False
                while i < len(lines) and (
                    lines[i].startswith("```")
                    or in_code_block
                    or (
                        not lines[i].startswith("**") and not lines[i].startswith("---")
                    )
                ):
                    if lines[i].startswith("```"):
                        in_code_block = not in_code_block
                        if not in_code_block:
                            break
                    elif in_code_block or lines[i].strip():
                        current_bug["error"].append(lines[i])
                    i += 1
                i -= 1
            elif line.startswith("**Reproduction**:"):
                i += 1
                in_code_block = False
                while i < len(lines) and (
                    lines[i].startswith("```")
                    or in_code_block
                    or (
                        not lines[i].startswith("**") and not lines[i].startswith("---")
                    )
                ):
                    if lines[i].startswith("```"):
                        in_code_block = not in_code_block
                        if not in_code_block:
                            break
                    elif in_code_block or lines[i].strip():
                        current_bug["reproduction"].append(lines[i])
                    i += 1
                i -= 1
            elif line.startswith("**Impact**:"):
                i += 1
                while i < len(lines) and lines[i].startswith("- "):
                    current_bug["impact"].append(lines[i].strip())
                    i += 1
                i -= 1
            elif line.startswith("**Workaround**:"):
                current_bug["workaround"] = (
                    line.split(":", 1)[1].strip() if ":" in line else ""
                )
            elif line.startswith("**Related Code**:"):
                i += 1
                while i < len(lines) and lines[i].startswith("- "):
                    current_bug["related_code"].append(lines[i].strip())
                    i += 1
                i -= 1
            elif line.startswith("**Related Issues**:"):
                i += 1
                while i < len(lines) and lines[i].startswith("- "):
                    current_bug["related_issues"].append(lines[i].strip())
                    i += 1
                i -= 1
            elif line.startswith("**Affected Tests**:"):
                i += 1
                while i < len(lines) and lines[i].startswith("- "):
                    current_bug["affected_tests"].append(lines[i].strip())
                    i += 1
                i -= 1
            elif line.startswith("**Note**:") or line.startswith("**Notes**:"):
                i += 1
                while (
                    i < len(lines)
                    and not lines[i].startswith("**")
                    and not lines[i].startswith("---")
                ):
                    if lines[i].strip():
                        current_bug["notes"].append(lines[i].strip())
                    i += 1
                i -= 1

        i += 1

    # Don't forget the last bug
    if current_bug:
        bugs.append(current_bug)

    return bugs


def create_issue_body(bug):
    """Create GitHub issue body from bug dict."""
    body_parts = []

    if bug["description"]:
        body_parts.append("## Description\n")
        body_parts.append("\n".join(bug["description"]))
        body_parts.append("")

    if bug["error"]:
        body_parts.append("## Error Message\n")
        body_parts.append("\n".join(bug["error"]))
        body_parts.append("")

    if bug["reproduction"]:
        body_parts.append("## Reproduction Steps\n")
        body_parts.append("\n".join(bug["reproduction"]))
        body_parts.append("")

    if bug["files"]:
        body_parts.append("## Affected Files\n")
        for file in bug["files"]:
            body_parts.append(f"- `{file}`")
        body_parts.append("")

    if bug["impact"]:
        body_parts.append("## Impact\n")
        for impact in bug["impact"]:
            body_parts.append(impact)
        body_parts.append("")

    if bug["related_code"]:
        body_parts.append("## Related Code\n")
        for code in bug["related_code"]:
            body_parts.append(code)
        body_parts.append("")

    if bug["affected_tests"]:
        body_parts.append("## Affected Tests\n")
        for test in bug["affected_tests"]:
            body_parts.append(test)
        body_parts.append("")

    if bug["related_issues"]:
        body_parts.append("## Related Issues\n")
        for issue in bug["related_issues"]:
            body_parts.append(issue)
        body_parts.append("")

    if bug["workaround"]:
        body_parts.append(f"## Workaround\n\n{bug['workaround']}\n")

    if bug["notes"]:
        body_parts.append("## Notes\n")
        body_parts.append("\n".join(bug["notes"]))
        body_parts.append("")

    # Add metadata
    body_parts.append("---")
    body_parts.append(f"**Discovered**: {bug['discovered']}")
    body_parts.append(f"**Status**: {bug['status']}")
    body_parts.append(f"**Severity**: {bug['severity']}")
    body_parts.append("")
    body_parts.append(
        "**Source**: This issue was automatically created from `tests/BUG_LOG.md`"
    )

    return "\n".join(body_parts)


def get_labels(bug):
    """Get GitHub labels for bug."""
    labels = ["bug"]  # Always use bug label

    # Add help wanted for high priority bugs
    severity = bug.get("severity", "").lower()
    if severity == "high":
        labels.append("help wanted")

    return labels


def create_issue(bug, dry_run=True):
    """Create a GitHub issue for the bug."""
    import tempfile
    import os

    title = f"{bug['id']}: {bug['title']}"
    body = create_issue_body(bug)
    labels = get_labels(bug)

    if dry_run:
        print(f"\n{'=' * 80}")
        print(f"Would create issue: {title}")
        print(f"Labels: {', '.join(labels)}")
        print(f"\nBody preview (first 500 chars):\n{body[:500]}...")
        return None

    # Create the issue using gh CLI with a temporary file for body
    label_args = []
    for label in labels:
        label_args.extend(["--label", label])

    with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
        f.write(body)
        body_file = f.name

    try:
        cmd = [
            "gh",
            "issue",
            "create",
            "--title",
            title,
            "--body-file",
            body_file,
        ] + label_args
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        issue_url = result.stdout.strip()
        print(f"Created: {title} - {issue_url}")
        return issue_url
    except subprocess.CalledProcessError as e:
        print(f"Error creating issue {bug['id']}: {e.stderr}")
        return None
    finally:
        # Clean up temp file
        if os.path.exists(body_file):
            os.unlink(body_file)


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description="Create GitHub issues from BUG_LOG.md")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without actually creating",
    )
    parser.add_argument(
        "--bug", help="Create issue for specific bug ID (e.g., BUG-001)"
    )
    args = parser.parse_args()

    bugs = parse_bug_log()
    print(f"Found {len(bugs)} bugs in BUG_LOG.md")

    if args.bug:
        bugs = [b for b in bugs if b["id"] == args.bug.upper()]
        if not bugs:
            print(f"Bug {args.bug} not found!")
            return

    created = []
    for bug in bugs:
        issue_url = create_issue(bug, dry_run=args.dry_run)
        if issue_url:
            created.append((bug["id"], issue_url))

    if not args.dry_run and created:
        print(f"\n\nCreated {len(created)} issues:")
        for bug_id, url in created:
            print(f"  {bug_id}: {url}")


if __name__ == "__main__":
    main()
