from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from parity_hunt.diff import Comparison


REPO_ROOT = Path(__file__).resolve().parents[2]
STATE_PATH = REPO_ROOT / "tmp" / "parity_hunt_dedupe.json"


def _gh_search_exists(query: str) -> bool:
    # Use `gh issue list --search`. Return True if anything matches.
    #
    # NOTE: Keep this conservative. We only want to de-dupe when we're very
    # confident it's the *same* issue, not merely related.
    try:
        out = subprocess.run(
            [
                "gh",
                "issue",
                "list",
                "--state",
                "all",
                "--search",
                query,
                "--limit",
                "1",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=15,
        )
        return out.returncode == 0 and out.stdout.strip() != ""
    except Exception:
        return False


@dataclass
class DedupeIndex:
    created_by_signature: Dict[str, str]

    @classmethod
    def load(cls) -> "DedupeIndex":
        created: Dict[str, str] = {}
        if STATE_PATH.exists():
            try:
                created = json.loads(STATE_PATH.read_text()).get(
                    "created_by_signature", {}
                )
            except Exception:
                created = {}
        return cls(created_by_signature=created)

    def save(self) -> None:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(
            json.dumps(
                {"created_by_signature": self.created_by_signature},
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )

    def signature(self, c: Comparison) -> str:
        # Stable signature for mismatch: scenario + first 120 chars of summary + top-level keys.
        keys = ",".join(sorted(c.details.keys()))
        return f"{c.scenario_id}|{c.summary[:120]}|{keys}"

    def is_known_gap(self, c: Comparison) -> bool:
        sig = self.signature(c)
        if sig in self.created_by_signature:
            return True

        # GH search: only treat as duplicate if there's already a parity-gap issue
        # for this exact scenario id.
        #
        # We intentionally do NOT attempt "fuzzy" matching or keyword heuristics
        # because they hide new parity gaps.
        if _gh_search_exists(f"PySpark parity gap: {c.scenario_id}"):
            return True
        if _gh_search_exists(f"Scenario: `{c.scenario_id}`"):
            return True
        return False

    def record_created(self, c: Comparison, issue_url: str) -> None:
        self.created_by_signature[self.signature(c)] = issue_url
