"""
scripts/agent_metrics.py

Print a calibration report over recorded incidents so risk-weight tuning is
driven by data, not vibes: incident counts, risk-level distribution, and (if
human feedback was recorded via IncidentStore.record_outcome) the actionable
rate — i.e. how often a fired alert was actually worth acting on.

Usage:
    python scripts/agent_metrics.py
    python scripts/agent_metrics.py --incident-dir data/incidents
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.agent.incident_store import IncidentStore  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--incident-dir", default="data/incidents")
    args = p.parse_args()

    summary = IncidentStore(args.incident_dir).summary()
    print(json.dumps(summary, indent=2))

    rate = summary.get("actionable_rate")
    if rate is None:
        print(
            "\nNo human feedback recorded yet. Capture it with "
            "IncidentStore.record_outcome(key, actionable=True/False) to enable "
            "precision tracking and risk-weight calibration."
        )
    else:
        print(f"\nActionable rate: {rate:.0%} over {summary['feedback_count']} labeled alerts.")
        if rate < 0.5:
            print("  -> Likely alerting too aggressively; consider raising thresholds.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
