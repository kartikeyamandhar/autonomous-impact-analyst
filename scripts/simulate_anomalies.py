"""
scripts/simulate_anomalies.py

Inject controlled anomalies into the raw Databricks tables so the Phase 4
detectors have something to find. Every mutation is reversible by re-running
the seed script (`rollback`).

Examples:
    python scripts/simulate_anomalies.py --action null_injection \
        --table raw.coingecko_coins_markets --column current_price --pct 0.5
    python scripts/simulate_anomalies.py --action row_drop \
        --table raw.coingecko_coins_markets --pct 0.3
    python scripts/simulate_anomalies.py --action rollback
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import Any

from databricks import sql
from dotenv import load_dotenv

load_dotenv()

CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")


def _fq(table: str) -> str:
    return table if table.count(".") >= 2 else f"{CATALOG}.{table}"


def _first_column(conn: Any, fq: str) -> str:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {fq} LIMIT 0")
        return cur.description[0][0]
    finally:
        cur.close()


def _sample_pred(conn: Any, fq: str, pct: float) -> str:
    """Deterministic ~pct row sample. Databricks rejects rand() in
    UPDATE/DELETE predicates, so hash a stable column instead."""
    seed = _first_column(conn, fq)
    return f"pmod(abs(hash(`{seed}`)), 100) < {int(round(pct * 100))}"


def _connect() -> Any:
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def simulate_column_drop(conn: Any, table: str, column: str) -> None:
    fq = _fq(table)
    cur = conn.cursor()
    try:
        # DROP COLUMN on Delta requires name-based column mapping.
        cur.execute(
            f"ALTER TABLE {fq} SET TBLPROPERTIES ("
            "'delta.columnMapping.mode' = 'name', "
            "'delta.minReaderVersion' = '2', "
            "'delta.minWriterVersion' = '5')"
        )
        cur.execute(f"ALTER TABLE {fq} DROP COLUMN `{column}`")
        print(f"[column_drop] dropped {fq}.{column}")
    finally:
        cur.close()


def simulate_null_injection(conn: Any, table: str, column: str, pct: float) -> None:
    fq = _fq(table)
    pred = _sample_pred(conn, fq, pct)
    cur = conn.cursor()
    try:
        cur.execute(f"UPDATE {fq} SET `{column}` = NULL WHERE {pred}")
        print(f"[null_injection] set {fq}.{column} = NULL for ~{pct:.0%} of rows")
    finally:
        cur.close()


def simulate_row_drop(conn: Any, table: str, pct: float) -> None:
    fq = _fq(table)
    pred = _sample_pred(conn, fq, pct)
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {fq} WHERE {pred}")
        print(f"[row_drop] deleted ~{pct:.0%} of rows from {fq}")
    finally:
        cur.close()


def simulate_value_breach(conn: Any, table: str, column: str) -> None:
    fq = _fq(table)
    pred = _sample_pred(conn, fq, 0.05)
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE {fq} SET `{column}` = CONCAT('-', `{column}`) WHERE {pred}"
        )
        print(f"[value_breach] negated ~5% of {fq}.{column}")
    finally:
        cur.close()


def rollback() -> None:
    print("[rollback] re-running seed script to restore raw tables...")
    seed = os.path.join(os.path.dirname(__file__), "seed_databricks.py")
    subprocess.run([sys.executable, seed], check=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Inject reversible anomalies into raw tables.")
    p.add_argument(
        "--action",
        required=True,
        choices=["column_drop", "null_injection", "row_drop", "value_breach", "rollback"],
    )
    p.add_argument("--table")
    p.add_argument("--column")
    p.add_argument("--pct", type=float, default=0.3)
    args = p.parse_args()

    if args.action == "rollback":
        rollback()
        return 0

    if not args.table:
        p.error("--table is required for this action")

    conn = _connect()
    try:
        if args.action == "column_drop":
            if not args.column:
                p.error("--column required for column_drop")
            simulate_column_drop(conn, args.table, args.column)
        elif args.action == "null_injection":
            if not args.column:
                p.error("--column required for null_injection")
            simulate_null_injection(conn, args.table, args.column, args.pct)
        elif args.action == "row_drop":
            simulate_row_drop(conn, args.table, args.pct)
        elif args.action == "value_breach":
            if not args.column:
                p.error("--column required for value_breach")
            simulate_value_breach(conn, args.table, args.column)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
