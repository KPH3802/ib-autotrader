#!/usr/bin/env python3
"""
migrate_positions_db.py — Phase 1 Performance Tracking Migration

Safely adds new columns to open_positions table and creates signal_benchmarks
lookup table. Safe to run on a live DB with existing positions — all new columns
are nullable, existing data is untouched, and operations are idempotent.

Usage:
    python3 migrate_positions_db.py              # Uses config.py POSITIONS_DB path
    python3 migrate_positions_db.py --db /path   # Override DB path
"""

import sys
import sqlite3
import argparse
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# New columns to add to open_positions (all nullable)
# ---------------------------------------------------------------------------
NEW_COLUMNS = [
    ("expected_return_pct", "REAL"),
    ("expected_hold_days", "INTEGER"),
    ("score", "INTEGER"),
    ("spy_entry_price", "REAL"),
    ("vs_expected_pct", "REAL"),
    ("spy_return_pct", "REAL"),
    ("alpha_vs_spy", "REAL"),
]

# ---------------------------------------------------------------------------
# Signal benchmarks from validated backtests (Full 2025)
# ---------------------------------------------------------------------------
SIGNAL_BENCHMARKS = [
    ("8K_1.01",       "SHORT", 3.17,  5),
    ("DIV_CUT",       "BUY",   15.77, 60),
    ("PEAD_BULL",     "BUY",   4.24,  28),
    ("PEAD_BEAR",     "SHORT", 1.74,  28),
    ("SI_SQUEEZE",    "BUY",   1.70,  28),
    ("COT_BULL",      "BUY",   1.70,  56),
    ("COT_BEAR",      "SHORT", 0.75,  56),
    ("CEL_BEAR",      "SHORT", 0.21,  5),
    ("THIRTEENF_BULL","BUY",   9.97,  91),
]


def get_db_path(override=None):
    """Resolve DB path from override or config.py."""
    if override:
        return Path(override)
    try:
        sys.path.insert(0, str(SCRIPT_DIR))
        import config
        return Path(getattr(config, "POSITIONS_DB", SCRIPT_DIR / "positions.db"))
    except ImportError:
        return SCRIPT_DIR / "positions.db"


def get_existing_columns(cursor, table):
    """Return set of column names for a table."""
    cursor.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}


def migrate(db_path):
    """Run the full migration — idempotent and safe on live data."""
    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        sys.exit(1)

    print(f"Migrating: {db_path}")
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    # --- Step 1: Add new columns to open_positions ---
    existing = get_existing_columns(c, "open_positions")
    added = []
    for col_name, col_type in NEW_COLUMNS:
        if col_name not in existing:
            c.execute(f"ALTER TABLE open_positions ADD COLUMN {col_name} {col_type}")
            added.append(col_name)
            print(f"  Added column: {col_name} {col_type}")
        else:
            print(f"  Column already exists: {col_name} — skipping")

    # --- Step 2: Create signal_benchmarks table ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS signal_benchmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            direction TEXT NOT NULL,
            expected_return_pct REAL NOT NULL,
            expected_hold_days INTEGER NOT NULL,
            UNIQUE(source, direction)
        )
    """)
    print("  signal_benchmarks table ready")

    # Upsert benchmark data (INSERT OR REPLACE so re-runs update values)
    for source, direction, ret_pct, hold_days in SIGNAL_BENCHMARKS:
        c.execute("""
            INSERT OR REPLACE INTO signal_benchmarks
            (source, direction, expected_return_pct, expected_hold_days)
            VALUES (?, ?, ?, ?)
        """, (source, direction, ret_pct, hold_days))
    print(f"  Loaded {len(SIGNAL_BENCHMARKS)} signal benchmarks")

    conn.commit()

    # --- Verification ---
    print("\n--- Verification ---")
    updated_cols = get_existing_columns(c, "open_positions")
    for col_name, _ in NEW_COLUMNS:
        status = "OK" if col_name in updated_cols else "MISSING"
        print(f"  {col_name}: {status}")

    c.execute("SELECT COUNT(*) FROM open_positions WHERE status = 'OPEN'")
    open_count = c.fetchone()[0]
    print(f"  Open positions: {open_count} (untouched — new columns are NULL)")

    c.execute("SELECT COUNT(*) FROM signal_benchmarks")
    bench_count = c.fetchone()[0]
    print(f"  Signal benchmarks: {bench_count} rows")

    conn.close()
    print(f"\nMigration complete. {len(added)} column(s) added.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phase 1 positions.db migration")
    parser.add_argument("--db", help="Override path to positions.db")
    args = parser.parse_args()
    migrate(get_db_path(args.db))
