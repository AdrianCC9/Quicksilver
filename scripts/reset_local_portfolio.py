from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import delete

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.local_mysql_storage import LocalMySQLStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reset local mock portfolio runs, positions, trades, and snapshots."
    )
    parser.add_argument("--database-url")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    storage = LocalMySQLStorage(database_url=args.database_url)
    try:
        storage.create_tables()
        with storage.engine.begin() as connection:
            for table in (
                storage.portfolio_trades,
                storage.portfolio_positions,
                storage.portfolio_snapshots,
                storage.portfolio_runs,
            ):
                result = connection.execute(delete(table))
                print(f"Deleted {result.rowcount or 0} rows from {table.name}.")
    finally:
        storage.close()


if __name__ == "__main__":
    main()
