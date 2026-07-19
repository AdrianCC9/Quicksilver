from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", override=False)

from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402
from config import settings  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply Quicksilver database migrations.")
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--revision", default="head")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    alembic_config = Config(str(PROJECT_ROOT / "alembic.ini"))
    alembic_config.set_main_option("sqlalchemy.url", args.database_url.replace("%", "%%"))
    command.upgrade(alembic_config, args.revision)
    print(f"Applied migrations through {args.revision}.")


if __name__ == "__main__":
    main()
