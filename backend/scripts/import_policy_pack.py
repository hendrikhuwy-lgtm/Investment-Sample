from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.config import Settings, get_db_path
from app.models.db import connect, init_db
from app.services.policy_assumptions import import_policy_pack


BACKEND_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = BACKEND_ROOT / "app" / "storage" / "schema.sql"


def main() -> None:
    parser = argparse.ArgumentParser(description="Import a Daily Brief policy pack JSON file into the governance registry.")
    parser.add_argument("path", help="Path to a JSON file containing assumptions, benchmark_profiles, stress_methodologies, and/or regime_methodology.")
    args = parser.parse_args()

    payload = json.loads(Path(args.path).read_text(encoding="utf-8"))
    settings = Settings.from_env()
    conn = connect(get_db_path(settings=settings))
    try:
        init_db(conn, SCHEMA_PATH)
        result = import_policy_pack(conn, payload)
        print(json.dumps(result, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
