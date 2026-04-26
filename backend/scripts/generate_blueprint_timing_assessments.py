from __future__ import annotations

import json
import sqlite3

from app.config import get_db_path
from app.v2.surfaces.blueprint.explorer_contract_builder import generate_current_timing_assessment_artifacts


def main() -> None:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        result = generate_current_timing_assessment_artifacts(conn)
    finally:
        conn.close()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
