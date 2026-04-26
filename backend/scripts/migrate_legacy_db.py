#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from scripts.merge_legacy_db import merge_legacy_db


def migrate_legacy_db(source_path: Path | None = None, target_path: Path | None = None) -> dict:
    return merge_legacy_db(legacy_path=source_path, canonical_path=target_path, rename_legacy=True)


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Compatibility wrapper. Use backend/scripts/merge_legacy_db.py for full options.",
    )
    parser.add_argument("--source", type=Path, default=None, help="Legacy DB path.")
    parser.add_argument("--target", type=Path, default=None, help="Canonical DB path.")
    args = parser.parse_args()
    result = migrate_legacy_db(source_path=args.source, target_path=args.target)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
