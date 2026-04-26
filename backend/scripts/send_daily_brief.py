#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.config import Settings
from app.schedulers.daily_brief import run_daily_brief_once

CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate and send daily investment brief email.")
    parser.add_argument("--force-send", action="store_true", help="Send even if already sent today.")
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Generate brief and persist run status, but do not send SMTP email.",
    )
    parser.add_argument(
        "--force-cache-only",
        action="store_true",
        help="Use cached datasets only for this run.",
    )
    parser.add_argument(
        "--scheduled",
        action="store_true",
        help="Run only during the 08:00 China-time window (intended for launchd/cron).",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    settings = Settings.from_env()

    try:
        now_utc = datetime.now(UTC)
        if args.scheduled:
            now_china = now_utc.astimezone(CHINA_TZ)
            if now_china.hour != 8:
                print(
                    json.dumps(
                        {
                            "status": "skipped",
                            "reason": "outside_china_8am_window",
                            "attempted_at_utc": now_utc.isoformat(),
                            "attempted_at_china": now_china.isoformat(),
                        },
                        indent=2,
                    )
                )
                return 0
        payload = run_daily_brief_once(
            settings=settings,
            now_utc=now_utc,
            force_send=bool(args.force_send),
            send_email=not bool(args.no_email),
            force_cache_only=bool(args.force_cache_only),
        )
        print(json.dumps(payload, indent=2))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": str(exc),
                },
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
