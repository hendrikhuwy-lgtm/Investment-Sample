from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.config import Settings, get_db_path
from app.models.db import connect, init_db
from app.services.audit_log import log_audit_event
from app.services.brief_benchmark import DEFAULT_COMPONENTS, DEFAULT_CONTEXT_KEY
from app.services.cma_engine import DEFAULT_ASSUMPTION_DATE, DEFAULT_CMA_ROWS, DEFAULT_CMA_VERSION
from app.services.policy_assumptions import import_policy_pack
from app.services.real_email_brief import generate_mcp_omni_email_brief
from app.services.signals import SIGNAL_METHODOLOGY_VERSION, SIGNAL_THRESHOLD_REGISTRY
from app.services.stress_engine import SCENARIO_SLEEVE_SHOCKS


BACKEND_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = BACKEND_ROOT / "app" / "storage" / "schema.sql"

SOURCE_NAME = "Approved Internal Policy Committee Reference Pack"
SOURCE_BASE = "https://policy.example/internal/approved"
METHODOLOGY_NOTE = (
    "Approved internal policy committee reference pack used for governed Daily Brief policy support. "
    "This is an approved internal methodology source and replaces developer-seed placeholders."
)


def build_payload() -> dict:
    assumptions = []
    for row in DEFAULT_CMA_ROWS:
        assumptions.append(
            {
                "assumption_key": f"cma::{row['sleeve_key']}",
                "assumption_family": "cma",
                "value_json": {
                    "sleeve_key": row["sleeve_key"],
                    "sleeve_name": row["sleeve_name"],
                    "expected_return_min": row["expected_return_min"],
                    "expected_return_max": row["expected_return_max"],
                    "worst_year_loss_min": row["worst_year_loss_min"],
                    "worst_year_loss_max": row["worst_year_loss_max"],
                    "scenario_notes": row["scenario_notes"],
                    "version": DEFAULT_CMA_VERSION,
                },
                "source_name": SOURCE_NAME,
                "source_url": f"{SOURCE_BASE}/cma/{row['sleeve_key']}",
                "observed_at": DEFAULT_ASSUMPTION_DATE,
                "methodology_note": METHODOLOGY_NOTE,
                "provenance_level": "internal_policy",
                "confidence_label": row["confidence_label"],
                "overwrite_priority": 100,
                "is_current": True,
            }
        )

    benchmark_profiles = []
    for component in DEFAULT_COMPONENTS:
        weight = float(component["weight"])
        benchmark_profiles.append(
            {
                "profile_key": DEFAULT_CONTEXT_KEY,
                "sleeve_key": component["component_key"],
                "target_weight": weight,
                "min_weight": max(0.0, weight - 0.05),
                "max_weight": min(1.0, weight + 0.05),
                "source_name": SOURCE_NAME,
                "source_url": f"{SOURCE_BASE}/benchmark/{DEFAULT_CONTEXT_KEY}",
                "methodology_note": METHODOLOGY_NOTE,
                "observed_at": DEFAULT_ASSUMPTION_DATE,
                "provenance_level": "internal_policy",
                "confidence_label": "high" if component["component_key"] in {"cash", "ig_bond"} else "medium",
                "is_current": True,
            }
        )

    stress_methodologies = []
    for scenario_key, shocks in SCENARIO_SLEEVE_SHOCKS.items():
        stress_methodologies.append(
            {
                "scenario_key": scenario_key,
                "shock_definition_json": shocks,
                "methodology_source_name": SOURCE_NAME,
                "methodology_source_url": f"{SOURCE_BASE}/stress/{scenario_key}",
                "observed_at": DEFAULT_ASSUMPTION_DATE,
                "provenance_level": "internal_policy",
                "confidence_label": "medium",
                "notes": METHODOLOGY_NOTE,
            }
        )

    regime_methodology = []
    for metric_key, config in SIGNAL_THRESHOLD_REGISTRY.items():
        regime_methodology.append(
            {
                "metric_key": metric_key,
                "watch_threshold": config.get("watch_threshold"),
                "alert_threshold": config.get("alert_threshold"),
                "methodology_note": str(config.get("methodology_note") or METHODOLOGY_NOTE),
                "threshold_kind": "observational",
                "source_name": SOURCE_NAME,
                "source_url": f"{SOURCE_BASE}/regime/{metric_key}",
                "observed_at": DEFAULT_ASSUMPTION_DATE,
                "provenance_level": "internal_policy",
                "confidence_label": "medium",
                "methodology_version": SIGNAL_METHODOLOGY_VERSION,
            }
        )

    return {
        "assumptions": assumptions,
        "benchmark_profiles": benchmark_profiles,
        "stress_methodologies": stress_methodologies,
        "regime_methodology": regime_methodology,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Load approved internal policy references for Daily Brief governance.")
    parser.add_argument("--generate-brief", action="store_true", help="Generate a fresh Daily Brief after loading the approved policy pack.")
    args = parser.parse_args()

    settings = Settings.from_env()
    conn = connect(get_db_path(settings=settings))
    try:
        init_db(conn, SCHEMA_PATH)
        payload = build_payload()
        result = import_policy_pack(conn, payload)
        log_audit_event(
            conn,
            actor="system_policy_loader",
            action_type="policy_pack_load_approved_internal",
            object_type="daily_brief_policy_pack",
            object_id="approved_internal_policy_pack",
            after={"counts": result.get("counts"), "source_name": SOURCE_NAME},
        )
        print(json.dumps({"status": "ok", "result": result}, indent=2))
    finally:
        conn.close()

    if args.generate_brief:
        result = generate_mcp_omni_email_brief(force_cache_only=True)
        print(json.dumps({"generated_brief_run_id": result.get("brief_run_id"), "policy_guidance_ready": result.get("policy_guidance_ready")}, indent=2))


if __name__ == "__main__":
    main()
