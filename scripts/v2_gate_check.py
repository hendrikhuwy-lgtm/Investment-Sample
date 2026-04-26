#!/usr/bin/env python3
"""
V2 Gate Check — runs after /codex:result to validate milestone gate conditions.
Exits 0 on GATE PASS, exits 1 on GATE FAIL.

Usage: python3 scripts/v2_gate_check.py [milestone_number]
If no milestone given, checks the most recently modified gate file.
"""

import json
import os
import sys
import glob
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
GATES_DIR = REPO_ROOT / "backend" / "app" / ".v2-coordination" / "gates"
V2_DIR = REPO_ROOT / "backend" / "app" / "v2"
SHARED_DIR = REPO_ROOT / "shared"

MILESTONE_REQUIRED_FIELDS = {
    1: {
        "milestone_1_track_a.json": ["freeze_done", "donors_created", "translators_created", "mcp_table_done", "policy_docs_done"],
        "milestone_1_track_b.json": ["domain_objects_defined", "doctrine_stubs_created", "v2_contracts_created", "v2_ids_created", "spec_docs_done"],
        "milestone_1_track_c.json": ["blueprint_audited", "candidate_report_audited", "daily_brief_audited", "portfolio_audited"],
    },
    2: {
        "milestone_2_track_a.json": ["tier1a_adapters_done", "translators_implemented"],
        "milestone_2_track_b.json": ["interpretation_engine_done", "doctrine_evaluator_done", "holdings_overlay_done", "fixtures_produced"],
        "milestone_2_track_c.json": ["contract_validation_done"],
    },
    3: {
        "milestone_3_track_a.json": ["tier1a_live", "source_registry_done"],
        "milestone_3_track_b.json": ["explorer_route_live", "report_route_live", "contract_tests_pass"],
        "milestone_3_track_c.json": ["blueprint_rebound", "candidate_report_rebound", "smoke_test_pass"],
    },
    4: {
        "milestone_4_track_a.json": ["tier1b_news_adapter_done", "tier1b_macro_adapter_done", "tier1b_translators_done"],
        "milestone_4_track_b.json": ["daily_brief_route_live", "contract_tests_pass", "fixture_produced"],
        "milestone_4_track_c.json": ["daily_brief_rebound", "smoke_test_pass"],
    },
    5: {
        "milestone_5_track_b.json": ["portfolio_route_live", "contract_tests_pass"],
        "milestone_5_track_c.json": ["portfolio_rebound", "smoke_test_pass"],
    },
    6: {
        "milestone_6_track_b.json": ["compare_route_live", "changes_route_live", "change_ledger_live"],
        "milestone_6_track_c.json": ["compare_rebound", "changes_rebound", "smoke_test_pass"],
    },
    7: {
        "milestone_7_track_b.json": ["notebook_route_live", "evidence_workspace_live"],
        "milestone_7_track_c.json": ["final_cutover_done", "legacy_demotion_done"],
    },
}

SURFACE_NATIVE_MILESTONES = [3, 4, 5, 6, 7]

def check_no_assembler_in_v2():
    """Check that blueprint_payload_assembler is not imported anywhere in backend/app/v2/"""
    if not V2_DIR.exists():
        return True, None  # V2 dir doesn't exist yet, nothing to check
    result = subprocess.run(
        ["grep", "-rE", r"^(from|import)\s+.*blueprint_payload_assembler", str(V2_DIR)],
        capture_output=True, text=True
    )
    if result.stdout.strip():
        return False, f"VIOLATION: blueprint_payload_assembler imported in v2/:\n{result.stdout}"
    return True, None

def check_no_old_contract_import():
    """Check that v2_surface_contracts.ts does not import from canonical_frontend_contract.ts"""
    v2_contracts = SHARED_DIR / "v2_surface_contracts.ts"
    if not v2_contracts.exists():
        return True, None  # File doesn't exist yet
    content = v2_contracts.read_text()
    if "canonical_frontend_contract" in content:
        return False, "VIOLATION: v2_surface_contracts.ts imports from canonical_frontend_contract.ts"
    return True, None

def check_gate_files(milestone: int):
    """Check all required gate JSON files for a milestone."""
    required = MILESTONE_REQUIRED_FIELDS.get(milestone, {})
    if not required:
        return True, [], [f"No gate definition found for milestone {milestone} — manual review required"]

    failures = []
    warnings = []

    for filename, fields in required.items():
        gate_file = GATES_DIR / filename
        if not gate_file.exists():
            failures.append(f"MISSING: {filename}")
            continue

        try:
            data = json.loads(gate_file.read_text())
        except json.JSONDecodeError as e:
            failures.append(f"INVALID JSON in {filename}: {e}")
            continue

        for field in fields:
            if field not in data:
                failures.append(f"{filename}: missing field '{field}'")
            elif data[field] is not True:
                val = data[field]
                failures.append(f"{filename}: '{field}' = {val!r} (expected true)")

        if "notes" in data and data["notes"]:
            warnings.append(f"{filename} notes: {data['notes']}")

    return len(failures) == 0, failures, warnings


def main():
    milestone = None
    if len(sys.argv) > 1:
        try:
            milestone = int(sys.argv[1])
        except ValueError:
            print(f"Usage: {sys.argv[0]} [milestone_number]")
            sys.exit(1)
    else:
        # Find most recently modified gate file
        gate_files = sorted(GATES_DIR.glob("milestone_*.json"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not gate_files:
            print("No gate files found. Run Codex agents first.")
            sys.exit(1)
        # Extract milestone number from filename
        import re
        match = re.search(r"milestone_(\d+)_", gate_files[0].name)
        if match:
            milestone = int(match.group(1))
        else:
            print(f"Could not determine milestone from: {gate_files[0].name}")
            sys.exit(1)

    print(f"\n{'='*60}")
    print(f"V2 GATE CHECK — MILESTONE {milestone}")
    print(f"{'='*60}\n")

    all_pass = True
    all_failures = []
    all_warnings = []

    # Check gate JSON files
    gate_pass, failures, warnings = check_gate_files(milestone)
    all_failures.extend(failures)
    all_warnings.extend(warnings)
    if not gate_pass:
        all_pass = False

    # Check anti-corruption: no assembler in V2
    assembler_pass, assembler_msg = check_no_assembler_in_v2()
    if not assembler_pass:
        all_pass = False
        all_failures.append(assembler_msg)

    # Check contract ownership: no old contract import
    contract_pass, contract_msg = check_no_old_contract_import()
    if not contract_pass:
        all_pass = False
        all_failures.append(contract_msg)

    # Surface-native meaning gate reminder for M3+
    if milestone in SURFACE_NATIVE_MILESTONES:
        all_warnings.append(
            "SURFACE-NATIVE GATE (manual): Confirm this surface renders with zero legacy route fallback, "
            "zero payload translation from old assemblers, and zero page-side semantic reconstruction."
        )

    if all_warnings:
        print("WARNINGS:")
        for w in all_warnings:
            print(f"  ⚠  {w}")
        print()

    if all_failures:
        print("FAILURES:")
        for f in all_failures:
            print(f"  ✗  {f}")
        print(f"\n{'='*60}")
        print(f"GATE FAIL — Milestone {milestone} is NOT ready to merge")
        print(f"{'='*60}\n")
        sys.exit(1)
    else:
        print(f"{'='*60}")
        print(f"GATE PASS — Milestone {milestone} is ready to merge")
        print(f"{'='*60}\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
