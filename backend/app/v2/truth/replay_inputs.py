from __future__ import annotations

import hashlib
import json
from typing import Any


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def source_fingerprints(source_authority_fields: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    fingerprints: list[dict[str, Any]] = []
    for item in list(source_authority_fields or []):
        field_name = str(item.get("field_name") or "").strip()
        if not field_name:
            continue
        payload = {
            "field_name": field_name,
            "source_name": item.get("source_name"),
            "source_type": item.get("source_type"),
            "authority_class": item.get("authority_class"),
            "observed_at": item.get("observed_at"),
            "resolved_value": item.get("resolved_value"),
        }
        fingerprints.append(
            {
                "field_name": field_name,
                "fingerprint": hashlib.sha1(_stable_json(payload).encode("utf-8")).hexdigest()[:16],
                "authority_class": item.get("authority_class"),
                "observed_at": item.get("observed_at"),
            }
        )
    return fingerprints


def compact_replay_inputs(
    *,
    source_authority_fields: list[dict[str, Any]] | None = None,
    reconciliation_report: list[dict[str, Any]] | None = None,
    implementation_profile: dict[str, Any] | None = None,
    recommendation_gate: dict[str, Any] | None = None,
    truth_envelopes: dict[str, dict[str, Any]] | None = None,
    primary_document_manifest: list[dict[str, Any]] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "source_authority_map": list(source_authority_fields or []),
        "source_fingerprints": source_fingerprints(source_authority_fields),
        "reconciliation_report": list(reconciliation_report or []),
        "implementation_profile": dict(implementation_profile or {}),
        "recommendation_gate": dict(recommendation_gate or {}),
        "truth_envelopes": dict(truth_envelopes or {}),
        "primary_document_manifest": list(primary_document_manifest or []),
        **dict(extra or {}),
    }
