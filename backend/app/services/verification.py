from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def _normalize_ter(ter_value: float | None) -> float | None:
    """
    Normalize TER to fraction form (0.0022 for 0.22%).

    Handles both fraction (0.0022) and percent (0.22) forms.
    If value > 0.1, assumes percent form and converts to fraction.
    """
    if ter_value is None:
        return None
    ter_float = float(ter_value)
    # If TER > 10%, likely in percent form (e.g., 0.22 for 22 bps)
    # Most ETFs have TER < 2%, so 10% is safe threshold
    if ter_float > 0.10:
        return ter_float / 100.0
    return ter_float


def _parse_iso_date(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if len(raw) == 10:
            return datetime.fromisoformat(f"{raw}T00:00:00+00:00")
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:  # noqa: BLE001
        return None


def _extract_factsheet_asof(citations: list[dict[str, Any]]) -> str | None:
    dates: list[datetime] = []
    for citation in citations:
        factsheet_asof = _parse_iso_date(citation.get("factsheet_asof"))
        if factsheet_asof:
            dates.append(factsheet_asof)
            continue
        published = _parse_iso_date(citation.get("published_at"))
        if published:
            dates.append(published)
    if not dates:
        return None
    return max(dates).date().isoformat()


def verify_candidate_proofs(
    candidate: dict[str, Any],
    citations: list[dict[str, Any]],
    now: datetime,
    freshness_days_threshold: int,
) -> dict[str, Any]:
    instrument_type = str(candidate.get("instrument_type") or "etf_ucits")
    now_utc = now if now.tzinfo else now.replace(tzinfo=UTC)

    factsheet_asof = _extract_factsheet_asof(citations)
    factsheet_dt = _parse_iso_date(factsheet_asof)

    proof_isin = False
    proof_domicile = False
    proof_share_class_match = False
    proof_ter = False
    proof_factsheet_fresh = False

    primary_sources = [
        item for item in citations
        if str(item.get("url") or "").strip() and str(item.get("tier") or "primary") == "primary"
    ]

    if instrument_type in {"etf_ucits", "etf_us"}:
        # Support both ISIN (UCITS) and CUSIP (US ETFs)
        candidate_isin = str(candidate.get("isin") or "").strip().upper()
        candidate_cusip = str(candidate.get("cusip") or "").strip().upper()
        candidate_canonical = str(candidate.get("canonical_identifier") or "").strip().upper()
        candidate_share_class_id = str(candidate.get("share_class_id") or "").strip().upper()

        # Build list of acceptable identifiers (prioritize explicit ISIN/CUSIP)
        expected_identifiers = []
        if candidate_isin:
            expected_identifiers.append(candidate_isin)
        if candidate_cusip:
            expected_identifiers.append(candidate_cusip)
        if candidate_canonical:
            expected_identifiers.append(candidate_canonical)
        if candidate_share_class_id and not expected_identifiers:
            expected_identifiers.append(candidate_share_class_id)

        expected_domicile = str(candidate.get("domicile") or "").strip().upper()
        expected_share_class = str(candidate.get("accumulation_or_distribution") or "").strip().lower()

        for citation in primary_sources:
            citation_identifier = str(citation.get("proof_identifier") or "").strip().upper()
            citation_domicile = str(citation.get("proof_domicile") or "").strip().upper()
            citation_share_class = str(citation.get("proof_share_class") or "").strip().lower()
            citation_ter = citation.get("proof_ter")

            if expected_identifiers and citation_identifier and citation_identifier in expected_identifiers:
                proof_isin = True
            if expected_domicile and citation_domicile and citation_domicile == expected_domicile:
                proof_domicile = True
            if expected_share_class and citation_share_class and expected_share_class == citation_share_class:
                proof_share_class_match = True
            if citation_ter is not None:
                try:
                    candidate_ter = candidate.get("expense_ratio")
                    if candidate_ter is None:
                        continue

                    # Normalize both to fraction form
                    normalized_citation_ter = _normalize_ter(float(citation_ter))
                    normalized_candidate_ter = _normalize_ter(float(candidate_ter))

                    # Use 1 basis point tolerance (0.0001)
                    if abs(normalized_citation_ter - normalized_candidate_ter) < 0.0001:
                        proof_ter = True
                except (TypeError, ValueError):  # noqa: BLE001
                    pass

        if factsheet_dt is not None:
            age_days = (now_utc - factsheet_dt).days
            proof_factsheet_fresh = age_days <= int(freshness_days_threshold)

        required = {
            "isin_proven": proof_isin,
            "domicile_proven": proof_domicile,
            "share_class_proven": proof_share_class_match,
            "ter_proven": proof_ter,
            "factsheet_fresh": proof_factsheet_fresh,
        }
        missing = [key for key, value in required.items() if not value]
        verification_status = "verified" if not missing else "partially_verified"
    else:
        missing = ["proof_scope_etf_only"]
        if instrument_type == "cash_account_sg":
            missing.append("yield_not_sourced")
        verification_status = "partially_verified"

    return {
        "proof_isin": proof_isin,
        "proof_domicile": proof_domicile,
        "proof_share_class_match": proof_share_class_match,
        "proof_ter": proof_ter,
        "proof_factsheet_fresh": proof_factsheet_fresh,
        "factsheet_asof": factsheet_asof,
        "verification_missing": missing,
        "verification_status": verification_status,
        "last_verified_at": now_utc.isoformat(),
        "primary_sources": list(primary_sources),
    }
