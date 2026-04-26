from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.config import Settings


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _citation_key(source_id: str, url: str) -> str:
    return hashlib.sha1(f"{source_id}|{url}".encode("utf-8")).hexdigest()


def ensure_citation_health_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS citation_health_checks (
          citation_key TEXT PRIMARY KEY,
          source_id TEXT NOT NULL,
          url TEXT NOT NULL,
          publisher TEXT,
          source_tier TEXT,
          authoritative_flag INTEGER NOT NULL DEFAULT 0,
          document_type TEXT,
          retention_required INTEGER NOT NULL DEFAULT 0,
          retention_status TEXT NOT NULL DEFAULT 'not_retained',
          health_status TEXT NOT NULL DEFAULT 'unknown',
          http_status INTEGER,
          document_hash TEXT,
          last_checked_at TEXT,
          retrieved_at TEXT,
          notes TEXT
        )
        """
    )
    conn.commit()


def _hash_document(url: str, *, timeout: int, max_bytes: int) -> tuple[str | None, str, str | None]:
    hasher = hashlib.sha256()
    req = Request(url, method="GET", headers={"User-Agent": "investment-agent/0.1"})
    try:
        with urlopen(req, timeout=timeout) as response:  # noqa: S310
            total = 0
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    return None, "hash_skipped_size_limit", f"Document exceeded hash limit of {max_bytes} bytes"
                hasher.update(chunk)
    except Exception as exc:  # noqa: BLE001
        return None, "hash_failed", str(exc)
    return hasher.hexdigest(), "content_hashed", None


def _check_url(url: str, *, timeout: int = 5, hash_content: bool = False, hash_max_bytes: int = 5_000_000) -> tuple[str, int | None, str | None, str | None, str]:
    host = (urlparse(url).hostname or "").lower()
    if host in {"policy.example", "policy.local"}:
        return "healthy", 200, "Internal governed policy reference", None, "internal_governed"
    try:
        req = Request(url, method="HEAD", headers={"User-Agent": "investment-agent/0.1"})
        with urlopen(req, timeout=timeout) as response:  # noqa: S310
            code = int(getattr(response, "status", 200))
            if 200 <= code < 400:
                document_hash = None
                retention_status = "not_retained"
                notes = None
                if hash_content:
                    document_hash, retention_status, notes = _hash_document(
                        url,
                        timeout=timeout,
                        max_bytes=hash_max_bytes,
                    )
                return "healthy", code, notes, document_hash, retention_status
            return "degraded", code, f"Unexpected HTTP status {code}", None, "not_retained"
    except HTTPError as exc:
        return "broken", int(exc.code), str(exc), None, "not_retained"
    except URLError as exc:
        return "broken", None, str(exc), None, "not_retained"
    except Exception as exc:  # noqa: BLE001
        return "degraded", None, str(exc), None, "not_retained"


def refresh_citation_health(
    conn: sqlite3.Connection,
    citations: list[dict[str, Any]],
    *,
    settings: Settings,
    force: bool = False,
    max_checks: int = 12,
) -> list[dict[str, Any]]:
    ensure_citation_health_tables(conn)
    results: list[dict[str, Any]] = []
    now = datetime.now(UTC)
    ttl = timedelta(days=settings.blueprint_citation_health_max_age_days)
    checks_run = 0
    for citation in citations:
        source_id = str(citation.get("source_id") or "")
        url = str(citation.get("url") or "")
        if not source_id or not url:
            continue
        key = _citation_key(source_id, url)
        row = conn.execute(
            """
            SELECT citation_key, health_status, http_status, last_checked_at, retention_status, source_tier, publisher, authoritative_flag, document_type
            FROM citation_health_checks
            WHERE citation_key = ?
            """,
            (key,),
        ).fetchone()
        should_check = False
        if force:
            if row is None:
                should_check = True
            else:
                checked_at = row["last_checked_at"]
                parsed_checked = datetime.fromisoformat(str(checked_at).replace("Z", "+00:00")) if checked_at else None
                should_check = parsed_checked is None or (now - parsed_checked) >= ttl
        if should_check and checks_run < max_checks:
            health_status, http_status, notes, document_hash, retention_status = _check_url(
                url,
                timeout=5,
                hash_content=bool(
                    citation.get("purpose") in {"Identity", "Fees", "Domicile", "Distribution", "Tax context"}
                ),
                hash_max_bytes=int(settings.blueprint_citation_hash_max_bytes),
            )
            checks_run += 1
            conn.execute(
                """
                INSERT INTO citation_health_checks (
                  citation_key, source_id, url, publisher, source_tier, authoritative_flag, document_type,
                  retention_required, retention_status, health_status, http_status, document_hash,
                  last_checked_at, retrieved_at, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(citation_key) DO UPDATE SET
                  publisher=excluded.publisher,
                  source_tier=excluded.source_tier,
                  authoritative_flag=excluded.authoritative_flag,
                  document_type=excluded.document_type,
                  retention_required=excluded.retention_required,
                  health_status=excluded.health_status,
                  http_status=excluded.http_status,
                  last_checked_at=excluded.last_checked_at,
                  retrieved_at=excluded.retrieved_at,
                  notes=excluded.notes
                """,
                (
                    key,
                    source_id,
                    url,
                    citation.get("publisher"),
                    citation.get("tier"),
                    1 if citation.get("tier") in {"primary", "official"} else 0,
                    citation.get("purpose"),
                    1 if citation.get("purpose") in {"Identity", "Fees", "Domicile", "Distribution", "Tax context"} else 0,
                    retention_status,
                    health_status,
                    http_status,
                    document_hash,
                    _now_iso(),
                    citation.get("retrieved_at"),
                    notes,
                ),
            )
            row = conn.execute("SELECT * FROM citation_health_checks WHERE citation_key = ?", (key,)).fetchone()
        elif row is None:
            conn.execute(
                """
                INSERT INTO citation_health_checks (
                  citation_key, source_id, url, publisher, source_tier, authoritative_flag, document_type,
                  retention_required, retention_status, health_status, http_status, document_hash,
                  last_checked_at, retrieved_at, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'unknown', NULL, NULL, NULL, ?, ?)
                """,
                (
                    key,
                    source_id,
                    url,
                    citation.get("publisher"),
                    citation.get("tier"),
                    1 if citation.get("tier") in {"primary", "official"} else 0,
                    citation.get("purpose"),
                    1 if citation.get("purpose") in {"Identity", "Fees", "Domicile", "Distribution", "Tax context"} else 0,
                    "not_retained",
                    citation.get("retrieved_at"),
                    "Health check deferred",
                ),
            )
            row = conn.execute("SELECT * FROM citation_health_checks WHERE citation_key = ?", (key,)).fetchone()
        if row is not None:
            results.append(dict(row))
    conn.commit()
    return results


def summarize_citation_health(
    conn: sqlite3.Connection,
    citations: list[dict[str, Any]],
    *,
    settings: Settings,
    force_refresh: bool = False,
) -> dict[str, Any]:
    rows = refresh_citation_health(conn, citations, settings=settings, force=force_refresh)
    health_counts = {"healthy": 0, "degraded": 0, "broken": 0, "unknown": 0}
    retention_counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("health_status") or "unknown")
        health_counts[key] = health_counts.get(key, 0) + 1
        retention_key = str(row.get("retention_status") or "not_retained")
        retention_counts[retention_key] = retention_counts.get(retention_key, 0) + 1
    overall = "healthy"
    if health_counts.get("broken"):
        overall = "broken"
    elif health_counts.get("degraded"):
        overall = "degraded"
    elif health_counts.get("unknown"):
        overall = "unknown"
    return {
        "overall_status": overall,
        "counts": health_counts,
        "retention_counts": retention_counts,
        "hashed_documents_count": sum(1 for row in rows if row.get("document_hash")),
        "entries": [
            {
                "source_id": row.get("source_id"),
                "url": row.get("url"),
                "publisher": row.get("publisher"),
                "source_tier": row.get("source_tier"),
                "authoritative_flag": bool(row.get("authoritative_flag")),
                "document_type": row.get("document_type"),
                "retention_required": bool(row.get("retention_required")),
                "retention_status": row.get("retention_status"),
                "document_hash": row.get("document_hash"),
                "health_status": row.get("health_status"),
                "http_status": row.get("http_status"),
                "last_checked_at": row.get("last_checked_at"),
                "retrieved_at": row.get("retrieved_at"),
                "notes": row.get("notes"),
            }
            for row in rows
        ],
    }


def summarize_policy_citation_health(citations: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(citations)
    sourced = 0
    stale = 0
    broken = 0
    methodology_missing = 0
    for citation in citations:
        importance = str(citation.get("importance") or "").lower()
        url = str(citation.get("url") or "")
        observed_at = str(citation.get("observed_at") or "")
        if not url:
            broken += 1
        if "provenance=sourced" in importance or "provenance=external" in importance or "provenance=internal_policy" in importance:
            sourced += 1
        if "methodology=" not in importance:
            methodology_missing += 1
        if observed_at:
            try:
                observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
                if observed.tzinfo is None:
                    observed = observed.replace(tzinfo=UTC)
                age = (datetime.now(UTC) - observed).days
                if age > 365:
                    stale += 1
            except (ValueError, TypeError):
                stale += 1
    overall = "healthy"
    if broken or methodology_missing:
        overall = "broken"
    elif stale:
        overall = "degraded"
    elif sourced < total:
        overall = "degraded"
    return {
        "overall_status": overall,
        "total": total,
        "sourced_count": sourced,
        "stale_count": stale,
        "broken_count": broken,
        "methodology_missing_count": methodology_missing,
        "guidance_ready": overall == "healthy" and total > 0,
    }
