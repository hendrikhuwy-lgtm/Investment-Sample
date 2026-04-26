from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.config import get_db_path


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_evidence_object_links (
          link_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          object_type TEXT NOT NULL,
          object_id TEXT NOT NULL,
          object_label TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_v2_evidence_object_links_target
        ON v2_evidence_object_links (candidate_id, object_type, object_id)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_evidence_documents (
          document_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          linked_object_type TEXT NOT NULL,
          linked_object_id TEXT NOT NULL,
          linked_object_label TEXT NOT NULL,
          title TEXT NOT NULL,
          document_type TEXT NOT NULL,
          url TEXT,
          retrieved_utc TEXT,
          freshness_state TEXT NOT NULL,
          stale INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_evidence_claims (
          claim_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          object_type TEXT NOT NULL,
          object_id TEXT NOT NULL,
          object_label TEXT NOT NULL,
          claim_text TEXT NOT NULL,
          claim_meta TEXT NOT NULL,
          directness TEXT NOT NULL,
          freshness_state TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_evidence_benchmark_mappings (
          mapping_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          sleeve_label TEXT NOT NULL,
          instrument_label TEXT NOT NULL,
          benchmark_label TEXT NOT NULL,
          baseline_label TEXT NOT NULL,
          directness TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_evidence_tax_assumptions (
          assumption_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          label TEXT NOT NULL,
          value TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_evidence_gaps (
          gap_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          object_label TEXT NOT NULL,
          issue_text TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _ensure_object_link(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    object_type: str,
    object_id: str,
    object_label: str,
) -> None:
    row = conn.execute(
        """
        SELECT link_id
        FROM v2_evidence_object_links
        WHERE candidate_id = ? AND object_type = ? AND object_id = ?
        """,
        (candidate_id, object_type, object_id),
    ).fetchone()
    if row is not None:
        return
    conn.execute(
        """
        INSERT INTO v2_evidence_object_links (
          link_id, candidate_id, object_type, object_id, object_label, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
          f"evidence_link_{uuid.uuid4().hex}",
          candidate_id,
          object_type,
          object_id,
          object_label,
          _now_iso(),
        ),
    )


def add_document(
    candidate_id: str,
    *,
    linked_object_type: str,
    linked_object_id: str,
    linked_object_label: str,
    title: str,
    document_type: str,
    url: str | None,
    retrieved_utc: str | None,
    freshness_state: str,
    stale: bool,
) -> dict[str, Any]:
    document = {
        "document_id": f"evidence_document_{uuid.uuid4().hex}",
        "candidate_id": candidate_id,
        "linked_object_type": linked_object_type,
        "linked_object_id": linked_object_id,
        "linked_object_label": linked_object_label,
        "title": title,
        "document_type": document_type,
        "url": url,
        "retrieved_utc": retrieved_utc,
        "freshness_state": freshness_state,
        "stale": stale,
        "created_at": _now_iso(),
    }
    with _connection() as conn:
        _ensure_object_link(
            conn,
            candidate_id=candidate_id,
            object_type=linked_object_type,
            object_id=linked_object_id,
            object_label=linked_object_label,
        )
        conn.execute(
            """
            INSERT INTO v2_evidence_documents (
              document_id, candidate_id, linked_object_type, linked_object_id, linked_object_label,
              title, document_type, url, retrieved_utc, freshness_state, stale, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document["document_id"],
                document["candidate_id"],
                document["linked_object_type"],
                document["linked_object_id"],
                document["linked_object_label"],
                document["title"],
                document["document_type"],
                document["url"],
                document["retrieved_utc"],
                document["freshness_state"],
                int(document["stale"]),
                document["created_at"],
            ),
        )
        conn.commit()
    return document


def add_claim(
    candidate_id: str,
    *,
    object_type: str,
    object_id: str,
    object_label: str,
    claim_text: str,
    claim_meta: str,
    directness: str,
    freshness_state: str,
) -> dict[str, Any]:
    claim = {
        "claim_id": f"evidence_claim_{uuid.uuid4().hex}",
        "candidate_id": candidate_id,
        "object_type": object_type,
        "object_id": object_id,
        "object_label": object_label,
        "claim_text": claim_text,
        "claim_meta": claim_meta,
        "directness": directness,
        "freshness_state": freshness_state,
        "created_at": _now_iso(),
    }
    with _connection() as conn:
        _ensure_object_link(
            conn,
            candidate_id=candidate_id,
            object_type=object_type,
            object_id=object_id,
            object_label=object_label,
        )
        conn.execute(
            """
            INSERT INTO v2_evidence_claims (
              claim_id, candidate_id, object_type, object_id, object_label,
              claim_text, claim_meta, directness, freshness_state, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim["claim_id"],
                claim["candidate_id"],
                claim["object_type"],
                claim["object_id"],
                claim["object_label"],
                claim["claim_text"],
                claim["claim_meta"],
                claim["directness"],
                claim["freshness_state"],
                claim["created_at"],
            ),
        )
        conn.commit()
    return claim


def add_mapping(
    candidate_id: str,
    *,
    sleeve_label: str,
    instrument_label: str,
    benchmark_label: str,
    baseline_label: str,
    directness: str,
) -> dict[str, Any]:
    mapping = {
        "mapping_id": f"evidence_mapping_{uuid.uuid4().hex}",
        "candidate_id": candidate_id,
        "sleeve_label": sleeve_label,
        "instrument_label": instrument_label,
        "benchmark_label": benchmark_label,
        "baseline_label": baseline_label,
        "directness": directness,
        "created_at": _now_iso(),
    }
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO v2_evidence_benchmark_mappings (
              mapping_id, candidate_id, sleeve_label, instrument_label, benchmark_label,
              baseline_label, directness, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                mapping["mapping_id"],
                mapping["candidate_id"],
                mapping["sleeve_label"],
                mapping["instrument_label"],
                mapping["benchmark_label"],
                mapping["baseline_label"],
                mapping["directness"],
                mapping["created_at"],
            ),
        )
        conn.commit()
    return mapping


def add_tax_assumption(candidate_id: str, *, label: str, value: str) -> dict[str, Any]:
    assumption = {
        "assumption_id": f"evidence_tax_{uuid.uuid4().hex}",
        "candidate_id": candidate_id,
        "label": label,
        "value": value,
        "created_at": _now_iso(),
    }
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO v2_evidence_tax_assumptions (
              assumption_id, candidate_id, label, value, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                assumption["assumption_id"],
                assumption["candidate_id"],
                assumption["label"],
                assumption["value"],
                assumption["created_at"],
            ),
        )
        conn.commit()
    return assumption


def add_gap(candidate_id: str, *, object_label: str, issue_text: str) -> dict[str, Any]:
    gap = {
        "gap_id": f"evidence_gap_{uuid.uuid4().hex}",
        "candidate_id": candidate_id,
        "object_label": object_label,
        "issue_text": issue_text,
        "created_at": _now_iso(),
    }
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO v2_evidence_gaps (
              gap_id, candidate_id, object_label, issue_text, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                gap["gap_id"],
                gap["candidate_id"],
                gap["object_label"],
                gap["issue_text"],
                gap["created_at"],
            ),
        )
        conn.commit()
    return gap


def read_workspace(candidate_id: str) -> dict[str, list[dict[str, Any]]]:
    with _connection() as conn:
        object_links = [dict(row) for row in conn.execute(
            """
            SELECT * FROM v2_evidence_object_links
            WHERE candidate_id = ?
            ORDER BY created_at ASC
            """,
            (candidate_id,),
        ).fetchall()]
        documents = [dict(row) for row in conn.execute(
            """
            SELECT * FROM v2_evidence_documents
            WHERE candidate_id = ?
            ORDER BY created_at DESC
            """,
            (candidate_id,),
        ).fetchall()]
        claims = [dict(row) for row in conn.execute(
            """
            SELECT * FROM v2_evidence_claims
            WHERE candidate_id = ?
            ORDER BY created_at DESC
            """,
            (candidate_id,),
        ).fetchall()]
        mappings = [dict(row) for row in conn.execute(
            """
            SELECT * FROM v2_evidence_benchmark_mappings
            WHERE candidate_id = ?
            ORDER BY created_at DESC
            """,
            (candidate_id,),
        ).fetchall()]
        tax_assumptions = [dict(row) for row in conn.execute(
            """
            SELECT * FROM v2_evidence_tax_assumptions
            WHERE candidate_id = ?
            ORDER BY created_at DESC
            """,
            (candidate_id,),
        ).fetchall()]
        gaps = [dict(row) for row in conn.execute(
            """
            SELECT * FROM v2_evidence_gaps
            WHERE candidate_id = ?
            ORDER BY created_at DESC
            """,
            (candidate_id,),
        ).fetchall()]
    return {
        "object_links": object_links,
        "documents": documents,
        "claims": claims,
        "mappings": mappings,
        "tax_assumptions": tax_assumptions,
        "gaps": gaps,
    }
