from __future__ import annotations

import sqlite3
from pathlib import Path

from app.config import Settings
from app.models.db import connect, init_db
from app.services import ingest_mcp
from app.models.migrations import SCHEMA_VERSION
from scripts.merge_legacy_db import merge_legacy_db


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "app" / "storage" / "schema.sql"


def test_migrations_run_clean_on_empty_db(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.sqlite3"
    conn = connect(db_path)
    report = init_db(conn, SCHEMA_PATH)

    row = conn.execute("SELECT schema_version FROM schema_meta WHERE id = 1").fetchone()
    assert row is not None
    assert int(row[0]) == SCHEMA_VERSION
    assert report["post"]["after"] == SCHEMA_VERSION
    conn.close()


def test_run_id_columns_exist_after_init(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime.sqlite3"
    conn = connect(db_path)
    init_db(conn, SCHEMA_PATH)

    for table_name in ("email_runs", "daily_logs", "mcp_connectivity_runs", "mcp_items", "outbox_artifacts"):
        cols = {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()}
        assert "run_id" in cols
    conn.close()


def test_get_db_path_is_used_by_ingest(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "coherence.sqlite3"
    conn = connect(db_path)
    init_db(conn, SCHEMA_PATH)
    conn.close()

    ingest_calls = {"n": 0}

    def fake_ingest_db_path(*args, **kwargs):  # noqa: ANN002, ANN003
        ingest_calls["n"] += 1
        return db_path

    monkeypatch.setattr(ingest_mcp, "get_db_path", fake_ingest_db_path)
    monkeypatch.setattr(ingest_mcp, "parse_registry_snapshot", lambda _path: [])
    monkeypatch.setattr(ingest_mcp, "_load_connector_candidate_records", lambda _settings: [])
    monkeypatch.setattr(
        ingest_mcp,
        "mcp_network_diagnostics",
        lambda settings=None, limit=10: {
            "hosts": [],
            "dns_v4_ok_count": 0,
            "dns_v6_ok_count": 0,
            "tcp_v4_ok_count": 0,
            "tcp_v6_ok_count": 0,
            "tls_v4_ok_count": 0,
            "tls_v6_ok_count": 0,
            "dns_resolution_failures": [],
            "tcp_connectivity_failures": [],
            "tls_handshake_failures": [],
            "ipv6_unhealthy": True,
            "ipv6_probe_error": "test",
            "proxy_env_detected": False,
            "proxy_mode": "auto",
            "network_profile": "dns_broken",
            "environment_verdict": "dns_broken",
        },
    )

    settings = Settings(db_path=str(db_path), mcp_live_required=False, mcp_priority_mode="all")
    result = ingest_mcp.ingest_mcp_omni(settings=settings, enforce_live_gate=False)
    assert result.total_servers == 0
    assert ingest_calls["n"] >= 1


def _create_merge_fixture(path: Path, fill_rows: bool) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS daily_logs (
              log_id TEXT PRIMARY KEY,
              run_id TEXT,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS email_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_date_sgt TEXT NOT NULL,
              attempted_at TEXT NOT NULL,
              status TEXT NOT NULL,
              recipient TEXT NOT NULL,
              run_id TEXT
            );
            CREATE TABLE IF NOT EXISTS mcp_connectivity_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id TEXT,
              server_id TEXT NOT NULL,
              server_name TEXT NOT NULL,
              endpoint_url TEXT NOT NULL,
              endpoint_type TEXT NOT NULL,
              connectable INTEGER NOT NULL,
              status TEXT NOT NULL,
              started_at TEXT NOT NULL,
              finished_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mcp_items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id TEXT,
              server_id TEXT NOT NULL,
              retrieved_at TEXT NOT NULL,
              raw_hash TEXT NOT NULL,
              item_type TEXT NOT NULL,
              item_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbox_artifacts (
              artifact_id TEXT PRIMARY KEY,
              run_id TEXT,
              artifact_type TEXT NOT NULL,
              artifact_path TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            """
        )
        if fill_rows:
            conn.execute(
                "INSERT INTO daily_logs (log_id, run_id, created_at) VALUES (?, ?, ?)",
                ("log_1", "run_1", "2026-02-19T02:07:00+00:00"),
            )
            conn.execute(
                """
                INSERT INTO email_runs (run_date_sgt, attempted_at, status, recipient, run_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("2026-02-19", "2026-02-19T02:07:11+00:00", "generated_no_send", "test@example.com", "run_1"),
            )
            conn.execute(
                """
                INSERT INTO mcp_connectivity_runs (
                  run_id, server_id, server_name, endpoint_url, endpoint_type,
                  connectable, status, started_at, finished_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "run_1",
                    "srv_1",
                    "Server 1",
                    "https://example.com/mcp",
                    "http",
                    1,
                    "ok",
                    "2026-02-19T02:07:20+00:00",
                    "2026-02-19T02:07:21+00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO mcp_items (run_id, server_id, retrieved_at, raw_hash, item_type, item_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("run_1", "srv_1", "2026-02-19T02:07:21+00:00", "hash", "resource", "{}"),
            )
            conn.execute(
                """
                INSERT INTO outbox_artifacts (artifact_id, run_id, artifact_type, artifact_path, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "art_1",
                    "run_1",
                    "html",
                    "/tmp/sample.html",
                    "2026-02-19T02:07:30+00:00",
                ),
            )
        conn.commit()
    finally:
        conn.close()


def test_merge_legacy_db_increases_canonical_counts(tmp_path: Path) -> None:
    canonical = tmp_path / "canonical.sqlite3"
    legacy = tmp_path / "legacy.sqlite3"
    _create_merge_fixture(canonical, fill_rows=False)
    _create_merge_fixture(legacy, fill_rows=True)

    result = merge_legacy_db(legacy_path=legacy, canonical_path=canonical, rename_legacy=True)
    assert result["status"] == "merged"

    before = result["counts_before"]
    after = result["counts_after"]
    assert after["daily_logs"] > before["daily_logs"]
    assert after["email_runs"] > before["email_runs"]
    assert after["mcp_connectivity_runs"] > before["mcp_connectivity_runs"]
    assert after["mcp_items"] > before["mcp_items"]
    assert after["outbox_artifacts"] > before["outbox_artifacts"]

    backup_path = Path(str(result["legacy_backup_path"]))
    assert backup_path.exists()
    assert not legacy.exists()
