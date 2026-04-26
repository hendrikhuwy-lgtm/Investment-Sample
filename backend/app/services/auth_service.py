from __future__ import annotations

import hashlib
import os
import secrets
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any


SESSION_COOKIE_NAME = "ia_session"
SESSION_DAYS = 7
BOOTSTRAP_PRIMARY_USERNAME = os.getenv("IA_BOOTSTRAP_PRIMARY_USERNAME") or "John"
BOOTSTRAP_PRIMARY_PASSWORD = os.getenv("IA_BOOTSTRAP_PRIMARY_PASSWORD") or "HU123"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _pbkdf2(password: str, *, salt: str, iterations: int = 200_000) -> str:
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    return f"pbkdf2_sha256${iterations}${salt}${digest.hex()}"


def _verify_password(password: str, encoded: str) -> bool:
    try:
        algo, iterations_raw, salt, _hash = encoded.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        expected = _pbkdf2(password, salt=salt, iterations=int(iterations_raw))
        return secrets.compare_digest(expected, encoded)
    except Exception:  # noqa: BLE001
        return False


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def ensure_auth_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
          user_id TEXT PRIMARY KEY,
          username TEXT NOT NULL UNIQUE,
          display_name TEXT NOT NULL,
          email TEXT,
          password_hash TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          created_at TEXT NOT NULL,
          last_active_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS roles (
          role_id TEXT PRIMARY KEY,
          role_name TEXT NOT NULL UNIQUE,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_roles (
          user_role_id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          role_name TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_user_roles_user_role
        ON user_roles (user_id, role_name)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_sessions (
          session_id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          session_token_hash TEXT NOT NULL UNIQUE,
          issued_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          revoked_at TEXT,
          source_ip TEXT,
          user_agent TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_auth_sessions_user
        ON auth_sessions (user_id, expires_at DESC)
        """
    )
    _seed_roles(conn)
    _seed_bootstrap_users(conn)
    conn.commit()


def _seed_roles(conn: sqlite3.Connection) -> None:
    now = _now_iso()
    for role_name in ("admin", "portfolio_manager", "reviewer", "read_only"):
        conn.execute(
            """
            INSERT INTO roles (role_id, role_name, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(role_name) DO NOTHING
            """,
            (f"role_{uuid.uuid4().hex[:12]}", role_name, now),
        )


def _upsert_user(
    conn: sqlite3.Connection,
    *,
    username: str,
    display_name: str,
    email: str | None,
    password: str,
    roles: list[str],
    force_password_reset: bool = False,
) -> None:
    now = _now_iso()
    existing = conn.execute("SELECT user_id FROM users WHERE username = ? LIMIT 1", (username,)).fetchone()
    password_hash = _pbkdf2(password, salt=secrets.token_hex(16))
    if existing is None:
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        conn.execute(
            """
            INSERT INTO users (user_id, username, display_name, email, password_hash, status, created_at, last_active_at)
            VALUES (?, ?, ?, ?, ?, 'active', ?, NULL)
            """,
            (user_id, username, display_name, email, password_hash, now),
        )
    else:
        user_id = str(existing["user_id"])
        if force_password_reset:
            conn.execute(
                """
                UPDATE users
                SET display_name = ?, email = ?, password_hash = ?, status = 'active'
                WHERE user_id = ?
                """,
                (display_name, email, password_hash, user_id),
            )
        else:
            conn.execute(
                """
                UPDATE users
                SET display_name = COALESCE(?, display_name), email = COALESCE(?, email), status = 'active'
                WHERE user_id = ?
                """,
                (display_name, email, user_id),
            )
    for role_name in roles:
        conn.execute(
            """
            INSERT INTO user_roles (user_role_id, user_id, role_name, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, role_name) DO NOTHING
            """,
            (f"user_role_{uuid.uuid4().hex[:12]}", user_id, role_name, now),
        )


def _seed_bootstrap_users(conn: sqlite3.Connection) -> None:
    bootstrap_password = os.getenv("IA_BOOTSTRAP_ADMIN_PASSWORD") or "admin"
    _upsert_user(
        conn,
        username=BOOTSTRAP_PRIMARY_USERNAME,
        display_name=BOOTSTRAP_PRIMARY_USERNAME,
        email=None,
        password=BOOTSTRAP_PRIMARY_PASSWORD,
        roles=["admin"],
        force_password_reset=True,
    )
    _upsert_user(
        conn,
        username="admin",
        display_name="Platform Admin",
        email=None,
        password=bootstrap_password,
        roles=["admin"],
    )
    _upsert_user(
        conn,
        username="pm",
        display_name="Portfolio Manager",
        email=None,
        password=os.getenv("IA_BOOTSTRAP_PM_PASSWORD") or bootstrap_password,
        roles=["portfolio_manager"],
    )
    _upsert_user(
        conn,
        username="reviewer",
        display_name="Reviewer",
        email=None,
        password=os.getenv("IA_BOOTSTRAP_REVIEWER_PASSWORD") or bootstrap_password,
        roles=["reviewer"],
    )
    _upsert_user(
        conn,
        username="viewer",
        display_name="Read Only",
        email=None,
        password=os.getenv("IA_BOOTSTRAP_VIEWER_PASSWORD") or bootstrap_password,
        roles=["read_only"],
    )


def _serialize_user(conn: sqlite3.Connection, user_row: sqlite3.Row) -> dict[str, Any]:
    role_rows = conn.execute(
        "SELECT role_name FROM user_roles WHERE user_id = ? ORDER BY role_name ASC",
        (str(user_row["user_id"]),),
    ).fetchall()
    return {
        "user_id": str(user_row["user_id"]),
        "username": str(user_row["username"]),
        "display_name": str(user_row["display_name"]),
        "email": user_row["email"],
        "status": str(user_row["status"]),
        "created_at": user_row["created_at"],
        "last_active_at": user_row["last_active_at"],
        "roles": [str(row["role_name"]) for row in role_rows],
    }


def authenticate_user(conn: sqlite3.Connection, *, username: str, password: str) -> dict[str, Any] | None:
    ensure_auth_tables(conn)
    row = conn.execute(
        """
        SELECT user_id, username, display_name, email, password_hash, status, created_at, last_active_at
        FROM users
        WHERE username = ?
        LIMIT 1
        """,
        (username.strip(),),
    ).fetchone()
    if row is None or str(row["status"]) != "active":
        return None
    if not _verify_password(password, str(row["password_hash"])):
        return None
    return _serialize_user(conn, row)


def create_session(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    source_ip: str | None,
    user_agent: str | None,
) -> tuple[str, dict[str, Any]]:
    ensure_auth_tables(conn)
    raw_token = secrets.token_urlsafe(32)
    session_id = f"session_{uuid.uuid4().hex[:12]}"
    issued_at = datetime.now(UTC)
    expires_at = issued_at + timedelta(days=SESSION_DAYS)
    conn.execute(
        """
        INSERT INTO auth_sessions (
          session_id, user_id, session_token_hash, issued_at, expires_at, revoked_at, source_ip, user_agent
        ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
        """,
        (
            session_id,
            user_id,
            _hash_token(raw_token),
            issued_at.isoformat(),
            expires_at.isoformat(),
            source_ip,
            user_agent,
        ),
    )
    conn.execute("UPDATE users SET last_active_at = ? WHERE user_id = ?", (issued_at.isoformat(), user_id))
    conn.commit()
    return raw_token, {
        "session_id": session_id,
        "user_id": user_id,
        "issued_at": issued_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }


def get_session_user(conn: sqlite3.Connection, raw_token: str | None) -> dict[str, Any] | None:
    ensure_auth_tables(conn)
    if not raw_token:
        return None
    now = _now_iso()
    row = conn.execute(
        """
        SELECT s.session_id, s.user_id, s.issued_at, s.expires_at, s.revoked_at,
               u.user_id AS auth_user_id, u.username, u.display_name, u.email, u.status, u.created_at, u.last_active_at
        FROM auth_sessions s
        JOIN users u ON u.user_id = s.user_id
        WHERE s.session_token_hash = ?
          AND s.revoked_at IS NULL
          AND s.expires_at > ?
          AND u.status = 'active'
        LIMIT 1
        """,
        (_hash_token(raw_token), now),
    ).fetchone()
    if row is None:
        return None
    return {
        "session_id": str(row["session_id"]),
        "issued_at": row["issued_at"],
        "expires_at": row["expires_at"],
        "user": {
            "user_id": str(row["auth_user_id"]),
            "username": str(row["username"]),
            "display_name": str(row["display_name"]),
            "email": row["email"],
            "status": str(row["status"]),
            "created_at": row["created_at"],
            "last_active_at": row["last_active_at"],
            "roles": [str(role["role_name"]) for role in conn.execute("SELECT role_name FROM user_roles WHERE user_id = ? ORDER BY role_name ASC", (str(row["auth_user_id"]),)).fetchall()],
        },
    }


def revoke_session(conn: sqlite3.Connection, raw_token: str | None) -> None:
    ensure_auth_tables(conn)
    if not raw_token:
        return
    conn.execute(
        "UPDATE auth_sessions SET revoked_at = ? WHERE session_token_hash = ? AND revoked_at IS NULL",
        (_now_iso(), _hash_token(raw_token)),
    )
    conn.commit()


def bootstrap_credentials_hint() -> dict[str, str]:
    return {
        "username": BOOTSTRAP_PRIMARY_USERNAME,
        "password": BOOTSTRAP_PRIMARY_PASSWORD,
    }


def list_users(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_auth_tables(conn)
    rows = conn.execute(
        """
        SELECT user_id, username, display_name, email, status, created_at, last_active_at
        FROM users
        ORDER BY username ASC
        """
    ).fetchall()
    return [_serialize_user(conn, row) for row in rows]


def create_user(
    conn: sqlite3.Connection,
    *,
    username: str,
    display_name: str,
    email: str | None,
    password: str,
    roles: list[str],
) -> dict[str, Any]:
    ensure_auth_tables(conn)
    existing = conn.execute("SELECT user_id FROM users WHERE username = ? LIMIT 1", (username.strip(),)).fetchone()
    if existing is not None:
        raise ValueError("User already exists.")
    _upsert_user(
        conn,
        username=username.strip(),
        display_name=display_name.strip() or username.strip(),
        email=(email or None),
        password=password,
        roles=roles or ["read_only"],
        force_password_reset=True,
    )
    conn.commit()
    row = conn.execute(
        """
        SELECT user_id, username, display_name, email, status, created_at, last_active_at
        FROM users
        WHERE username = ?
        LIMIT 1
        """,
        (username.strip(),),
    ).fetchone()
    if row is None:
        raise ValueError("User creation failed.")
    return _serialize_user(conn, row)


def update_user(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    display_name: str | None = None,
    email: str | None = None,
    status: str | None = None,
    password: str | None = None,
    roles: list[str] | None = None,
) -> dict[str, Any]:
    ensure_auth_tables(conn)
    row = conn.execute(
        """
        SELECT user_id, username, display_name, email, password_hash, status, created_at, last_active_at
        FROM users
        WHERE user_id = ?
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    if row is None:
        raise ValueError("User not found.")
    assignments: list[str] = []
    values: list[Any] = []
    if display_name is not None:
        assignments.append("display_name = ?")
        values.append(display_name.strip() or str(row["display_name"]))
    if email is not None:
        assignments.append("email = ?")
        values.append(email or None)
    if status is not None:
        assignments.append("status = ?")
        values.append(status)
    if password:
        assignments.append("password_hash = ?")
        values.append(_pbkdf2(password, salt=secrets.token_hex(16)))
    if assignments:
        values.append(user_id)
        conn.execute(f"UPDATE users SET {', '.join(assignments)} WHERE user_id = ?", tuple(values))
    if roles is not None:
        conn.execute("DELETE FROM user_roles WHERE user_id = ?", (user_id,))
        now = _now_iso()
        for role_name in roles:
            conn.execute(
                """
                INSERT INTO user_roles (user_role_id, user_id, role_name, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, role_name) DO NOTHING
                """,
                (f"user_role_{uuid.uuid4().hex[:12]}", user_id, role_name, now),
            )
    conn.commit()
    refreshed = conn.execute(
        """
        SELECT user_id, username, display_name, email, status, created_at, last_active_at
        FROM users
        WHERE user_id = ?
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    if refreshed is None:
        raise ValueError("User update failed.")
    return _serialize_user(conn, refreshed)
