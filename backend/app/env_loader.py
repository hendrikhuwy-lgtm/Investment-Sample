from __future__ import annotations

import os
from pathlib import Path


def _env_flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _production_mode() -> bool:
    for name in ("IA_ENV", "APP_ENV", "ENVIRONMENT", "NODE_ENV"):
        if os.getenv(name, "").strip().lower() in {"prod", "production"}:
            return True
    return False


def _strip_wrapping_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def load_local_env(repo_root: Path | None = None, *, override: bool = False) -> Path | None:
    if _env_flag("IA_LOAD_LOCAL_ENV", "0"):
        pass
    elif _env_flag("IA_DISABLE_LOCAL_ENV", "0") or _production_mode():
        return None

    resolved_repo_root = repo_root or Path(__file__).resolve().parents[2]
    env_path = resolved_repo_root / "ops" / "env" / ".env.local"
    if not env_path.exists():
        return None

    for raw_line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_key = key.strip()
        if not env_key:
            continue
        env_value = _strip_wrapping_quotes(value)
        if override or env_key not in os.environ:
            os.environ[env_key] = env_value

    return env_path
