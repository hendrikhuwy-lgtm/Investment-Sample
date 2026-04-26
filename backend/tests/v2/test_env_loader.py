from __future__ import annotations

import os
from pathlib import Path


def test_local_env_loader_skips_env_local_in_production(monkeypatch, tmp_path: Path) -> None:
    env_dir = tmp_path / "ops" / "env"
    env_dir.mkdir(parents=True)
    (env_dir / ".env.local").write_text('export SAMPLE_SECRET="should-not-load"\n', encoding="utf-8")

    monkeypatch.setenv("IA_ENV", "production")
    monkeypatch.delenv("IA_LOAD_LOCAL_ENV", raising=False)
    monkeypatch.delenv("SAMPLE_SECRET", raising=False)

    from app.env_loader import load_local_env

    assert load_local_env(tmp_path) is None
    assert "SAMPLE_SECRET" not in os.environ


def test_local_env_loader_can_be_explicitly_enabled(monkeypatch, tmp_path: Path) -> None:
    env_dir = tmp_path / "ops" / "env"
    env_dir.mkdir(parents=True)
    (env_dir / ".env.local").write_text('export SAMPLE_SECRET="local-dev-value"\n', encoding="utf-8")

    monkeypatch.setenv("IA_ENV", "production")
    monkeypatch.setenv("IA_LOAD_LOCAL_ENV", "1")
    monkeypatch.delenv("SAMPLE_SECRET", raising=False)

    from app.env_loader import load_local_env

    assert load_local_env(tmp_path) == env_dir / ".env.local"
    assert os.environ["SAMPLE_SECRET"] == "local-dev-value"
