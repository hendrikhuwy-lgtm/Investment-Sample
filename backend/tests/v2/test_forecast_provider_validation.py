from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def forecast_probe_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    db_path = tmp_path / "forecast_probe.sqlite3"
    monkeypatch.setenv("IA_DB_PATH", str(db_path))
    monkeypatch.setenv("IA_FORECAST_BENCHMARK_MODE", "0")
    monkeypatch.setenv("IA_CHRONOS_BASE_URL", "http://127.0.0.1:8002")
    monkeypatch.setenv("IA_TIMESFM_BASE_URL", "http://127.0.0.1:8003")
    monkeypatch.setenv("IA_MOIRAI_BASE_URL", "http://127.0.0.1:8004")
    monkeypatch.setenv("IA_LAGLLAMA_BASE_URL", "http://127.0.0.1:8005")
    return db_path


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, payload: Any = None, json_error: bool = False) -> None:
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def json(self) -> Any:
        if self._json_error:
            raise ValueError("bad json")
        return self._payload


def test_env_base_url_defaults_only_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.v2.forecasting.adapters.common import env_base_url

    monkeypatch.delenv("IA_CHRONOS_BASE_URL", raising=False)
    assert env_base_url("IA_CHRONOS_BASE_URL", "http://127.0.0.1:8002") == "http://127.0.0.1:8002"

    monkeypatch.setenv("IA_CHRONOS_BASE_URL", "")
    assert env_base_url("IA_CHRONOS_BASE_URL", "http://127.0.0.1:8002") == ""


def test_adapter_uses_local_default_but_respects_explicit_blank(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.forecasting.adapters.chronos_adapter as chronos_adapter

    monkeypatch.delenv("IA_CHRONOS_BASE_URL", raising=False)
    assert chronos_adapter.base_url() == "http://127.0.0.1:8002"
    assert chronos_adapter.configured() is True

    monkeypatch.setenv("IA_CHRONOS_BASE_URL", "")
    assert chronos_adapter.base_url() == ""
    assert chronos_adapter.configured() is False


def test_probe_provider_accepts_canonical_response(
    forecast_probe_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.forecasting.adapters.common as common
    from app.v2.forecasting.store import list_provider_probes
    from app.v2.forecasting.validation import probe_provider

    def _fake_request(method: str, url: str, json: dict[str, Any] | None = None, **_: Any) -> _FakeResponse:
        if method == "GET" and url.endswith("/health"):
            return _FakeResponse(payload={"status": "ok"})
        assert json is not None
        horizon = int(json["horizon"])
        return _FakeResponse(
            payload={
                "request_id": json["request_id"],
                "provider": "chronos",
                "model_name": "chronos",
                "generated_at": "2026-04-03T00:00:00+00:00",
                "point_path": [105.0 + idx for idx in range(horizon)],
                "quantiles": {
                    "0.1": [104.0 + idx for idx in range(horizon)],
                    "0.5": [105.0 + idx for idx in range(horizon)],
                    "0.9": [106.0 + idx for idx in range(horizon)],
                },
                "anomaly_score": 0.12,
            }
        )

    monkeypatch.setattr(common.requests, "request", _fake_request)

    result = probe_provider("chronos")

    assert result["ready"] is True
    assert result["normalized_result"] is not None
    assert len(result["normalized_result"]["point_path"]) == 5
    probes = list_provider_probes(provider="chronos", limit=10)
    assert len(probes) >= 2
    assert any(row["endpoint"] == "/health" and row["success"] for row in probes)
    assert any(row["endpoint"] == "/forecast" and row["success"] for row in probes)


def test_probe_provider_fails_closed_on_horizon_mismatch(
    forecast_probe_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.forecasting.adapters.common as common
    from app.v2.forecasting.validation import probe_provider

    def _fake_request(method: str, url: str, json: dict[str, Any] | None = None, **_: Any) -> _FakeResponse:
        if method == "GET":
            return _FakeResponse(payload={"status": "ok"})
        return _FakeResponse(
            payload={
                "request_id": json["request_id"] if json else "probe",
                "model_name": "timesfm",
                "generated_at": "2026-04-03T00:00:00+00:00",
                "point_path": [101.0, 102.0],
            }
        )

    monkeypatch.setattr(common.requests, "request", _fake_request)

    result = probe_provider("timesfm")

    assert result["ready"] is False
    assert result["reason_code"] == "horizon_mismatch"
    assert result["forecast"]["horizon_ok"] is False


def test_latest_readiness_requires_repeated_timesfm_forecast_success(
    forecast_probe_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.forecasting.validation as validation

    monkeypatch.setattr(
        validation,
        "latest_provider_probe",
        lambda provider, endpoint=None: {
            "provider": provider,
            "endpoint": endpoint,
            "success": True,
            "error_code": None,
        },
    )
    monkeypatch.setattr(
        validation,
        "list_provider_probes",
        lambda provider=None, limit=100: [
            {"provider": "timesfm", "endpoint": "/forecast", "success": True},
            {"provider": "timesfm", "endpoint": "/health", "success": True},
        ],
    )

    row = validation.latest_readiness("timesfm")

    assert row["ready"] is False
    assert row["reason_code"] == "forecast_unstable"
    assert row["forecast_success_streak"] == 1
    assert row["stability_required"] == 2


def test_build_bundle_falls_back_on_bad_json(
    forecast_probe_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.forecasting.adapters.common as common
    from app.v2.features.forecast_feature_service import build_candidate_support_bundle

    def _fake_request(method: str, url: str, **_: Any) -> _FakeResponse:
        if method == "GET":
            return _FakeResponse(payload={"status": "ok"})
        return _FakeResponse(status_code=200, payload=None, json_error=True)

    monkeypatch.setattr(common.requests, "request", _fake_request)

    payload = build_candidate_support_bundle(
        candidate_id="candidate_instrument_vwra",
        symbol="VWRA",
        label="VWRA",
        sleeve_purpose="Global equity core",
        implication="Keep the core allocation under review.",
        summary="Core sleeve remains the anchor implementation.",
        current_value=104.8,
        history=[100.0, 101.5, 102.2, 103.1, 104.0, 104.8, 105.1, 105.3],
        timestamps=[
            "2026-03-25T00:00:00+00:00",
            "2026-03-26T00:00:00+00:00",
            "2026-03-27T00:00:00+00:00",
            "2026-03-28T00:00:00+00:00",
            "2026-03-29T00:00:00+00:00",
            "2026-03-30T00:00:00+00:00",
            "2026-03-31T00:00:00+00:00",
            "2026-04-01T00:00:00+00:00",
        ],
        surface_name="candidate_report",
    )

    assert payload["forecast_support"]["provider"] == "deterministic_baseline"
    assert payload["forecast_support"]["degraded_state"] == "bad_json"


def test_forecast_probe_and_readiness_routes(
    forecast_probe_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient

    import app.v2.forecasting.service as forecast_service
    from app.v2.app import app

    monkeypatch.setattr(
        forecast_service,
        "forecast_probe_payload",
        lambda provider=None: {"generated_at": "2026-04-03T00:00:00+00:00", "count": 1, "ready_count": 1, "providers": [{"provider": provider or "chronos", "ready": True}]},
    )
    monkeypatch.setattr(
        forecast_service,
        "forecast_readiness_payload",
        lambda: {"generated_at": "2026-04-03T00:00:00+00:00", "providers": [{"provider": "chronos", "ready": True}]},
    )

    with TestClient(app) as client:
        probe = client.post("/api/v2/admin/forecast/probe")
        readiness = client.get("/api/v2/admin/forecast/readiness")
        targeted = client.post("/api/v2/admin/forecast/probe/chronos")

    assert probe.status_code == 200
    assert probe.json()["providers"][0]["provider"] == "chronos"
    assert readiness.status_code == 200
    assert readiness.json()["providers"][0]["ready"] is True
    assert targeted.status_code == 200
    assert targeted.json()["providers"][0]["provider"] == "chronos"
