from __future__ import annotations

from typing import Any


def get_regime_transition_context() -> dict[str, Any]:
    return {
        "status": "not_implemented",
        "message": "Regime transition modeling is deferred. No transition probabilities are computed in the current scope.",
        "required_inputs": [
            "historical_regime_labels",
            "transition_matrix",
            "continuous_score_mapping",
            "backtest_validation_dataset",
        ],
        "provenance": ["placeholder_interface"],
    }
