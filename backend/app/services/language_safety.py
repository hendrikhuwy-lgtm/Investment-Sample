from __future__ import annotations

import re
from typing import Iterable


PROHIBITED_PATTERNS: tuple[str, ...] = (
    r"\bbuy\b",
    r"\bsell\b",
    r"\ballocate now\b",
    r"\bincrease exposure\b",
    r"\breduce exposure\b",
    r"\brebalance now\b",
)

PERSISTENT_DISCLAIMER = (
    "Objective diagnostic aggregator for monitoring and implementation review only; "
    "no trade directives are provided."
)


def find_prohibited_language(text: str) -> list[str]:
    lower = text.lower()
    matched: list[str] = []
    for pattern in PROHIBITED_PATTERNS:
        if re.search(pattern, lower):
            matched.append(pattern)
    return matched


def assert_no_directive_language(texts: Iterable[str]) -> None:
    violations: list[str] = []
    for text in texts:
        violations.extend(find_prohibited_language(text))
    if violations:
        joined = ", ".join(sorted(set(violations)))
        raise ValueError(f"Prohibited directive language detected: {joined}")
