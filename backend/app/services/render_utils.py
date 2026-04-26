from __future__ import annotations

import html
import re
from typing import Any


RANGE_PATTERN = re.compile(
    r"(?<![\w/*])([+-]?\d[\d,]*(?:\.\d+)?%?\s*-\s*[+-]?\d[\d,]*(?:\.\d+)?%?)(?![\w/*])"
)
NUMBER_PATTERN = re.compile(r"(?<![\w/*])([+-]?\d[\d,]*(?:\.\d+)?(?:%|x)?)(?![%\w/*])")
SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+")
ISO_DATE_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


def escape_html(text: str) -> str:
    return html.escape(text, quote=True)


def split_sentences(text: str) -> list[str]:
    sentences = [part.strip() for part in SENTENCE_SPLIT_PATTERN.split(text.strip()) if part.strip()]
    if not sentences and text.strip():
        return [text.strip()]
    return sentences


def first_sentence(text: str) -> str:
    sentences = split_sentences(text)
    return sentences[0] if sentences else ""


def _protect_iso_dates(text: str) -> tuple[str, dict[str, str]]:
    replacements: dict[str, str] = {}

    def repl(match: re.Match[str]) -> str:
        key = f"__DATE_{len(replacements)}__"
        replacements[key] = match.group(0)
        return key

    return ISO_DATE_PATTERN.sub(repl, text), replacements


def _restore_iso_dates(text: str, replacements: dict[str, str]) -> str:
    for key, value in replacements.items():
        text = text.replace(key, value)
    return text


def emphasize_numbers_markdown(text: str) -> str:
    protected, replacements = _protect_iso_dates(text)
    emphasized = RANGE_PATTERN.sub(lambda match: f"**{match.group(1)}**", protected)
    emphasized = NUMBER_PATTERN.sub(lambda match: f"**{match.group(1)}**", emphasized)
    return _restore_iso_dates(emphasized, replacements)


def emphasize_numbers_html(text: str) -> str:
    protected, replacements = _protect_iso_dates(text)
    escaped = escape_html(protected)
    emphasized = RANGE_PATTERN.sub(lambda match: f"<strong>{match.group(1)}</strong>", escaped)
    emphasized = NUMBER_PATTERN.sub(lambda match: f"<strong>{match.group(1)}</strong>", emphasized)
    return _restore_iso_dates(emphasized, replacements)


def range_bar_percent(range_bar: str) -> int | None:
    filled = range_bar.count("█")
    empty = range_bar.count("░")
    total = filled + empty
    if total <= 0:
        return None
    return int(round((filled / total) * 100))


def yes_no(value: Any) -> str:
    return "Yes" if bool(value) else "No"


def split_item_and_why(text: str) -> tuple[str, str]:
    normalized = " ".join(text.split())
    parts = normalized.split(":", 1)
    if len(parts) == 2 and len(parts[0]) <= 80:
        left = parts[0].strip()
        right = parts[1].strip()
        if right:
            return left, right
    if len(normalized) <= 120:
        return normalized, "Publicly sourced proxy context for large-player activity."
    return normalized[:120] + "...", normalized[120:].strip() or "Publicly sourced proxy context."
