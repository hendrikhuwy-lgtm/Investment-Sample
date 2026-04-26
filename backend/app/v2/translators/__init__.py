"""Donor-to-V2 translation helpers."""

from app.v2.translators.benchmark_truth_translator import translate as translate_benchmark_truth
from app.v2.translators.instrument_truth_translator import translate as translate_instrument_truth
from app.v2.translators.macro_signal_translator import translate as translate_macro_signal
from app.v2.translators.market_signal_translator import translate as translate_market_signal
from app.v2.translators.news_signal_translator import translate as translate_news_signal

__all__ = [
    "translate_benchmark_truth",
    "translate_instrument_truth",
    "translate_macro_signal",
    "translate_market_signal",
    "translate_news_signal",
]
