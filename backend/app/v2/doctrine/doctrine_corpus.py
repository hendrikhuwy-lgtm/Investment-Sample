from __future__ import annotations

from typing import TypedDict


class DoctrineCorpusEntry(TypedDict):
    author: str
    work: str
    guidance: str


DOCTRINE_CORPUS: dict[str, list[DoctrineCorpusEntry]] = {
    "circle_of_competence": [
        {
            "author": "Buffett / Berkshire",
            "work": "shareholder letters",
            "guidance": "Stay inside understandable businesses or instruments, and admit when the edge is absent.",
        },
        {
            "author": "Howard Marks / Oaktree",
            "work": "memos",
            "guidance": "Humility about what is knowable should narrow conviction before capital is committed.",
        },
    ],
    "price_vs_value": [
        {
            "author": "Buffett / Berkshire",
            "work": "shareholder letters",
            "guidance": "A good asset can still be a weak decision if price, costs, or implementation erase the edge.",
        }
    ],
    "risk_before_return": [
        {
            "author": "Howard Marks / Oaktree",
            "work": "The Most Important Thing",
            "guidance": "The first question is what can go wrong and how exposed the portfolio becomes if it does.",
        }
    ],
    "cycle_temperature": [
        {
            "author": "Howard Marks / Oaktree",
            "work": "cycle memos",
            "guidance": "Cycle awareness should adjust aggressiveness, not manufacture certainty.",
        }
    ],
    "asymmetry": [
        {
            "author": "Howard Marks / Oaktree",
            "work": "memos",
            "guidance": "Prefer setups where downside is bounded relative to the upside that matters for the sleeve.",
        }
    ],
    "patience_and_time_horizon": [
        {
            "author": "Buffett / Berkshire",
            "work": "shareholder letters",
            "guidance": "Time horizon is an edge only if the decision process is willing to wait and avoid churn.",
        }
    ],
    "uncertainty_acknowledged": [
        {
            "author": "Howard Marks / Oaktree",
            "work": "memos",
            "guidance": "Forecasts can frame possibilities, but uncertainty must remain visible in the contract.",
        }
    ],
    "avoid_overclaiming": [
        {
            "author": "Buffett / Berkshire",
            "work": "shareholder letters",
            "guidance": "A narrow conclusion stated honestly is better than a confident claim the evidence cannot carry.",
        }
    ],
    "implementation_discipline": [
        {
            "author": "Buffett / Berkshire",
            "work": "shareholder letters",
            "guidance": "Execution friction, taxes, and turnover belong in the investment decision, not after it.",
        }
    ],
    "sleeve_purpose_integrity": [
        {
            "author": "Howard Marks / Oaktree",
            "work": "memos",
            "guidance": "Each sleeve should do a clear job; doctrinal conviction weakens when role drift appears.",
        }
    ],
}


def corpus_entries_for_principle(principle_id: str) -> list[DoctrineCorpusEntry]:
    return list(DOCTRINE_CORPUS.get(str(principle_id or ""), []))

