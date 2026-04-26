from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class DoctrinePrinciple(BaseModel):
    model_config = ConfigDict(extra="forbid")

    principle_id: str
    title: str
    objective: str
    evaluation_focus: list[str] = Field(default_factory=list)
    default_posture: str = "neutral"


PRINCIPLE_REGISTRY: dict[str, DoctrinePrinciple] = {
    "circle_of_competence": DoctrinePrinciple(
        principle_id="circle_of_competence",
        title="Circle of Competence",
        objective="Reduce conviction when the instrument or claim set is not easy to explain or monitor.",
        evaluation_focus=["instrument simplicity", "explainability", "source quality"],
        default_posture="constraining",
    ),
    "price_vs_value": DoctrinePrinciple(
        principle_id="price_vs_value",
        title="Price Versus Value",
        objective="Separate attractive exposure from attractive implementation.",
        evaluation_focus=["fees", "tracking quality", "switching friction"],
    ),
    "risk_before_return": DoctrinePrinciple(
        principle_id="risk_before_return",
        title="Risk Before Return",
        objective="Make downside, fragility, and policy limits visible before upside claims.",
        evaluation_focus=["drawdown path", "concentration", "portfolio fragility"],
        default_posture="cautious",
    ),
    "cycle_temperature": DoctrinePrinciple(
        principle_id="cycle_temperature",
        title="Cycle Temperature",
        objective="Use macro and cycle context to cool or warm conviction without replacing evidence.",
        evaluation_focus=["regime claims", "macro dependence", "timing sensitivity"],
        default_posture="cautious",
    ),
    "asymmetry": DoctrinePrinciple(
        principle_id="asymmetry",
        title="Asymmetry",
        objective="Favour payoff shapes where likely downside is controlled relative to sleeve benefit.",
        evaluation_focus=["downside bound", "upside relevance", "replacement edge"],
    ),
    "patience_and_time_horizon": DoctrinePrinciple(
        principle_id="patience_and_time_horizon",
        title="Patience and Time Horizon",
        objective="Reward choices that fit low-turnover, long-horizon implementation discipline.",
        evaluation_focus=["churn", "time horizon", "need for action now"],
        default_posture="supportive",
    ),
    "uncertainty_acknowledged": DoctrinePrinciple(
        principle_id="uncertainty_acknowledged",
        title="Uncertainty Acknowledged",
        objective="Expose uncertainty directly in the user-visible contract.",
        evaluation_focus=["forecast humility", "confidence", "scenario branching"],
        default_posture="cautious",
    ),
    "avoid_overclaiming": DoctrinePrinciple(
        principle_id="avoid_overclaiming",
        title="Avoid Overclaiming",
        objective="Keep language inside what evidence and benchmark authority can support.",
        evaluation_focus=["claim boundary", "benchmark authority", "evidence depth"],
        default_posture="cautious",
    ),
    "implementation_discipline": DoctrinePrinciple(
        principle_id="implementation_discipline",
        title="Implementation Discipline",
        objective="Force operational, tax, and switching costs into the decision state.",
        evaluation_focus=["tax friction", "turnover", "execution burden"],
        default_posture="constraining",
    ),
    "sleeve_purpose_integrity": DoctrinePrinciple(
        principle_id="sleeve_purpose_integrity",
        title="Sleeve Purpose Integrity",
        objective="Ensure the candidate still matches the sleeve's reason for existing.",
        evaluation_focus=["sleeve purpose", "role drift", "portfolio fit"],
        default_posture="blocking",
    ),
}


def get_principle(principle_id: str) -> DoctrinePrinciple:
    return PRINCIPLE_REGISTRY[str(principle_id)]


def list_principles() -> list[DoctrinePrinciple]:
    return list(PRINCIPLE_REGISTRY.values())

