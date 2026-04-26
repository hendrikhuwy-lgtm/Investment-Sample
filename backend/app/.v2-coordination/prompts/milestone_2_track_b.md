# TRACK B — MILESTONE 2: Core Logic Layer + Doctrine Engine + Fixtures

Working directory: /Users/huwenyihendrik/Projects/investment-agent-track-b
Branch: track/b

Complete ALL tasks below before marking the gate file as done.

---

## TASK 1: READ CONTEXT FIRST

Read these files before writing anything:
- `backend/app/v2/core/domain_objects.py` — all 18 domain objects from M1
- `backend/app/v2/doctrine/doctrine_principle_registry.py` — 10 principles from M1
- `backend/app/v2/core/interpretation_engine.py` — existing stub from M1
- `backend/app/v2/core/holdings_overlay.py` — existing stub from M1
- `backend/app/v2/core/change_ledger.py` — existing stub from M1
- `shared/v2_surface_contracts.ts` — contracts you own
- `backend/app/services/policy_authority.py` — policy rubric donor
- `backend/app/services/policy_types.py` — policy type definitions
- `backend/app/.v2-coordination/fixtures/` — any fixtures already written

---

## TASK 2: IMPLEMENT INTERPRETATION ENGINE

Replace the stub in `backend/app/v2/core/interpretation_engine.py`:

```python
from backend.app.v2.core.domain_objects import (
    InstrumentTruth, MarketSeriesTruth, SignalPacket, InterpretationCard
)

def interpret(
    truth: InstrumentTruth,
    market: MarketSeriesTruth,
) -> tuple[SignalPacket, InterpretationCard]:
    """
    Produces a SignalPacket and InterpretationCard from truth + market data.
    Must NOT import from blueprint_payload_assembler or cortex_blueprint_presentation.
    """
```

Rules for the implementation:
- `SignalPacket.magnitude`: "significant" if `abs(change_pct_1d) > 2.0`, "moderate" if `> 0.5`, else "minor"
- `SignalPacket.direction`: "up" if positive change, "down" if negative, "neutral" if zero/None
- `InterpretationCard.implication_horizon`: "immediate" if significant, "near_term" if moderate, "long_term" if minor
- `InterpretationCard.conviction`: leave as "medium" for now — doctrine evaluator will adjust in M3
- `InterpretationCard.why_it_matters_economically`: generic template string based on asset_class
- `InterpretationCard.why_it_matters_here`: generic template string based on sleeve affiliation
- All None fields in input should produce graceful outputs (no crash)

---

## TASK 3: IMPLEMENT MANDATE RUBRIC

Create `backend/app/v2/core/mandate_rubric.py` (replace stub if exists):

```python
from backend.app.v2.core.domain_objects import (
    CandidateAssessment, PolicyBoundary, FrameworkRestraint, ConstraintSummary
)

def apply_rubric(
    assessment: CandidateAssessment,
    boundary: PolicyBoundary,
    restraints: list[FrameworkRestraint],
) -> ConstraintSummary:
    """
    Applies mandate policy boundary and framework restraints to a candidate assessment.
    Returns ConstraintSummary with annotations.
    Must NOT import from blueprint_payload_assembler.
    """
```

Rules:
- If `boundary.action_boundary == "blocked"`: set `overclaim_risk = "high"`
- If any restraint has `constraint_type == "conviction_adjustment"`: add its description to `doctrine_annotations`
- `overclaim_risk` default: "low" if no restraints, "medium" if any restraints present

---

## TASK 4: IMPLEMENT DOCTRINE EVALUATOR

Replace the stub in `backend/app/v2/doctrine/doctrine_evaluator.py`:

```python
from backend.app.v2.core.domain_objects import CandidateAssessment, ConstraintSummary, FrameworkRestraint
from backend.app.v2.doctrine.doctrine_principle_registry import PRINCIPLE_REGISTRY

def evaluate(
    candidate_assessment: CandidateAssessment,
    active_principles: list[str] | None = None,
) -> list[FrameworkRestraint]:
    """
    Evaluates a candidate assessment against doctrine principles.
    Returns a list of FrameworkRestraint objects to be passed to mandate_rubric.
    """
```

Rules:
- Default to all 10 principles if `active_principles` is None
- For each principle, produce a `FrameworkRestraint` with:
  - `restraint_id`: `f"{principle_id}:{candidate_assessment.candidate_id}"`
  - `principle_ref`: principle_id
  - `constraint_type`: "conviction_adjustment" for risk_before_return and avoid_overclaiming; "risk_annotation" for cycle_temperature and uncertainty_acknowledged; "boundary_note" for others
  - `description`: principle.description
- Must return at least one FrameworkRestraint for any non-empty assessment

---

## TASK 5: IMPLEMENT HOLDINGS OVERLAY (null-safe)

Replace the stub in `backend/app/v2/core/holdings_overlay.py`:

```python
def apply_overlay(base_contract: dict, holdings: any) -> dict:
    """
    Applies portfolio holdings overlay to a base surface contract.
    When holdings is None: returns base_contract UNCHANGED.
    When holdings present: adds holdings_overlay fields to a copy of base_contract.
    Must never mutate base_contract.
    """
```

Rules:
- `holdings=None` → return `base_contract` with `holdings_overlay_present=False` (no mutation)
- `holdings` present → return new dict with all base fields plus `holdings_overlay_present=True`
- Use `copy.deepcopy` to avoid mutation

---

## TASK 6: IMPLEMENT CHANGE LEDGER SCHEMA STUB

Replace the stub in `backend/app/v2/core/change_ledger.py` with a SQLAlchemy schema + stub functions:

```python
from sqlalchemy import Column, String, DateTime
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

class ChangeEventRecord(Base):
    __tablename__ = "v2_change_events"
    event_id = Column(String, primary_key=True)
    event_type = Column(String, nullable=False)
    surface_id = Column(String, nullable=False)
    changed_at_utc = Column(DateTime, nullable=False)
    summary = Column(String, nullable=False)

def record_change(event_type: str, surface_id: str, summary: str) -> str:
    """Stub — records a change event. Returns event_id. Full implementation in M6."""
    raise NotImplementedError("Change ledger not yet implemented — M6 target")

def get_diffs(surface_id: str, since_utc: str | None = None) -> list:
    """Stub — returns change diffs for a surface. Full implementation in M6."""
    raise NotImplementedError("Change ledger not yet implemented — M6 target")
```

---

## TASK 7: PRODUCE CANONICAL JSON FIXTURES

Write these two fixture files:

**`backend/app/.v2-coordination/fixtures/blueprint_explorer_contract_sample.json`**
A complete, realistic BlueprintExplorerContract JSON with:
- `surface_id`: "blueprint_explorer"
- `generated_at`: ISO 8601 timestamp
- `freshness_state`: "stored_valid_context"
- `holdings_overlay_present`: false
- `sleeves`: array of 3–5 BlueprintSleeveRow objects using real sleeve names from `backend/app/services/blueprint_candidate_truth.py` if available, else plausible names
- `market_state_summary`: 1–2 sentence interpretation string
- `review_posture`: one of "monitor" / "review_soon" / "act_now"

**`backend/app/.v2-coordination/fixtures/candidate_report_contract_sample.json`**
A complete CandidateReportContract JSON with:
- `surface_id`: "candidate_report"
- `candidate_id`: a real candidate_id from `backend/app/services/blueprint_candidate_truth.py` if available
- `freshness_state`: "stored_valid_context"
- `holdings_overlay_present`: false
- `investment_case`: 2–3 sentence string
- `current_implication`: 1–2 sentence string
- `main_tradeoffs`: array of 2–3 strings
- `baseline_comparisons`: array of 1–2 BaselineComparison objects
- `evidence_depth`: "substantial" / "moderate" / "limited"
- `mandate_boundary`: null or a 1-sentence string

---

## TASK 8: WRITE TESTS

Create `tests/v2/test_core_logic.py`:
```python
# Test that interpretation_engine.interpret() returns (SignalPacket, InterpretationCard)
# Test that holdings_overlay.apply_overlay(base, None) returns base unchanged
# Test that holdings_overlay.apply_overlay(base, {...}) returns new dict with holdings_overlay_present=True
# Test that doctrine_evaluator.evaluate(assessment) returns at least 1 FrameworkRestraint
# Test that mandate_rubric.apply_rubric() returns a ConstraintSummary
# CRITICAL: assert that no file in tests/v2/ imports from blueprint_payload_assembler
```

---

## GATE OUTPUT

When all 8 tasks are complete, write:
`backend/app/.v2-coordination/gates/milestone_2_track_b.json`

```json
{
  "interpretation_engine_done": true,
  "doctrine_evaluator_done": true,
  "holdings_overlay_done": true,
  "fixtures_produced": true
}
```

If any task is incomplete, set that field to `false` and add a `"notes"` field.
