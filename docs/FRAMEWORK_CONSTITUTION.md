# Framework Constitution

This document reflects the live framework doctrine now implemented in the backend canonical decision path.

## Investor Doctrine
- Singapore retail investor
- Long-horizon accumulation
- Passive-first
- Tax-aware
- Low-turnover
- Manual approval required for non-rebalance changes

## Default Stance
- Default output is no change.
- A candidate is not promoted because it is interesting or merely acceptable.
- Core sleeves require clean evidence, clean comparison, and practical edge over the incumbent.

## Promotion Ladder
- `research_only`
- `acceptable`
- `near_decision_ready`
- `buyable`

## Buyable Blockers
A candidate cannot become `buyable` if any of the following remain true:
- core holdings incomplete
- current-holding comparison incomplete
- nearest-rival comparison weak
- significant tax uncertainty
- benchmark authority too weak
- implementation friction unclear
- structure too complex
- mandate complexity violation
- switch cost unclear
- forecast-dependent case

## Portfolio-First Doctrine
- Canonical action boundary is backend-owned.
- `no_change` is preferred when practical edge is too small after friction.
- Non-rebalance `ADD`, `REPLACE`, and `TRIM` decisions require manual approval.

## Daily Brief Doctrine
- Daily Brief is a monitoring-first surface.
- Primary action states are `ignore`, `monitor`, and `review`.
- `urgent_review` and `escalate` are downgraded to `review`, not direct action.
- The brief must not imply a trade instruction.

## Frontend Doctrine
- Frontend renders canonical backend decision/report truth.
- Frontend may normalize payloads and group sections.
- Frontend may not rewrite recommendation meaning, trust, or action boundaries.
