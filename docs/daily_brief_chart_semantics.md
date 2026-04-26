# Daily Brief Chart Semantics

## Purpose

The Daily Brief detail chart is a decision-context layer for `What Matters Now` cards.
It is not a decorative graph and it is not a generic market picture.

Each chart should help the investor answer:

1. Where the current signal sits versus recent history
2. Whether the move is near a review, confirm, or break line
3. Whether the move is holding or fading
4. Whether related markets are confirming or resisting the read

## Typed Chart System

The V2 Daily Brief chart contract supports:

1. `threshold_line`
2. `ohlc_candlestick`
3. `confirmation_strip`
4. `event_reaction_strip`
5. `comparison_bar`

The first production implementation defaults to `threshold_line` for most significant market signals.

## Backend Ownership

The backend owns:

1. `chart_kind`
2. `chart_question`
3. threshold lines and zones
4. confirmation state
5. optional forecast overlay eligibility
6. chart suppression rules
7. related-series selection
8. investor-facing chart annotations

The frontend only renders the typed payload.

## Chart Payload Shape

The shared contract lives in:

- `shared/v2_surface_contracts.ts`

Key fields:

1. `chart_kind`
2. `chart_question`
3. `primary_series`
4. `comparison_series`
5. `thresholds`
6. `current_point`
7. `chart_horizon`
8. `forecast_overlay`
9. `source_validity_footer`
10. `chart_suppressed_reason`
11. `confirmation_state`
12. `chart_annotations`
13. `confirmation_strip`
14. `event_reaction_strip`

## Backend Logic Files

Core chart semantics and payload construction:

- `backend/app/v2/features/chart_payload_builders.py`

Daily Brief chart attachment:

- `backend/app/v2/features/daily_brief_decision_synthesis.py`

Contract serialization:

- `backend/app/v2/surfaces/daily_brief/contract_builder.py`

Forecast qualification:

- `backend/app/v2/features/forecast_feature_service.py`

## Frontend Rendering Files

Typed chart adapter:

- `frontend-cortex/src/adapters.ts`

Detail-card chart mount:

- `frontend-cortex/src/App.tsx`

Main chart renderer:

- `frontend-cortex/src/charts/DailyBriefDecisionChart.tsx`

Threshold-line chart:

- `frontend-cortex/src/charts/ThresholdLineChart.tsx`

Confirmation and event strip renderer:

- `frontend-cortex/src/charts/SignalStrip.tsx`

Chart styles:

- `frontend-cortex/src/cortex-reference.css`

## Current Family Mapping

Default `threshold_line`:

1. rates
2. real yields
3. credit spreads
4. FX
5. WTI
6. gold
7. inflation-sensitive market signals

Optional `confirmation_strip`:

1. WTI
2. credit
3. equity
4. FX

Default `event_reaction_strip`:

1. policy cards
2. geopolitical cards

Suppress when there is no priced market reaction or no decision value.

## Forecast Overlay Rule

Forecast overlays are optional and secondary.

They only render when:

1. forecast support is usable
2. the path is not degraded
3. uncertainty is not too wide without enough trigger pressure
4. the overlay adds decision value

Forecast does not replace source truth.
It qualifies the path and trigger pressure around the factual reading.
