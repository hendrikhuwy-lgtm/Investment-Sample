# V2 Frontend Target Map

| Surface | Canonical behavior source | Current page file | V2 route | Legacy routes to remove |
| --- | --- | --- | --- | --- |
| Blueprint Explorer | Blueprint sleeve comparison, implementation review, and research split currently anchored by blueprint page behavior | `frontend/src/pages/blueprint.tsx` | `/api/v2/surfaces/blueprint/explorer` | `/api/platform/portfolio-blueprint` and blueprint comparison helpers on legacy platform endpoints |
| Candidate Report | Candidate-detail narrative contract owned by V2; requested live page path is not present in this branch and the closest current prototype is `frontend/src/pages/blueprint_candidate_detail_sample.tsx` | `frontend/src/pages/blueprint_candidate_report.tsx` (missing in branch; closest prototype: `frontend/src/pages/blueprint_candidate_detail_sample.tsx`) | `/api/v2/surfaces/candidates/{candidate_id}/report` | Legacy blueprint candidate drawer and any `/api/platform/*` candidate-detail payloads |
| Daily Brief | Daily brief latest, run detail, replay, charts, and policy workflow behavior | `frontend/src/pages/daily_brief.tsx` | `/api/v2/surfaces/daily-brief` | `/api/daily-brief/reader/latest` and related legacy `/api/daily-brief/*` reader and workflow endpoints |
| Portfolio | Portfolio control, exposure, drift, uploads, and blueprint consequence behavior | `frontend/src/pages/portfolio.tsx` | `/api/v2/surfaces/portfolio` | Legacy `/api/platform/*` portfolio control, exposure, comparison, and upload-monitoring endpoints |

