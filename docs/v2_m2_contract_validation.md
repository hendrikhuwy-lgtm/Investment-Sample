# V2 M2 Contract Validation

## Blueprint Explorer Contract
- Fixture fields checked: `contract_version`, `generated_at`, `surface_id`, `freshness_state`, `holdings_overlay_present`, `sleeves`, `market_state_summary`, `review_posture`
- TypeScript interface coverage:
- `contract_version`: fail, `BlueprintExplorerContract` is not exported from `shared/v2_surface_contracts.ts`
- `generated_at`: fail, `BlueprintExplorerContract` is not exported from `shared/v2_surface_contracts.ts`
- `surface_id`: fail, `BlueprintExplorerContract` is not exported, and the only top-level surface discriminator in current contracts is `surface`, not `surface_id`
- `freshness_state`: fail, no matching top-level field exists for a Blueprint Explorer surface contract
- `holdings_overlay_present`: fail, `V2ContractBase` is not defined and the field is not present in the shared contracts
- `sleeves`: fail, no compatible legacy portfolio-overview sleeve shape should be reused for the Blueprint Explorer fixture (`sleeve_name`/`candidate_count`/`lead_candidate_id`/`lead_symbol`/`review_posture`)
- `market_state_summary`: fail, no matching top-level field exists for a Blueprint Explorer surface contract
- `review_posture`: fail, no matching top-level field exists for a Blueprint Explorer surface contract
- Mismatches found:
- Missing `BlueprintExplorerContract` export in `shared/v2_surface_contracts.ts`
- Missing `V2ContractBase` export in `shared/v2_surface_contracts.ts`
- Missing required base fields `surface_id`, `freshness_state`, and `holdings_overlay_present`
- Existing portfolio `sleeves` array is not compatible with the Blueprint Explorer sleeve row shape

## Candidate Report Contract
- Fixture fields checked: `contract_version`, `generated_at`, `surface_id`, `candidate_id`, `freshness_state`, `holdings_overlay_present`, `investment_case`, `current_implication`, `main_tradeoffs`, `baseline_comparisons`, `evidence_depth`, `mandate_boundary`
- TypeScript interface coverage:
- `contract_version`: fail, `CandidateReportContract` is not exported from `shared/v2_surface_contracts.ts`
- `generated_at`: fail, `CandidateReportContract` is not exported from `shared/v2_surface_contracts.ts`
- `surface_id`: fail, `CandidateReportContract` is not exported, and the current contracts use `surface`, not `surface_id`
- `candidate_id`: fail, no top-level candidate report contract exists; `candidate_id` only appears inside other nested structures
- `freshness_state`: fail, no matching top-level field exists for a Candidate Report surface contract
- `holdings_overlay_present`: fail, `V2ContractBase` is not defined and the field is not present in the shared contracts
- `investment_case`: fail, no matching top-level field exists for a Candidate Report surface contract
- `current_implication`: fail, no matching top-level field exists for a Candidate Report surface contract
- `main_tradeoffs`: fail, no matching top-level field exists for a Candidate Report surface contract
- `baseline_comparisons`: fail, no matching top-level field exists for a Candidate Report surface contract
- `evidence_depth`: fail, no matching top-level field exists for a Candidate Report surface contract
- `mandate_boundary`: fail, no matching top-level field exists for a Candidate Report surface contract
- Mismatches found:
- Missing `CandidateReportContract` export in `shared/v2_surface_contracts.ts`
- Missing `V2ContractBase` export in `shared/v2_surface_contracts.ts`
- Missing required base fields `surface_id`, `freshness_state`, and `holdings_overlay_present`
- Candidate Report top-level fields from the fixture are not represented in any exported surface contract

## Stable ID Usage
- `candidate_id`: found in `candidate_report_contract_sample.json`
- `sleeve_id`: found in `blueprint_explorer_contract_sample.json`

## Recommendation
Needs Track B fix:
- Export `V2ContractBase` with `contract_version`, `generated_at`, `surface_id`, `freshness_state`, and `holdings_overlay_present`
- Export `BlueprintExplorerContract` with top-level fields compatible with `blueprint_explorer_contract_sample.json`
- Export `CandidateReportContract` with top-level fields compatible with `candidate_report_contract_sample.json`
- Align the frontend stub to the final Track B exports once those contracts exist
