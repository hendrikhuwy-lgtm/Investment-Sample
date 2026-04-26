# Upstream Health Report

Summary: {"healthy": 11, "partial": 13, "structurally_under_covered": 0, "weak": 9}

## Registry Parity

Status: gap_detected

- BIL (cash_bills): warning | proxy benchmark requires stronger explicit limitation
- BILS (cash_bills): warning | proxy benchmark requires stronger explicit limitation
- SGD_CASH_RESERVE (cash_bills): info | missing source_config_registry, doc_registry
- SGD_MMF_POLICY (cash_bills): info | missing source_config_registry, doc_registry
- SGOV (cash_bills): warning | proxy benchmark requires stronger explicit limitation
- SG_TBILL_POLICY (cash_bills): info | missing source_config_registry, doc_registry
- UCITS_MMF_PLACEHOLDER (cash_bills): info | missing source_config_registry, doc_registry
- CAOS (convex): warning | missing benchmark_registry
- DBMF (convex): warning | missing benchmark_registry
- KMLM (convex): warning | missing doc_registry, benchmark_registry
- SPX_LONG_PUT (convex): info | missing source_config_registry, doc_registry, benchmark_registry
- TAIL (convex): warning | missing doc_registry, benchmark_registry

## Symbol Coverage

- CMOD [alternatives]: weak | missing | unknown | claim review_only | root cause missing_source_coverage | action downgrade_claims_or_escalate_later | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- IWDP [alternatives]: weak | missing | unknown | claim review_only | root cause missing_source_coverage | action downgrade_claims_or_escalate_later | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- SGLN [alternatives]: partial | html_summary | html_summary_backed | claim summary_limited | root cause fallback_masking | action monitor | why Issuer summary support is usable but still weaker than direct holdings truth.
- BIL [cash_bills]: partial | html_summary | html_summary_backed | claim summary_limited | root cause fallback_masking | action monitor | why Issuer summary support is usable but still weaker than direct holdings truth.
- BILS [cash_bills]: partial | html_summary | html_summary_backed | claim summary_limited | root cause fallback_masking | action monitor | why Issuer summary support is usable but still weaker than direct holdings truth.
- IB01 [cash_bills]: partial | direct_holdings | direct_holdings_backed | claim full_support | root cause missing_source_coverage | action downgrade_claims_or_escalate_later | why Direct holdings truth is present, but freshness or completeness still limits full authority.
- SGD_CASH_RESERVE [cash_bills]: weak | missing | unknown | claim review_only | root cause registry_incompleteness | action repair_by_engineering | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- SGD_MMF_POLICY [cash_bills]: weak | missing | unknown | claim review_only | root cause registry_incompleteness | action repair_by_engineering | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- SGOV [cash_bills]: healthy | direct_holdings | direct_holdings_backed | claim full_support | root cause missing_source_coverage | action preserve | why Direct holdings truth is present and fresh enough for primary authority.
- SG_TBILL_POLICY [cash_bills]: weak | missing | unknown | claim review_only | root cause registry_incompleteness | action repair_by_engineering | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- UCITS_MMF_PLACEHOLDER [cash_bills]: weak | missing | unknown | claim review_only | root cause registry_incompleteness | action repair_by_engineering | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- HMCH [china_satellite]: partial | factsheet_summary | issuer_structured_summary_backed | claim summary_limited | root cause fallback_masking | action monitor | why Issuer summary support is usable but still weaker than direct holdings truth.
- XCHA [china_satellite]: partial | html_summary | html_summary_backed | claim summary_limited | root cause fallback_masking | action monitor | why Issuer summary support is usable but still weaker than direct holdings truth.
- CAOS [convex]: healthy | factsheet_summary | structure_first | claim structure_first_limited | root cause missing_source_coverage | action preserve | why Structure-first product is being judged through strategy design rather than fake holdings completeness.
- DBMF [convex]: healthy | missing | structure_first | claim structure_first_limited | root cause missing_source_coverage | action preserve | why Structure-first product is being judged through strategy design rather than fake holdings completeness.
- KMLM [convex]: healthy | missing | structure_first | claim structure_first_limited | root cause registry_incompleteness | action preserve | why Structure-first product is being judged through strategy design rather than fake holdings completeness.
- SPX_LONG_PUT [convex]: weak | missing | unknown | claim review_only | root cause registry_incompleteness | action repair_by_engineering | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- TAIL [convex]: healthy | factsheet_summary | structure_first | claim structure_first_limited | root cause registry_incompleteness | action preserve | why Structure-first product is being judged through strategy design rather than fake holdings completeness.
- IWDA [developed_ex_us_optional]: healthy | direct_holdings | direct_holdings_backed | claim full_support | root cause missing_source_coverage | action preserve | why Direct holdings truth is present and fresh enough for primary authority.
- VEVE [developed_ex_us_optional]: partial | html_summary | html_summary_backed | claim summary_limited | root cause fallback_masking | action monitor | why Issuer summary support is usable but still weaker than direct holdings truth.
- EIMI [emerging_markets]: healthy | direct_holdings | direct_holdings_backed | claim full_support | root cause missing_source_coverage | action preserve | why Direct holdings truth is present and fresh enough for primary authority.
- VFEA [emerging_markets]: partial | factsheet_summary | unknown | claim review_only | root cause missing_source_coverage | action downgrade_claims_or_escalate_later | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- CSPX [global_equity_core]: healthy | direct_holdings | direct_holdings_backed | claim full_support | root cause missing_source_coverage | action preserve | why Direct holdings truth is present and fresh enough for primary authority.
- IWDA [global_equity_core]: healthy | direct_holdings | direct_holdings_backed | claim full_support | root cause missing_source_coverage | action preserve | why Direct holdings truth is present and fresh enough for primary authority.
- SSAC [global_equity_core]: healthy | direct_holdings | direct_holdings_backed | claim full_support | root cause missing_source_coverage | action preserve | why Direct holdings truth is present and fresh enough for primary authority.
- VWRA [global_equity_core]: partial | direct_holdings | direct_holdings_backed | claim full_support | root cause missing_source_coverage | action downgrade_claims_or_escalate_later | why Direct holdings truth is present, but freshness or completeness still limits full authority.
- VWRL [global_equity_core]: partial | factsheet_summary | unknown | claim review_only | root cause missing_source_coverage | action downgrade_claims_or_escalate_later | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- A35 [ig_bonds]: partial | html_summary | html_summary_backed | claim summary_limited | root cause fallback_masking | action monitor | why Issuer summary support is usable but still weaker than direct holdings truth.
- AGGU [ig_bonds]: healthy | direct_holdings | direct_holdings_backed | claim full_support | root cause missing_source_coverage | action preserve | why Direct holdings truth is present and fresh enough for primary authority.
- VAGU [ig_bonds]: partial | html_summary | html_summary_backed | claim summary_limited | root cause fallback_masking | action monitor | why Issuer summary support is usable but still weaker than direct holdings truth.
- CMOD [real_assets]: weak | missing | unknown | claim review_only | root cause missing_source_coverage | action downgrade_claims_or_escalate_later | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- IWDP [real_assets]: weak | missing | unknown | claim review_only | root cause missing_source_coverage | action downgrade_claims_or_escalate_later | why Truth support remains incomplete and needs engineering repair or explicit downgrade.
- SGLN [real_assets]: partial | html_summary | html_summary_backed | claim summary_limited | root cause fallback_masking | action monitor | why Issuer summary support is usable but still weaker than direct holdings truth.