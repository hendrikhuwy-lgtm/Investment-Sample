# V2 Domain Object Spec

| Object | Layer | Source truth origin | Translator path | Which surfaces use it |
| --- | --- | --- | --- | --- |
| InstrumentTruth | Layer 2 - Truth | Candidate registry plus `backend/app/services/blueprint_candidate_truth.py` issuer and factsheet fields | `backend/app/v2/translators/instrument_truth_translator.py` | Blueprint Explorer, Candidate Report |
| MarketSeriesTruth | Layer 2 - Truth | Market data and benchmark proxy series donors | `backend/app/v2/translators/market_series_truth_translator.py` | Daily Brief, Blueprint Explorer, Portfolio |
| BenchmarkTruth | Layer 2 - Truth | `backend/app/services/blueprint_benchmark_registry.py` plus benchmark history donors | `backend/app/v2/translators/benchmark_truth_translator.py` | Blueprint Explorer, Candidate Report |
| MacroTruth | Layer 2 - Truth | Macro and regime indicator donors | `backend/app/v2/translators/macro_truth_translator.py` | Daily Brief, Portfolio, Blueprint Explorer |
| PortfolioTruth | Layer 2 - Truth | Portfolio upload snapshots and holdings state donors | `backend/app/v2/translators/portfolio_truth_translator.py` | Portfolio, Candidate Report, Daily Brief |
| EvidencePack | Layer 2 - Truth | Candidate evidence citations and freshness rollups from truth translators | `backend/app/v2/translators/evidence_pack_translator.py` | Candidate Report, Daily Brief, Blueprint Explorer |
| SignalPacket | Layer 3 - Interpretation | Derived from MarketSeriesTruth, MacroTruth, BenchmarkTruth, and PortfolioTruth deltas | `backend/app/v2/core/interpretation_engine.py` | Daily Brief, Blueprint Explorer, Portfolio |
| InterpretationCard | Layer 3 - Interpretation | Rendered implication layer over SignalPacket | `backend/app/v2/core/interpretation_engine.py` | Daily Brief, Candidate Report |
| CandidateAssessment | Layer 3 - Interpretation | InstrumentTruth, BenchmarkTruth, EvidencePack, and policy inputs | `backend/app/v2/core/interpretation_engine.py` | Blueprint Explorer, Candidate Report, Portfolio |
| SleeveAssessment | Layer 3 - Interpretation | CandidateAssessment rollup by sleeve purpose and lead status | `backend/app/v2/core/interpretation_engine.py` | Blueprint Explorer, Portfolio |
| PortfolioPressure | Layer 3 - Interpretation | PortfolioTruth plus sleeve and mandate implications | `backend/app/v2/core/interpretation_engine.py` | Portfolio, Daily Brief |
| CompareAssessment | Layer 3 - Interpretation | Pairwise candidate comparison from CandidateAssessment and BenchmarkTruth | `backend/app/v2/core/interpretation_engine.py` | Candidate Report, Blueprint Explorer |
| PolicyBoundary | Layer 4 - Policy | Policy rubric donors and mandate rule translations | `backend/app/v2/core/mandate_rubric.py` | Candidate Report, Blueprint Explorer, Portfolio |
| FrameworkRestraint | Layer 4 - Policy | Framework lens translations and doctrine principle constraints | `backend/app/v2/translators/framework_restraint_translator.py` | Candidate Report, Daily Brief, Blueprint Explorer |
| VisibleDecisionState | Layer 4 - Policy | CandidateAssessment plus PolicyBoundary and doctrine output | `backend/app/v2/core/mandate_rubric.py` | Blueprint Explorer, Candidate Report, Daily Brief, Portfolio |
| ConstraintSummary | Layer 4 - Policy | PolicyBoundary and FrameworkRestraint aggregation | `backend/app/v2/core/mandate_rubric.py` | Candidate Report, Blueprint Explorer, Daily Brief |
| ChangeEvent | Layer 6 - Change | Change ledger entries derived from truth, interpretation, and visible-state diffs | `backend/app/v2/core/change_ledger.py` | Planned for all V2 surfaces via change summaries |
| DecisionDiff | Layer 6 - Change | Before and after decision-state snapshots | `backend/app/v2/core/change_ledger.py` | Planned for Candidate Report, Blueprint Explorer |
| TrustDiff | Layer 6 - Change | Freshness and source-trust deltas across rebuilds | `backend/app/v2/core/change_ledger.py` | Planned for Daily Brief, Candidate Report, Portfolio |
| SurfaceChangeSummary | Layer 6 - Change | Surface-level rollup over ChangeEvent entries | `backend/app/v2/core/change_ledger.py` | Planned for all V2 surfaces |

