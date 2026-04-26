# ARCHITECTURE REDUCTION AUDIT
**Investment-Agent Portfolio Operating System**
**Date:** 2026-03-13
**Audit Type:** Forensic End-to-End Flow Analysis
**Verdict:** System is well-architected; reduction opportunities limited

---

## SECTION 1: EXECUTIVE DIAGNOSIS

### 1.1 Why the System Still Behaves Like Older Versions

**FINDING**: The system does NOT behave like older versions in its core architecture. **The perception of similarity is a presentation layer issue, not an architectural one.**

**Evidence**:
1. **Blueprint** has undergone complete pipeline rebuild:
   - 7-stage deterministic flow (evidence → gates → scoring → ranking → recommendation → memo)
   - Clean truth ownership (field_truth → completeness → eligibility → quality → recommendation)
   - No duplicated decision logic
   - Policy gates are authoritative and well-separated

2. **Daily Brief** has structured fact-pack-led explanation:
   - Fact compilation → grounding evaluation → explanation generation → validation → rendering
   - Holdings authority properly tiered (live → stale → proxy)
   - Citation discipline enforced
   - Dual-path explanation (template + LLM) with fallback

**Why it FEELS old**:
- **User-facing labels haven't changed**: "eligible_with_caution", "best_in_class", "recommendation_ready" are the same vocabulary as before
- **Email sections are still 13+**: Daily Brief structure unchanged despite backend improvements
- **Explanation fields still 16**: No consolidation happened
- **Drawer UI still shows machine states**: eligibility_state + recommendation_state + badge + user_facing_state all visible (even though they're now properly derived)

**Root cause**: Backend was modernized, frontend presentation was not.

---

### 1.2 Root Problem Classification

**Architectural**: ❌ No significant architectural problems found
**Object Ownership**: ✅ EXCELLENT - clear ownership, no conflicts
**Presentation**: ⚠️ **THIS IS THE ACTUAL PROBLEM**

**What works**:
- Truth resolution is provenance-based and deterministic
- Policy gates are properly separated from scoring
- Eligibility derives from blockers/cautions correctly
- Recommendation logic is multi-factor and explicit
- Grounding modes prevent false holdings claims
- Citation discipline is enforced

**What needs work**:
- Too many state labels shown to user (9 overlapping concepts)
- Explanation schema has 16 fields when 10-12 would suffice
- Email has 13+ sections when 5-7 would be clearer
- Candidate drawer shows eligibility → scoring → ranking → badge as separate when they should be synthesized into ONE decision view
- Pressure enumeration duplicates blockers (adds label complexity without new information)

---

### 1.3 Highest Leverage Reduction Opportunities

**OPPORTUNITY 1**: **Consolidate User-Facing State Labels** (Blueprint)
**Impact**: HIGH | **Effort**: MEDIUM | **Type**: Presentation simplification

Current: 9 overlapping state concepts visible in UI
- eligibility_state
- recommendation_state
- badge
- readiness_level
- decision_state
- user_facing_state
- policy_gate_state
- data_quality_state
- scoring_state

Target: 3 authoritative states
- Readiness tier (research → review → shortlist → recommendation)
- Policy fit status (clean → limited → blocked)
- Recommendation role (primary → backup → watchlist → research)

All other states are **derived** and should be internal only.

**Why this matters**: User sees "eligible_with_caution" + "watchlist_only" + "acceptable" + "review_ready" which are all saying the same thing differently.

---

**OPPORTUNITY 2**: **Remove Pressure Enumeration** (Blueprint)
**Impact**: MEDIUM | **Effort**: LOW | **Type**: Architectural cleanup

Pressures are built FROM blockers/cautions:
```python
if blockers or cautions:
    add_pressure("readiness", evidence=blockers[:3] or cautions[:3])
```

This creates dual representation:
- eligibility_blockers[] (authoritative)
- pressures[] (reformatted blockers)

**Action**: Delete `_classify_pressures()` function, enhance blocker categorization directly.

**Lines saved**: ~150
**Dependencies to update**: 10 files reference pressures

---

**OPPORTUNITY 3**: **Consolidate Explanation Fields** (Daily Brief)
**Impact**: MEDIUM | **Effort**: MEDIUM | **Type**: Schema simplification

Current: 16 explanation fields per signal
Target: 11-12 fields

Merge:
- `analyst_synthesis` + `system_relevance` → single `portfolio_context` field
- `strengthen_read` + `weaken_read` → single `monitoring_conditions` field
- Keep scenarios separate (genuinely distinct branching logic)

**Why this matters**: LLM prompts have to generate 16 distinct fields, validation checks 16 keys, template has 16 extraction rules. Simplification reduces complexity across ALL layers.

---

**OPPORTUNITY 4**: **Simplify Email Structure** (Daily Brief)
**Impact**: HIGH (user experience) | **Effort**: HIGH | **Type**: Product redesign

Current: 13+ sections in email
Target: 5-7 core sections

Consolidate:
1. **Market Snapshot**: What changed + Why it matters (merge Executive + What Changed)
2. **Portfolio Impact**: Sleeve relevance + Actions (merge Portfolio Mapping + Review Actions)
3. **Policy Context**: Expected returns, benchmarks, scenarios (keep as-is)
4. **Monitoring**: Alerts + Opportunities + Data graphs (merge 3 sections)
5. **Evidence**: Sources + Citations (merge appendix sections)

Optional (for internal/PM mode):
6. Implementation guidance (fund selection)
7. Diagnostics (freshness, MCP status)

**Why this matters**: 13 sections create "framework exposition" feel instead of "market interpretation note" feel.

---

**OPPORTUNITY 5**: **Remove Schema Generators** (Both)
**Impact**: LOW | **Effort**: LOW | **Type**: Code cleanup

Files:
- `daily_brief_explanation_schema.py` (~100 lines)
- `blueprint_candidate_explanation_schema.py` (~80 lines)

**Finding**: These extract dynamic context from fact_packs (affected sleeves, metrics, grounding modes), they're NOT static constants as initially assumed.

**Action**: KEEP but rename to `*_schema_builder.py` for clarity. Not reduction candidates.

---

**OPPORTUNITY 6**: **Demote Fact Packs to Diagnostics** (Both)
**Impact**: LOW | **Effort**: LOW | **Type**: Role clarification

Fact packs serve two purposes:
1. **Primary**: Compilation of evidence for explanation generation
2. **Secondary**: Validation constraint definition (allowed dates, numbers, sleeve mappings)

**Current issue**: Fact packs are used for BOTH, creating confusion about their authority.

**Action**: Keep fact packs but document clearly:
- Primary use: Explanation input
- Diagnostic use: Validation constraints, operator review
- NOT: Long-term storage (regenerated per request)

---

**OPPORTUNITY 7**: **Merge Evidence Renderers** (Daily Brief)
**Impact**: LOW | **Effort**: LOW | **Type**: Code consolidation

**COMPLETED**: Already merged `daily_brief_evidence_builder.py` + `daily_brief_footnote_builder.py` → `daily_brief_evidence_renderer.py`

**Lines saved**: ~30

---

### 1.4 What This Audit Did NOT Find

**NOT FOUND**: Duplicated truth ownership
**NOT FOUND**: Competing decision objects
**NOT FOUND**: Misplaced scoring logic
**NOT FOUND**: Overlapping gate evaluation
**NOT FOUND**: Inconsistent state derivation

**Conclusion**: The backend architecture is **well-designed**. The problem is **presentation complexity**, not architectural decay.

---

## SECTION 2: CURRENT PRODUCT REALITY VS INTENDED PRODUCT

### 2.1 Blueprint Reality

**What it actually behaves like today**:
An **institutionally rigorous candidate evaluation engine** that processes instruments through 7 stages (evidence → gates → eligibility → scoring → ranking → recommendation → memo), maintains full audit trails, enforces policy constraints, and produces detailed comparison memos.

**What it was intended to be**:
A **policy-governed candidate selection product** answering: what this candidate is, whether it fits mandate, why it's good/weak, how it compares to peers, what blocks conviction, and whether it's clean/limited/blocked.

**The gap**:
- ✅ **Product goal achieved** in backend logic
- ❌ **Presentation layer shows machine states** instead of synthesized investment judgment
- ❌ **User sees 9 state labels** instead of 1 coherent decision statement
- ❌ **Drawer feels like admin panel** because it enumerates: eligibility_state + badge + recommendation_state + readiness_level + policy_gate_state + data_quality_state + scoring_state + user_facing_state + pressures
- ✅ **Memo generation exists** but often gets buried under state labels

**Fix**: Hide internal states, show ONE decision view with explanation.

---

### 2.2 Daily Brief Reality

**What it actually behaves like today**:
A **comprehensive market intelligence report** that ingests 11+ FRED series, MCP feeds, web sources, and portfolio holdings; detects regime shifts using threshold rules; generates fact-packed signal cards with grounding modes; produces 16-field explanations (deterministic or LLM); validates citations/freshness; and renders a 13-section institutional email with policy guidance.

**What it was intended to be**:
A **market interpretation note** answering: what changed, why it matters economically, which sleeves affected, what deserves review now, what stays background.

**The gap**:
- ✅ **Signal detection is rigorous** (threshold-based, percentile-aware, regime-shift sensitive)
- ✅ **Grounding modes prevent false claims** (live holdings vs proxy clearly distinguished)
- ✅ **Citation discipline enforced** (no uncited claims)
- ❌ **13+ sections create framework exposition feel** instead of interpretation note
- ❌ **16 explanation fields per signal** when 10-12 would suffice
- ❌ **Policy pack, implementation layer, diagnostics all in same email** when they should be separate artifacts
- ✅ **LLM explanation path works** but validation fallback suggests it's not fully trusted

**Fix**: Simplify email structure (13 → 5-7 sections), consolidate explanation fields (16 → 11), separate policy/diagnostics into appendices.

---

## SECTION 3: END-TO-END FLOW SUMMARY

### 3.1 Blueprint Flow (7 Stages)

```
1. Candidate Registration & Universe Management
   → blueprint_candidate_registry.py
   → Creates canonical_instruments + sleeve_candidate_memberships tables
   → Seed data or live refresh

2. Field Truth Resolution
   → blueprint_candidate_truth.py
   → Provenance-ranked observation selection
   → Resolves to candidate_field_current (authoritative)
   → Computes completeness → readiness_level

3. Evidence Pack Assembly + Source Integrity
   → blueprint_pipeline.py
   → Builds evidence_pack (core_identity, portfolio_character, implementation, structure)
   → Evaluates source_integrity_result (fresh/stale/incomplete/proxy-based)

4. Policy Gate Evaluation (10 gates)
   → blueprint_decision_semantics.py
   → Evaluates: sleeve_fit, structural_eligibility, exposure_integrity, liquidity, cost, tax, benchmark, bounded_loss, portfolio_usefulness, governance
   → Output: gate_result with decisive failures + reopen conditions

5. Eligibility Evaluation
   → blueprint_candidate_eligibility.py
   → Synthesizes blockers + cautions from gates, sources, risk controls
   → Derives: eligibility_state {eligible, eligible_with_caution, data_incomplete, ineligible}
   → Builds pressures[] (currently redundant with blockers)

6. Scoring (V1/V2 Models) + Ranking
   → blueprint_investment_quality.py → composite_score
   → blueprint_recommendations.py → rank_in_sleeve + badge assignment
   → Derives: recommendation_state, user_facing_state

7. Recommendation Decision + Memo Generation
   → blueprint_pipeline.py → recommendation_result (decision_type, lead_strength, practical_edge)
   → blueprint_thesis.py → investment_thesis
   → blueprint_candidate_explanation_writer.py → detail_explanation (4 sections)
```

**Truth ownership**: Clear and singular at each stage. No conflicts found.

---

### 3.2 Daily Brief Flow (5 Stages)

```
1. Data Ingestion
   → FRED series (real_email_brief.py)
   → MCP data (ingest_mcp.py)
   → Volume indicators
   → Web sources (Oaktree, Taleb)
   → Portfolio holdings (portfolio_state.py)

2. Signal Detection + Grounding
   → signals.py → extended_market_signals() applies threshold rules
   → brief_grounding_state.py → evaluate_brief_grounding() determines holdings authority
   → brief_holdings_mapper.py → map_signal_to_holdings()

3. Fact Pack Compilation
   → daily_brief_fact_pack.py → build_signal_fact_pack()
   → Compiles: signal_summary_facts, implication_facts, boundary_facts, review_action_facts, scenario_facts, strengthen/weaken_facts
   → Attaches: grounding_mode, evidence_classification, freshness_state

4. Explanation Generation (Dual Path)
   → daily_brief_explanation_formatter.py (deterministic template)
   OR
   → daily_brief_explanation_writer.py (LLM with validation + fallback)
   → Output: 16 explanation fields
   → Validation: daily_brief_explanation_validator.py (10+ constraints)

5. Email Assembly + Delivery
   → reporting.py → build_narrated_email_brief() (13+ sections)
   → brief_delivery_state.py → can_send_brief() (approval, citations, freshness gates)
   → SMTP send or file output
```

**Truth ownership**: FRED series, MCP data, portfolio holdings all have clear canonical stores. Grounding mode is deterministic. Fact packs are not persisted (regenerated per run). Explanations are stateless (template or LLM, no caching).

---

## SECTION 4: CANONICAL TRUTH ASSESSMENT

### 4.1 Meanings with Clear Owner (Blueprint)

| Meaning | Canonical Owner | Authority Type |
|---------|-----------------|----------------|
| Field truth (expense_ratio, holdings_count, etc.) | `candidate_field_current` table | Multi-source provenance resolution |
| Readiness tier | `candidate_completeness_snapshots.readiness_level` | Evidence completeness count |
| Policy gate status | `decision_record.policy_gates[]` on candidate | Gate evaluation rules |
| Composite score | `investment_quality.composite_score` | V1/V2 scoring model |
| Rank in sleeve | `investment_quality.rank_in_sleeve` | Deterministic sort (eligibility + score) |
| Badge | `investment_quality.badge` | Deterministic mapping from user_facing_state |
| Decision type | `recommendation_result.decision_type` | Logic: ADD/REPLACE/HOLD/REJECT/RESEARCH |
| Investment thesis | `blueprint_thesis.py` output | Multi-factor synthesis |

**Assessment**: ✅ EXCELLENT - no ownership conflicts

---

### 4.2 Meanings with Clear Owner (Daily Brief)

| Meaning | Canonical Owner | Authority Type |
|---------|-----------------|----------------|
| FRED series data | `fred_series_cache.json` + `metric_snapshots` table | FRED API (official source) |
| Signal threshold | `SIGNAL_THRESHOLD_REGISTRY` in signals.py | Configuration (DB override possible) |
| Grounding mode | `brief_grounding_state.evaluate_brief_grounding()` result | Tiered authority (live → stale → proxy) |
| Portfolio holdings | `portfolio_snapshot_rows` table | Latest user upload |
| Fact pack | `build_signal_fact_pack()` output | Evidence compilation (not persisted) |
| Explanation fields | Template or LLM output | Stateless generation |
| Email sections | `build_narrated_email_brief()` output | Rendering function (idempotent) |

**Assessment**: ✅ EXCELLENT - no ownership conflicts

---

### 4.3 Meanings with Multiple Competing Owners

**BLUEPRINT**:

❌ **Candidate usability status**:
- `eligibility_state` says: eligible, eligible_with_caution, data_incomplete, ineligible
- `recommendation_state` says: recommended_primary, recommended_backup, watchlist_only, research_only, rejected_*
- `badge` says: best_in_class, recommended, acceptable, caution, not_ranked
- `user_facing_state` says: fully_clean_recommendable, best_available_with_limits, research_ready_but_not_recommendable, blocked_by_*
- `readiness_level` says: recommendation_ready, shortlist_ready, review_ready, research_visible

**Problem**: These are NOT independent states - they're all DERIVED from the same upstream factors. But presenting all 5 to user creates confusion.

**Resolution**: Keep all as internal computation, show ONLY `user_facing_state` to user (or derive a simpler single label).

---

**DAILY BRIEF**:

⚠️ **Signal importance/urgency**:
- `signal_role` says: dominant, supporting, contextual, background
- `signal_state` says: alert, watch, normal
- `long_state` says: normal, watch, elevated, stress_emerging, stress_regime
- `short_state` says: (same set)
- `percentile_5y`, `percentile_10y`, `percentile_60d` all give different context

**Problem**: Multiple "importance" signals with overlapping meanings.

**Resolution**: These are actually ORTHOGONAL (role = what type of signal, state = threshold breach, percentile = historical context). Not a duplication, just multiple dimensions. **No fix needed** - clarify in documentation.

---

### 4.4 Meanings with No Clear Owner

**BLUEPRINT**: ❌ None found - everything has clear ownership

**DAILY BRIEF**:

⚠️ **Economic interpretation of regime shift**:
- Regime shifts are detected (threshold breach)
- Percentiles are computed (quantitative positioning)
- But "what this means for portfolio" is:
  - Sometimes in fact-pack `implication_facts`
  - Sometimes in LLM-generated `investment_implication` field
  - Sometimes in `system_relevance` field
  - No single authoritative "economic meaning" object

**Resolution**: This is by design - explanation is generated, not resolved from canonical source. **No fix needed** unless economic interpretation should be structured (e.g., regime → standard implication mapping).

---

## SECTION 5: DUPLICATION FINDINGS

### 5.1 Most Important Duplicated Meanings

**DUPLICATION 1: Pressure Objects Duplicate Blockers** (Blueprint)

**Severity**: MEDIUM
**User-visible effect**: Drawer shows both blockers list AND pressures list
**Evidence**:
```python
# In blueprint_candidate_eligibility.py:
if blockers or cautions:
    add_pressure("readiness", evidence=blockers[:3] or cautions[:3])
```

Pressures are built FROM blockers, not independently evaluated.

**Recommendation**: Delete pressure enumeration, enhance blocker categorization with `root_cause` + `severity` + `actionability` fields.

---

**DUPLICATION 2: State Label Proliferation** (Blueprint)

**Severity**: HIGH
**User-visible effect**: Candidate drawer shows 5+ overlapping state labels
**Evidence**:
- eligibility_state derives from blockers
- recommendation_state derives from eligibility + score + rank
- badge derives from recommendation_state
- user_facing_state derives from recommendation_state + gates + readiness
- All are deterministic mappings, not independent evaluations

**Recommendation**: Show ONLY `user_facing_state` (or a further simplified label) to user. Keep others as internal computation artifacts.

---

**DUPLICATION 3: Explanation Field Redundancy** (Daily Brief)

**Severity**: MEDIUM
**User-visible effect**: LLM generates similar content for `analyst_synthesis` and `system_relevance`
**Evidence**: Both fields explain "why it matters" - first from market perspective, second from portfolio perspective. When holdings unavailable, both often say similar things.

**Recommendation**: Merge into single `portfolio_context` field with structured sub-parts (market_context + portfolio_tie_in).

---

**DUPLICATION 4: Series Metadata in Multiple Locations** (Daily Brief)

**Severity**: LOW
**User-visible effect**: None (internal only)
**Evidence**:
- FRED series appear in: `fred_series_cache.json`, `graph_rows[]`, `metric_snapshots` table, `Citation` objects
- Same data (value, date, lag) stored 4x

**Recommendation**: KEEP - each serves different purpose (cache for refresh, graph_rows for rendering, metric_snapshots for history, Citations for provenance). Not harmful duplication.

---

## SECTION 6: SECTION OVERLAP FINDINGS

### 6.1 Blueprint Candidate Drawer Sections

**Current sections** (typical drawer):
1. Header (symbol, name, badge, rank)
2. Status block (eligibility_state, recommendation_state, user_facing_state)
3. Scoring block (composite_score, component scores, data_confidence)
4. Gate status (overall + decisive failures)
5. Comparison block (vs winner, vs baseline, vs current holding)
6. Evidence summary (source_integrity, readiness, missing fields)
7. Pressures block (primary/secondary pressure enumeration)
8. Memo/Explanation (investment_case, current_standing, tradeoffs, decision_change)
9. Technical appendix (gate details, score provenance, audit trace)

**Overlap findings**:

❌ **Sections 2 + 3 + 6 all describe "usability"**:
- Section 2: eligibility_state
- Section 3: data_confidence
- Section 6: readiness_level, critical_missing_fields

These should be MERGED into single "Evidence Status" section showing:
- Readiness tier (research/review/shortlist/recommendation)
- Critical missing (if any)
- Data confidence (high/medium/low)

❌ **Sections 7 + 8 both explain "what's wrong"**:
- Section 7: Pressures (reformatted blockers)
- Section 8: Tradeoffs (narrative blockers)

Should be MERGED - delete pressure section, enhance tradeoffs narrative.

✅ **Sections 5 + 9 are genuinely distinct**:
- Comparison = peer analysis
- Technical appendix = audit trail

**Recommendation**: Consolidate 9 sections → 6 sections:
1. Header
2. Decision (recommendation_state + decision_type + lead_strength)
3. Investment Case (from memo)
4. Peer Comparison (vs winner/baseline/current)
5. Evidence Status (readiness + missing + confidence)
6. Technical Detail (gates + scores + audit)

---

### 6.2 Daily Brief Email Sections

**Current sections**:
1. Top Sheet (metadata)
2. Executive Snapshot (policy + monitoring)
3. What Changed
4. MCP Updates
5. Policy Pack Layer (expected returns, benchmarks, scenarios)
6. Execution Layer (IPS, fund selection)
7. Data Graphs
8. Data Recency Summary
9. Long Horizon Context
10. Multi-Perspective Interpretation (4 lenses)
11. Alerts Timeline
12. Opportunities
13. Big Players Activity
14. Portfolio Mapping
15. Convex Compliance
16. Implementation Products
17. Source Appendix
18. Sources (footnotes)

**Overlap findings**:

❌ **Sections 2 + 3 both answer "what changed"**:
- Executive Snapshot has "monitoring now" (5 items)
- What Changed has regime delta

Should MERGE into single "Market Snapshot" section.

❌ **Sections 7 + 8 + 9 all explain data context**:
- Data Graphs = metric tables
- Data Recency = freshness summary
- Long Horizon Context = percentile interpretation

Should MERGE into single "Evidence & Context" section.

❌ **Sections 11 + 12 + 13 all list observations**:
- Alerts = regime shifts
- Opportunities = positive signals
- Big Players = institutional activity

Should MERGE into single "Monitoring Dashboard" section.

❌ **Sections 5 + 6 + 14 + 15 + 16 all cover portfolio guidance**:
- Policy Pack = long-term allocation
- Execution Layer = implementation
- Portfolio Mapping = current drift
- Convex Compliance = sleeve status
- Implementation Products = fund candidates

Should MERGE into single "Portfolio Guidance" section with sub-sections.

**Recommendation**: Consolidate 18 sections → 7 sections:
1. Market Snapshot (what changed + why + urgency)
2. Portfolio Impact (sleeve relevance + drift + actions)
3. Policy Context (expected returns + scenarios)
4. Monitoring Dashboard (alerts + opportunities + big players)
5. Evidence & Context (data + freshness + percentile interpretation)
6. Implementation Guidance (fund selection + tax + convex)
7. Sources & Appendix

---

## SECTION 7: REDUCTION CANDIDATES

See separate file: `REDUCTION_CANDIDATES.md` for detailed prioritized list.

**Summary by classification**:

**KEEP** (21 components):
- All field truth resolution
- All policy gate evaluation
- All scoring models
- All grounding state evaluation
- All fact pack compilation
- All citation discipline
- All approval workflows

**KEEP_AND_SIMPLIFY** (4 components):
- Email structure (13 → 7 sections)
- Explanation schema (16 → 11 fields)
- Candidate drawer (9 → 6 sections)
- State label presentation (9 → 1-3 labels)

**COLLAPSE** (3 components):
- Evidence + footnote builders (DONE)
- Comparison modules (NOT duplicates - serve different purposes)
- Schema builders (NOT static - extract dynamic context)

**RETIRE** (2 components):
- Pressure enumeration (duplicates blockers)
- Deterministic explanation formatter (if LLM-committed)

**REPLACE** (0 components):
- No fundamentally wrong logic found

---

## SECTION 8: MINIMUM VIABLE REDUCTION PLAN

### Phase 1: Safe Deletions (Week 1)

**1.1 Delete Pressure Enumeration**
- File: `blueprint_candidate_eligibility.py` - `_classify_pressures()` function
- Lines saved: ~150
- Dependencies: 10 files (need coordinated update)
- Risk: MEDIUM (widespread usage)
- Benefit: Removes dual representation of blockers

**1.2 Mark Deterministic Formatter as Fallback-Only**
- Files: `daily_brief_explanation_formatter.py`, `blueprint_candidate_explanation_formatter.py`
- Lines saved: 0 (role change, not deletion)
- Risk: LOW
- Benefit: Clarifies that LLM is primary path

---

### Phase 2: Presentation Simplifications (Weeks 2-3)

**2.1 Hide Internal State Labels from UI**
- Change: Candidate drawer shows ONLY `user_facing_state` or synthesized decision label
- Remove from UI: eligibility_state, recommendation_state, badge (internal only), readiness_level (internal only), policy_gate_state, data_quality_state, scoring_state
- Risk: LOW (backend unchanged)
- Benefit: User sees ONE decision statement instead of 9 labels

**2.2 Consolidate Email Sections**
- Target: 18 sections → 7 core sections (as detailed in 6.2)
- Risk: MEDIUM (user experience change)
- Benefit: "Market interpretation note" feel instead of "framework exposition"

**2.3 Reduce Explanation Fields**
- Target: 16 fields → 11 fields (merge analyst_synthesis + system_relevance, strengthen + weaken)
- Risk: LOW (schema change, not logic change)
- Benefit: Simpler LLM prompts, faster generation, easier validation

---

### Phase 3: Structural Cleanups (Week 4)

**3.1 Enhance Blocker Categorization**
- Add to blockers: `root_cause`, `severity`, `actionability`, `fix_path`
- Remove: Pressure objects entirely
- Risk: LOW (enrichment, not breaking change)
- Benefit: Richer context without dual representation

**3.2 Consolidate Candidate Drawer Sections**
- Target: 9 sections → 6 sections (as detailed in 6.1)
- Risk: LOW (presentation only)
- Benefit: Clearer information hierarchy

---

**Total reduction**: ~150-200 lines of code, 5-7 presentation simplifications

**NOT included** (requires product redesign, not reduction):
- Email structure redesign (needs user acceptance testing)
- Dual explanation path unification (needs LLM reliability commitment)
- V1/V2 scoring model merge (needs weight redistribution validation)

---

## SECTION 9: RISKS AND DEPENDENCIES

### 9.1 Safe Reductions (Low Risk)

✅ Hiding internal state labels from UI
✅ Enhancing blocker categorization
✅ Marking deterministic formatter as fallback-only
✅ Consolidating drawer sections (presentation only)

**Why safe**: Backend logic unchanged, only UI presentation affected.

---

### 9.2 Moderate Risk Reductions

⚠️ Deleting pressure enumeration
⚠️ Consolidating explanation fields
⚠️ Merging email sections

**Why moderate**:
- Pressure deletion has 10 file dependencies
- Explanation field changes affect LLM prompts + validation + templates
- Email section changes affect user experience

**Mitigation**: Phased rollout, A/B testing, rollback plan.

---

### 9.3 High Risk Changes (NOT Recommended for Reduction Pass)

❌ Deleting schema generators (they're NOT static - extract dynamic context)
❌ Merging comparison modules (they serve different purposes - portfolio vs peer)
❌ Removing dual explanation paths (reliability mechanism, not duplication)
❌ Unifying V1/V2 scoring models (scoring logic change, not reduction)
❌ Deleting fact packs (core compilation layer, not just validation)

**Why high risk**: These are NOT duplicates or legacy scaffolding - they serve distinct purposes.

---

### 9.4 Dependencies

**Pressure Enumeration Deletion**:
- Affects: portfolio_blueprint.py, blueprint_recommendations.py, blueprint_thesis.py, blueprint_deliverable_candidates.py, blueprint_candidate_universe.py, blueprint_rejection_memo.py, tests
- Requires: Coordinated update to use blockers[] instead of pressures[]

**Explanation Field Consolidation**:
- Affects: LLM prompts, validation rules, template extraction, schema definition
- Requires: Schema version bump, prompt retuning, validation rule updates

**Email Section Merge**:
- Affects: reporting.py rendering logic, email template structure
- Requires: User acceptance testing, rollback capability

---

## CONCLUSION: WHAT TO DO NEXT

### The Honest Verdict

**This system is NOT suffering from architectural decay.** It's suffering from **presentation complexity**.

The backend is well-designed:
- Clear truth ownership
- No duplicated decision logic
- Proper separation of concerns
- Deterministic state derivation
- Audit trail maintained

The frontend shows too much:
- 9 state labels when 1-3 would suffice
- 18 email sections when 7 would be clearer
- Machine states exposed when investment judgment should be synthesized

---

### Recommended Action Plan

**DO THIS** (High leverage, low risk):
1. Hide internal state labels from Blueprint drawer (show 1-3 instead of 9)
2. Consolidate drawer sections (9 → 6)
3. Enhance blocker categorization, prepare to delete pressures
4. Simplify explanation schema (16 → 11 fields)

**CONSIDER THIS** (Medium leverage, medium risk):
1. Delete pressure enumeration (coordinate across 10 files)
2. Consolidate email sections (18 → 7)
3. Merge explanation field pairs (analyst_synthesis + system_relevance, etc.)

**DON'T DO THIS** (Assumed duplicates that aren't):
1. Delete schema generators (they extract dynamic context)
2. Merge comparison modules (different purposes)
3. Delete fact packs (core compilation layer)
4. Remove dual explanation paths (reliability mechanism)

---

### Final Assessment

**Lines of code to remove**: ~150-200
**Complexity reduction**: Moderate (fewer user-facing labels, clearer sections)
**Architectural improvement**: Minimal (architecture is already good)
**User experience improvement**: HIGH (simpler presentation)

**The real opportunity isn't code reduction - it's presentation simplification.**

Focus on what the user sees, not how much code exists.

---

*Audit completed 2026-03-13*
