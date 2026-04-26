import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(process.cwd(), "src");

function read(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), "utf8");
}

test("daily brief scenario copy reflects active forecast support rather than deterministic fallback", () => {
  const adapters = read("adapters.ts");
  assert.match(adapters, /Scenario blocks are backed by active forecast support in the live brief\./);
  assert.doesNotMatch(adapters, /deterministic fallback/);
});

test("candidate report empty scenario copy stays product-facing", () => {
  const app = read("App.tsx");
  assert.match(app, /Scenario blocks are not available for this report yet\./);
  assert.doesNotMatch(app, /Current V2 report contracts do not yet emit structured scenario blocks/);
});

test("blueprint explorer uses a horizontal staged workspace with compact rows and expanded detail", () => {
  const app = read("App.tsx");
  const adapters = read("adapters.ts");
  assert.match(app, /className="blueprint-sleeve-selector"/);
  assert.match(app, /className="blueprint-summary-stack"/);
  assert.match(app, /Total sleeves/);
  assert.match(app, /Total candidates/);
  assert.match(app, /Sleeve candidates/);
  assert.match(app, /className="blueprint-sleeve-card-target-band"/);
  assert.match(app, /className="blueprint-sleeve-card-footer"/);
  assert.match(app, /Strategic top-level sleeve/);
  assert.match(app, /Inside \$\{sleeve\.parentSleeveName/);
  assert.match(app, /className="blueprint-workspace-stack"/);
  assert.match(app, /className="blueprint-focus-strip"/);
  assert.match(app, /className="candidate-workspace-head"/);
  assert.match(app, /className="candidate-row-set candidate-row-set-horizontal"/);
  assert.match(app, /Why the leader matters/);
  assert.match(app, /Current posture/);
  assert.match(app, /What blocks action/);
  assert.match(app, /What reopens the view/);
  assert.match(app, /Target \{activeSleeve\.targetLabel\}/);
  assert.match(app, /Range \{activeSleeve\.rangeLabel\}/);
  assert.match(app, /Nested in \$\{activeSleeve\.parentSleeveName/);
  assert.doesNotMatch(app, /Compare readiness/);
  assert.match(app, /ETF facts/);
  assert.match(app, /Decision summary/);
  assert.match(app, /Evidence and integrity/);
  assert.match(app, /Score interpretation/);
  assert.match(app, /Portfolio role/);
  assert.match(adapters, /targetLabel: sleeve\.target_label/);
  assert.match(adapters, /rangeLabel: sleeve\.range_label/);
  assert.match(app, /candidate\.scoreComponents\.length/);
  assert.match(app, /candidate\.scoreBreakdown/);
  assert.doesNotMatch(app, /candidate\.researchSupportSummary/);
});

test("blueprint compare is fetched from the backend compare surface and shown through a centered compare modal", () => {
  const app = read("App.tsx");
  const api = read("api.ts");
  assert.match(app, /fetchCompare/);
  assert.match(app, /Open compare/);
  assert.match(app, /comparePanelOpen/);
  assert.match(app, /data-testid="blueprint-compare-modal"/);
  assert.match(app, /Cleaner for sleeve job/);
  assert.match(app, /compareDisplay\.compareSummary\.cleanerForSleeveJob/);
  assert.match(api, /\/blueprint\/compare\?/);
  assert.doesNotMatch(app, /Comparing \{compared\.length\} ETFs in/);
  assert.doesNotMatch(app, /Eligible state/);
});

test("candidate report keeps tracked implementation fields visible with explicit unresolved copy", () => {
  const adapters = read("adapters.ts");
  assert.match(adapters, /Not yet resolved/);
  assert.match(adapters, /Still missing from current implementation truth\./);
});

test("daily brief right rail is trigger-first rather than a second memo column", () => {
  const app = read("App.tsx");
  assert.match(app, /Current status/);
  assert.match(app, /What changes if confirmed/);
  assert.match(app, /What to watch next/);
  assert.match(app, /Affected sleeves/);
  assert.doesNotMatch(app, /Trigger condition/);
});

test("daily brief adapter keeps grouped supporting lines for deduped trigger cards", () => {
  const adapters = read("adapters.ts");
  assert.match(adapters, /whatChangesIfConfirmed/);
  assert.match(adapters, /supportingLines/);
});

test("adapters map bounded research support packs into intended research surfaces only", () => {
  const adapters = read("adapters.ts");
  assert.match(adapters, /function adaptResearchSupport/);
  assert.match(adapters, /memoryFoundationNote/);
  assert.doesNotMatch(adapters, /researchSupportSummary/);
});

test("portfolio renderer restores the richer portfolio operating layout", () => {
  const app = read("App.tsx");
  const adapters = read("adapters.ts");
  assert.match(app, /Holdings by weight/);
  assert.match(app, /Allocation vs Target/);
  assert.match(app, /portfolio-allocation-card-grid/);
  assert.match(app, /portfolio-allocation-lines/);
  assert.match(app, /Inside Global Equity Core/);
  assert.match(app, /Nested equity carveouts do not add to the top-level total\./);
  assert.match(app, /Range/);
  assert.match(adapters, /targetLabel: row\.target_label/);
  assert.match(adapters, /rangeLabel: row\.range_label/);
  assert.doesNotMatch(adapters, /target: "0\.0%"/);
  assert.match(app, /Holdings Explorer/);
  assert.match(app, /Portfolio Health/);
  assert.match(app, /Blueprint Relevance/);
  assert.match(app, /Daily Brief Connection/);
  assert.match(app, /Upload &amp; Sync/);
  assert.match(app, /type="file"/);
  assert.match(app, /accept="\.csv,text\/csv"/);
  assert.match(app, /Upload holdings/);
  assert.doesNotMatch(app, /Review mappings/);
  assert.doesNotMatch(app, /pf-section-title">What Matters Now/);
});

test("notebook and evidence surfaces render research support without touching daily brief copy", () => {
  const app = read("App.tsx");
  assert.match(app, /Persisted research memory support/);
  assert.match(app, /Retrieval and drift support/);
  assert.match(app, /Research support/);
});
