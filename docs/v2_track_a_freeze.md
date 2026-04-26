# V2 Track A Freeze

## Scope

Track A owns Layer 1 only:

- `backend/app/v2/sources/`
- `backend/app/v2/donors/`
- `backend/app/v2/translators/`
- source freshness policy and MCP connector rationalization

Track A does not own interpretation logic, product contracts, routes, frontend code, or Track B domain object behavior.

## Interface Freeze

The following interfaces are frozen for Milestone 1:

- `CandidateRegistryDonor`
- `BenchmarkRegistryDonor`
- `EtfSourceDonor`
- `ProviderSnapshotDonor`
- `PortfolioStateDonor`

The goal of the freeze is narrow: legacy service modules remain the donor side, and Track B should consume only translated V2 source records or the donor interfaces above. Track B should not import legacy service modules directly when a Layer 1 donor already exposes the required read path.

## Translation Boundary

Translator outputs are intentionally neutral:

- `SourceRecord`
- `TranslationResult`
- `TranslationIssue`

These are transport-layer records, not domain objects. They capture provenance, freshness, payload, and degradation state, but they do not perform interpretation or recommendation logic.

## Donor Rules

- Wrap existing donor modules; do not fork product logic into V2.
- Preserve upstream payload shape as much as practical.
- Add provenance and freshness at the translation layer.
- Prefer read-only adapters in Milestone 1.
- New connectors must be registered in `backend/app/v2/sources/registry.py` before they are used by higher layers.

## Milestone 1 Exit

Milestone 1 is considered frozen when:

- donor interfaces exist and concrete adapters compile
- the source registry is the single source of truth for Layer 1 source definitions
- MCP connectors in the current inventory are fully tiered
- freshness policy is documented and referenced from the registry

