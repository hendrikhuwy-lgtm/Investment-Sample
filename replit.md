# Investment Operating System

## Overview

A personalized investment operating system with two core layers: Daily Brief (macro, market, and policy intelligence) and Blueprint (portfolio design and candidate recommendations).

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM (provisioned but not yet used — data is currently hardcoded in routes)
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Frontend**: React + Vite + Tailwind CSS
- **State**: TanStack React Query (via generated hooks)
- **Routing**: Wouter
- **UI**: Radix UI primitives, shadcn-style components
- **Animations**: Framer Motion

## Architecture

```text
artifacts/
├── api-server/        # Express 5 API server
│   └── src/routes/
│       ├── health.ts  # GET /api/healthz
│       ├── brief.ts   # GET /api/brief/today
│       └── blueprint.ts # GET /api/blueprint
├── investment-os/     # React + Vite frontend (served at /)
│   └── src/
│       ├── pages/
│       │   ├── DailyBrief.tsx  # Main brief view (/)
│       │   └── Blueprint.tsx   # Portfolio view (/blueprint)
│       └── components/
│           ├── NavBar.tsx
│           ├── FreshnessBadge.tsx
│           ├── MacroSignalCard.tsx
│           ├── CandidateCard.tsx
│           └── EvidencePanel.tsx
lib/
├── api-spec/openapi.yaml  # OpenAPI contract (source of truth)
├── api-client-react/      # Generated React Query hooks
└── api-zod/               # Generated Zod schemas
```

## Key Design Rules (from spec)

### Daily Brief Layer
- Must explain: what happened, what it means, investment implication, boundary, review action
- Must separate explanation from evidence
- Must use explicit dated evidence with source
- Must distinguish: current | latest_available | lagged | refresh_failed
- Evidence-first, citation-safe, freshness-honest

### Blueprint Layer
- Candidates are thesis-first, not evaluation-first
- Candidate detail order: current status → thesis → investment case → why ahead/behind → tradeoffs → change conditions → supporting detail
- Readiness is a compact status badge, not the main narrative

## Data Model (OpenAPI)

Core types: `DailyBrief`, `MacroSignal`, `EvidenceItem`, `DataPoint`, `Blueprint`, `Candidate`, `Portfolio`

All evidence items carry: date, source, fact, freshness enum

## Running

- Frontend: `pnpm --filter @workspace/investment-os run dev`
- API: `pnpm --filter @workspace/api-server run dev`
- Codegen: `pnpm --filter @workspace/api-spec run codegen`
