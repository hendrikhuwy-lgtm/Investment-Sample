# Backend V2 Route Policy

## Official product namespace

- `/api/v2/surfaces/*`

## Sidecar namespaces

- `/api/v2/admin/*`
- `/api/v2/debug/*`
- `/api/v2/ops/*`

## Legacy compatibility namespaces

- `/api/platform/*`
- `/api/cortex/*`

## Rule

No new investor-visible logic may be added to legacy namespaces. Legacy routes may remain for compatibility, operational continuity, or migration support, but V2 product behavior must be implemented under `/api/v2/surfaces/*`.
