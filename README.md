# Investment Sample

Production-ready sample deployment for the Cortex investment operating surface.

The production app is a single FastAPI service:

- serves the built Cortex frontend at `/cortex/`
- serves the V2 API at `/api/v2/*`
- requires authentication in production
- expects secrets from host environment variables
- stores runtime SQLite data on persistent disk

## Local Development

```bash
cd frontend-cortex
npm run dev:stack
```

Local development uses:

- backend: `http://127.0.0.1:8001`
- frontend: `http://127.0.0.1:5177/cortex/`

## Production Build

```bash
scripts/build_cortex_production.sh
```

## Production Run

Set production environment variables first:

```bash
export IA_ENV=production
export IA_LOAD_LOCAL_ENV=0
export IA_CORTEX_FRONTEND_AUTOSTART=0
export IA_AUTH_ENABLED=1
export IA_AUTH_USERNAME=investor
export IA_AUTH_PASSWORD='<long random password>'
export IA_DB_PATH=storage/investment_agent.sqlite3
```

Then start:

```bash
backend/scripts/run_v2_prod.sh
```

Open:

```text
http://localhost:8001/cortex/
```

## Render Deployment

This repo includes `Dockerfile` and `render.yaml`.

On Render:

1. Create a Blueprint from this GitHub repo.
2. Enter the `sync: false` secret values when prompted.
3. Keep the persistent disk mounted at `/app/storage`.
4. Open `/cortex/` on the Render service URL.

Required secret:

- `IA_AUTH_PASSWORD`

Market data keys are optional but improve live data coverage:

- `FINNHUB_API_KEY`
- `FRED_API_KEY`
- `POLYGON_API_KEY`
- `EODHD_API_KEY`
- `TIINGO_API_KEY`
- `ALPHA_VANTAGE_API_KEY`
- `TWELVE_DATA_API_KEY`
- `FMP_API_KEY`
- `NASDAQ_DATA_LINK_API_KEY`

See [docs/production_deployment.md](docs/production_deployment.md) for operational notes.
