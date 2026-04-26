# Production Deployment Notes

Use the V2 backend as the production web server and serve the built Cortex bundle from `/cortex/`.

1. Build the frontend:

```bash
scripts/build_cortex_production.sh
```

2. Configure host environment variables. Start from `ops/env/.env.example`, but set real values in your hosting provider's secret/environment settings, not in git.

Required production values:

```bash
IA_ENV=production
IA_LOAD_LOCAL_ENV=0
IA_CORTEX_FRONTEND_AUTOSTART=0
IA_AUTH_ENABLED=1
IA_AUTH_PUBLIC_HEALTH=1
IA_AUTH_USERNAME=investor
IA_AUTH_PASSWORD=<long random password>
```

Optional market data provider keys can be added in the hosting provider's environment settings after the service is created. Leave unknown provider keys unset instead of using placeholder values.

3. Start the backend:

```bash
backend/scripts/run_v2_prod.sh
```

In production, the backend refuses to start if:

- auth is enabled but neither `IA_AUTH_PASSWORD` nor `IA_AUTH_BEARER_TOKEN` is set
- `IA_CORTEX_FRONTEND_AUTOSTART` is enabled
- `frontend-cortex/dist/index.html` is missing

For split frontend/backend hosting, set `IA_CORS_ORIGINS` to the public frontend origin, for example:

```bash
IA_CORS_ORIGINS=https://your-cortex-site.netlify.app
```
