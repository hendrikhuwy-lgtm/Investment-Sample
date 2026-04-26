FROM node:24-slim AS frontend-builder

WORKDIR /app

COPY frontend-cortex/package.json frontend-cortex/package-lock.json ./frontend-cortex/
COPY shared ./shared

WORKDIR /app/frontend-cortex
RUN npm ci

COPY frontend-cortex ./
RUN npm run build

FROM python:3.12-slim AS runtime

ENV IA_ENV=production \
    IA_LOAD_LOCAL_ENV=0 \
    IA_CORTEX_FRONTEND_AUTOSTART=0 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY backend ./backend
COPY shared ./shared
COPY docs ./docs
COPY ops/env/.env.example ./ops/env/.env.example
COPY --from=frontend-builder /app/frontend-cortex/dist ./frontend-cortex/dist

RUN chmod +x backend/scripts/run_v2_prod.sh

EXPOSE 8001

CMD ["bash", "backend/scripts/run_v2_prod.sh"]
