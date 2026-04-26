#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BACKEND_ROOT}/.." && pwd)"

export IA_ENV="${IA_ENV:-production}"
export IA_LOAD_LOCAL_ENV="${IA_LOAD_LOCAL_ENV:-0}"
export IA_CORTEX_FRONTEND_AUTOSTART="${IA_CORTEX_FRONTEND_AUTOSTART:-0}"
export IA_AUTH_ENABLED="${IA_AUTH_ENABLED:-1}"

AUTH_ENABLED_NORM="$(printf '%s' "${IA_AUTH_ENABLED}" | tr '[:upper:]' '[:lower:]')"
if [[ "${AUTH_ENABLED_NORM}" != "0" && "${AUTH_ENABLED_NORM}" != "false" ]]; then
  if [[ -z "${IA_AUTH_PASSWORD:-}" && -z "${IA_AUTH_BEARER_TOKEN:-}" ]]; then
    echo "Set IA_AUTH_PASSWORD or IA_AUTH_BEARER_TOKEN in the host environment before production start." >&2
    exit 1
  fi
fi

if [[ ! -f "${REPO_ROOT}/frontend-cortex/dist/index.html" ]]; then
  echo "Missing frontend-cortex/dist/index.html. Run scripts/build_cortex_production.sh first." >&2
  exit 1
fi

PYTHON_BIN="${BACKEND_ROOT}/.venv/bin/python"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

HOST="${IA_BACKEND_V2_HOST:-0.0.0.0}"
PORT="${IA_BACKEND_V2_PORT:-${PORT:-8001}}"

cd "${BACKEND_ROOT}"
exec "${PYTHON_BIN}" -m uvicorn app.v2.app:app --host "${HOST}" --port "${PORT}"
