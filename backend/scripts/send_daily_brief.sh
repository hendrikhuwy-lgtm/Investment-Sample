#!/usr/bin/env bash
set -euo pipefail

BACKEND_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ROOT="$(cd "${BACKEND_ROOT}/.." && pwd)"
cd "${BACKEND_ROOT}"

PY_BIN="${PY_BIN:-${BACKEND_ROOT}/.venv/bin/python}"
if [[ ! -x "${PY_BIN}" ]]; then
  echo "python executable not found: ${PY_BIN}" >&2
  exit 1
fi

# Optional local environment file for unattended scheduler runs.
ENV_FILE="${PROJECT_ROOT}/ops/env/.env"
if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

export IA_MCP_PRIORITY_MODE="${IA_MCP_PRIORITY_MODE:-connectors_only}"
export IA_MCP_PRIORITY_SERVERS="${IA_MCP_PRIORITY_SERVERS:-ai.auteng/docs,ai.auteng/mcp,ai.com.mcp/contabo,ai.com.mcp/registry,ai.com.mcp/openai-tools,ai.com.mcp/petstore,ai.com.mcp/lenny-rachitsky-podcast}"
export IA_MCP_CONNECTORS_CANDIDATES_PATH="${IA_MCP_CONNECTORS_CANDIDATES_PATH:-mcp/connectors/financial_intelligence_candidates.json}"
export IA_MCP_REGISTRY_PRIORITIES_PATH="${IA_MCP_REGISTRY_PRIORITIES_PATH:-mcp/connectors/current_registry_priorities.json}"
export IA_MCP_CONNECTOR_ADAPTER_BASE="${IA_MCP_CONNECTOR_ADAPTER_BASE:-http://127.0.0.1:${IA_BACKEND_PORT:-8000}}"

if [[ -z "${IA_TLS_CA_BUNDLE:-}" ]]; then
  if [[ -f "/etc/ssl/cert.pem" ]]; then
    export IA_TLS_CA_BUNDLE="/etc/ssl/cert.pem"
  elif [[ -f "/private/etc/ssl/cert.pem" ]]; then
    export IA_TLS_CA_BUNDLE="/private/etc/ssl/cert.pem"
  fi
fi
if [[ -n "${IA_TLS_CA_BUNDLE:-}" ]]; then
  export SSL_CERT_FILE="${SSL_CERT_FILE:-$IA_TLS_CA_BUNDLE}"
  export REQUESTS_CA_BUNDLE="${REQUESTS_CA_BUNDLE:-$IA_TLS_CA_BUNDLE}"
fi

echo "[ia-runtime] whoami=$(whoami)"
echo "[ia-runtime] pwd=$(pwd)"
echo "[ia-runtime] python=${PY_BIN}"
"${PY_BIN}" --version || true
env | sort | grep '^IA_' || true

exec "${PY_BIN}" scripts/send_daily_brief.py "$@"
