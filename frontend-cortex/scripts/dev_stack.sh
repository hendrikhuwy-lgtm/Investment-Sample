#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${FRONTEND_ROOT}/.." && pwd)"
BACKEND_ROOT="${REPO_ROOT}/backend"
PRESET_BACKEND_PORT="${IA_BACKEND_V2_PORT-}"
PRESET_BACKEND_HOST="${IA_BACKEND_V2_HOST-}"
PRESET_FRONTEND_HOST="${IA_CORTEX_FRONTEND_HOST-}"
PRESET_FRONTEND_PORT="${IA_CORTEX_FRONTEND_PORT-}"
PRESET_FRONTEND_CANONICAL_URL="${IA_CORTEX_FRONTEND_CANONICAL_URL-}"
PRESET_FORECAST_AUTOSTART="${IA_FORECAST_AUTOSTART_ENABLED-}"
PRESET_FORECAST_INCLUDE_BENCHMARKS="${IA_FORECAST_AUTOSTART_INCLUDE_BENCHMARKS-}"
PRESET_FORECAST_RECHECK_SECONDS="${IA_FORECAST_AUTOSTART_RECHECK_SECONDS-}"
PRESET_FORECAST_DEFER="${IA_FORECAST_DEFER_UNTIL_SURFACE_READY-}"
PRESET_RUNTIME_SCHEDULER="${IA_RUNTIME_SCHEDULER_ENABLED-}"

if [[ -f "${REPO_ROOT}/ops/env/.env.local" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/ops/env/.env.local"
fi

if [[ -n "${PRESET_BACKEND_PORT}" ]]; then
  export IA_BACKEND_V2_PORT="${PRESET_BACKEND_PORT}"
fi
if [[ -n "${PRESET_BACKEND_HOST}" ]]; then
  export IA_BACKEND_V2_HOST="${PRESET_BACKEND_HOST}"
fi
if [[ -n "${PRESET_FRONTEND_HOST}" ]]; then
  export IA_CORTEX_FRONTEND_HOST="${PRESET_FRONTEND_HOST}"
fi
if [[ -n "${PRESET_FRONTEND_PORT}" ]]; then
  export IA_CORTEX_FRONTEND_PORT="${PRESET_FRONTEND_PORT}"
fi
if [[ -n "${PRESET_FRONTEND_CANONICAL_URL}" ]]; then
  export IA_CORTEX_FRONTEND_CANONICAL_URL="${PRESET_FRONTEND_CANONICAL_URL}"
fi
if [[ -n "${PRESET_FORECAST_AUTOSTART}" ]]; then
  export IA_FORECAST_AUTOSTART_ENABLED="${PRESET_FORECAST_AUTOSTART}"
fi
if [[ -n "${PRESET_FORECAST_INCLUDE_BENCHMARKS}" ]]; then
  export IA_FORECAST_AUTOSTART_INCLUDE_BENCHMARKS="${PRESET_FORECAST_INCLUDE_BENCHMARKS}"
fi
if [[ -n "${PRESET_FORECAST_RECHECK_SECONDS}" ]]; then
  export IA_FORECAST_AUTOSTART_RECHECK_SECONDS="${PRESET_FORECAST_RECHECK_SECONDS}"
fi
if [[ -n "${PRESET_FORECAST_DEFER}" ]]; then
  export IA_FORECAST_DEFER_UNTIL_SURFACE_READY="${PRESET_FORECAST_DEFER}"
fi
if [[ -n "${PRESET_RUNTIME_SCHEDULER}" ]]; then
  export IA_RUNTIME_SCHEDULER_ENABLED="${PRESET_RUNTIME_SCHEDULER}"
fi

BACKEND_PORT="${IA_BACKEND_V2_PORT:-8001}"
BACKEND_HOST="${IA_BACKEND_V2_HOST:-127.0.0.1}"
FRONTEND_HOST="${IA_CORTEX_FRONTEND_HOST:-127.0.0.1}"
FRONTEND_PORT="${IA_CORTEX_FRONTEND_PORT:-5177}"
CORTEX_FRONTEND_CANONICAL_URL="${IA_CORTEX_FRONTEND_CANONICAL_URL:-http://${FRONTEND_HOST}:${FRONTEND_PORT}/cortex}"
BACKEND_HEALTH_URL="${IA_BACKEND_V2_HEALTH_URL:-http://${BACKEND_HOST}:${BACKEND_PORT}/api/v2/health}"
BACKEND_RUNTIME_URL="${IA_BACKEND_V2_RUNTIME_URL:-http://${BACKEND_HOST}:${BACKEND_PORT}/api/v2/admin/forecast/runtime}"
BACKEND_READINESS_URL="${IA_BACKEND_V2_READINESS_URL:-http://${BACKEND_HOST}:${BACKEND_PORT}/api/v2/admin/forecast/readiness}"
BACKEND_CORTEX_URL="${IA_BACKEND_V2_CORTEX_URL:-http://${BACKEND_HOST}:${BACKEND_PORT}/cortex/?frontend_alignment_probe=1}"
BACKEND_LOG="${REPO_ROOT}/outbox/forecast_sidecars/backend-v2.log"
mkdir -p "$(dirname "${BACKEND_LOG}")"

export IA_FORECAST_AUTOSTART_ENABLED="${IA_FORECAST_AUTOSTART_ENABLED:-0}"
export IA_FORECAST_AUTOSTART_INCLUDE_BENCHMARKS="${IA_FORECAST_AUTOSTART_INCLUDE_BENCHMARKS:-0}"
export IA_FORECAST_AUTOSTART_RECHECK_SECONDS="${IA_FORECAST_AUTOSTART_RECHECK_SECONDS:-45}"
export IA_FORECAST_DEFER_UNTIL_SURFACE_READY="${IA_FORECAST_DEFER_UNTIL_SURFACE_READY:-1}"
export IA_RUNTIME_SCHEDULER_ENABLED="${IA_RUNTIME_SCHEDULER_ENABLED:-0}"
BACKEND_PROBE_TIMEOUT_SECONDS="${IA_BACKEND_V2_PROBE_TIMEOUT_SECONDS:-2}"

backend_health_ok() {
  curl --max-time "${BACKEND_PROBE_TIMEOUT_SECONDS}" -fsS "${BACKEND_HEALTH_URL}" >/dev/null 2>&1
}

cortex_redirects_to_canonical() {
  local redirect_url
  redirect_url="$(curl --max-time "${BACKEND_PROBE_TIMEOUT_SECONDS}" -fsS -o /dev/null -w "%{redirect_url}" "${BACKEND_CORTEX_URL}" 2>/dev/null || true)"
  [[ "${redirect_url}" == "${CORTEX_FRONTEND_CANONICAL_URL}"* ]]
}

backend_compatible() {
  backend_health_ok \
    && curl --max-time "${BACKEND_PROBE_TIMEOUT_SECONDS}" -fsS "${BACKEND_RUNTIME_URL}" >/dev/null 2>&1 \
    && cortex_redirects_to_canonical
}

listener_pid() {
  lsof -t -iTCP:"${BACKEND_PORT}" -sTCP:LISTEN 2>/dev/null | head -n 1
}

terminate_stale_backend() {
  local pid
  pid="$(listener_pid || true)"
  if [[ -z "${pid}" ]]; then
    return 0
  fi
  kill "${pid}" >/dev/null 2>&1 || true
  for _ in $(seq 1 20); do
    if ! lsof -t -iTCP:"${BACKEND_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  kill -9 "${pid}" >/dev/null 2>&1 || true
}

if ! backend_compatible; then
  if [[ -n "$(listener_pid || true)" ]]; then
    terminate_stale_backend
  fi
  (
    cd "${BACKEND_ROOT}"
    IA_CORTEX_FRONTEND_CANONICAL_URL="${CORTEX_FRONTEND_CANONICAL_URL}" \
      nohup bash scripts/run_v2_dev.sh >>"${BACKEND_LOG}" 2>&1 &
  )
fi

for _ in $(seq 1 60); do
  if backend_compatible; then
    break
  fi
  sleep 1
done

forecast_autostart_norm="$(printf '%s' "${IA_FORECAST_AUTOSTART_ENABLED:-1}" | tr '[:upper:]' '[:lower:]')"
forecast_defer_norm="$(printf '%s' "${IA_FORECAST_DEFER_UNTIL_SURFACE_READY:-1}" | tr '[:upper:]' '[:lower:]')"
if [[ "${forecast_autostart_norm}" != "0" && "${forecast_autostart_norm}" != "false" && "${forecast_autostart_norm}" != "no" && "${forecast_autostart_norm}" != "off" \
  && "${forecast_defer_norm}" != "1" && "${forecast_defer_norm}" != "true" && "${forecast_defer_norm}" != "yes" && "${forecast_defer_norm}" != "on" ]]; then
  for _ in $(seq 1 60); do
    readiness_payload="$(curl -fsS "${BACKEND_READINESS_URL}" 2>/dev/null || true)"
    if [[ -n "${readiness_payload}" ]] && python3 -c 'import json,sys; payload=json.loads(sys.argv[1]); rows=payload.get("providers", []); core={"chronos","timesfm"}; ready={str(row.get("provider")) for row in rows if row.get("ready")}; sys.exit(0 if core <= ready else 1)' "${readiness_payload}"; then
      break
    fi
    sleep 1
  done
fi

cd "${FRONTEND_ROOT}"
echo "canonical_frontend=${CORTEX_FRONTEND_CANONICAL_URL}"
exec npm run dev -- --host "${FRONTEND_HOST}" --port "${FRONTEND_PORT}"
