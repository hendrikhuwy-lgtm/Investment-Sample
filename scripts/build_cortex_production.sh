#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
FRONTEND_ROOT="${REPO_ROOT}/frontend-cortex"

cd "${FRONTEND_ROOT}"

if [[ -f package-lock.json ]]; then
  npm ci
else
  npm install
fi

npm run build

test -f "${FRONTEND_ROOT}/dist/index.html"
echo "Built Cortex production bundle at ${FRONTEND_ROOT}/dist"
