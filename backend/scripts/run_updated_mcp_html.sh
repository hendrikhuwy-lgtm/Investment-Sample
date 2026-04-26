#!/usr/bin/env bash
set -euo pipefail

BACKEND_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ROOT="$(cd "${BACKEND_ROOT}/.." && pwd)"
cd "${BACKEND_ROOT}"

PY_BIN="${PY_BIN:-./.venv/bin/python}"
if [[ ! -x "${PY_BIN}" ]]; then
  echo "python executable not found: ${PY_BIN}" >&2
  exit 1
fi

"${PY_BIN}" - <<'PY'
from pathlib import Path

from app.config import Settings
from app.services.real_email_brief import generate_mcp_omni_email_brief

backend_root = Path.cwd()
project_root = backend_root.parent

settings = Settings(
    mcp_live_required=False,
    refresh_live_cache_on_brief=False,
)

result = generate_mcp_omni_email_brief(
    settings=settings,
    force_cache_only=True,
)

html_path = Path(result["html_path"])
final_html = project_root / "investment_agent_final_brief.html"
final_html.write_text(html_path.read_text(encoding="utf-8"), encoding="utf-8")

print("subject:", result["subject"])
print("html_outbox:", html_path)
print("html_final:", final_html)
print("mcp_connected_total:", f"{result.get('mcp_connected_count', 0)}/{result.get('mcp_total_count', 0)}")
print("cached_used:", result.get("cached_used"))
print("errors:", len(result.get("errors", [])))
PY
