#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SNAPSHOT_PATH="${REPO_ROOT}/mcp/registry_snapshot.json"
REGISTRY_HOST="registry.modelcontextprotocol.io"
REGISTRY_URL="https://${REGISTRY_HOST}/"

print_section() {
  printf '\n==== %s ====\n' "$1"
}

print_kv() {
  printf '%-28s %s\n' "$1" "$2"
}

sample_endpoint="$(python3 - <<'PY' "${SNAPSHOT_PATH}"
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

snapshot = Path(sys.argv[1])
if not snapshot.exists():
    print("")
    raise SystemExit(0)

try:
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
except Exception:
    print("")
    raise SystemExit(0)

rows = payload if isinstance(payload, list) else payload.get("servers", [])
if not isinstance(rows, list):
    print("")
    raise SystemExit(0)

def iter_urls(record):
    server = record.get("server", record)
    if not isinstance(server, dict):
        return
    remotes = server.get("remotes", [])
    if isinstance(remotes, list):
        for remote in remotes:
            if isinstance(remote, dict):
                url = str(remote.get("url", "") or "")
                if url.startswith(("http://", "https://", "ws://", "wss://")):
                    yield url
    direct = str(server.get("url", "") or "")
    if direct.startswith(("http://", "https://", "ws://", "wss://")):
        yield direct
    packages = server.get("packages", [])
    if isinstance(packages, list):
        for pkg in packages:
            if not isinstance(pkg, dict):
                continue
            transport = pkg.get("transport")
            if isinstance(transport, dict):
                url = str(transport.get("url", "") or "")
                if url.startswith(("http://", "https://", "ws://", "wss://")):
                    yield url

for row in rows:
    for url in iter_urls(row):
        parsed = urlparse(url)
        if parsed.hostname:
            print(url)
            raise SystemExit(0)

print("")
PY
)"

sample_host="$(python3 - <<'PY' "${sample_endpoint}"
import sys
from urllib.parse import urlparse
url = (sys.argv[1] or "").strip()
if not url:
    print("")
    raise SystemExit(0)
parsed = urlparse(url)
print(parsed.hostname or "")
PY
)"

if [[ -z "${sample_endpoint}" ]]; then
  sample_endpoint="${REGISTRY_URL}"
fi
if [[ -z "${sample_host}" ]]; then
  sample_host="${REGISTRY_HOST}"
fi

print_section "Environment"
print_kv "Date" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
print_kv "Snapshot" "${SNAPSHOT_PATH}"
print_kv "Registry Host" "${REGISTRY_HOST}"
print_kv "Sample MCP Host" "${sample_host}"
print_kv "Sample MCP Endpoint" "${sample_endpoint}"
print_kv "IA_PROXY_MODE" "${IA_PROXY_MODE:-<unset>}"
print_kv "HTTPS_PROXY" "${HTTPS_PROXY:-${https_proxy:-<unset>}}"
print_kv "HTTP_PROXY" "${HTTP_PROXY:-${http_proxy:-<unset>}}"
print_kv "NO_PROXY" "${NO_PROXY:-${no_proxy:-<unset>}}"

if [[ "$(uname -s)" == "Darwin" ]]; then
  print_section "macOS DNS Configuration"
  scutil --dns || true
else
  print_section "DNS Configuration"
  if command -v resolvectl >/dev/null 2>&1; then
    resolvectl status || true
  elif [[ -f /etc/resolv.conf ]]; then
    cat /etc/resolv.conf
  fi
fi

print_section "DNS Lookups"
if command -v nslookup >/dev/null 2>&1; then
  nslookup "${REGISTRY_HOST}" || true
  nslookup "${sample_host}" || true
else
  echo "nslookup not available"
fi

print_section "HTTP Connectivity (verbose)"
if command -v curl >/dev/null 2>&1; then
  curl -v --connect-timeout 8 --max-time 15 "${REGISTRY_URL}" >/dev/null 2>&1 || true
  curl -v --connect-timeout 8 --max-time 15 "${sample_endpoint}" >/dev/null 2>&1 || true
  echo "Curl verbose probes completed (see stderr output above in terminal)."
else
  echo "curl not available"
fi

print_section "Done"
echo "No system state was modified."
