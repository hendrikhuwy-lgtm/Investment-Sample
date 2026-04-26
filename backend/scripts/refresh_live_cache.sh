#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
CACHE_DIR="$PROJECT_DIR/outbox/live_cache"
mkdir -p "$CACHE_DIR"

REGISTRY_URL="${IA_MCP_REGISTRY_URL:-https://registry.modelcontextprotocol.io/v0/servers}"

fetch() {
  local url="$1"
  local out="$2"
  local tmp="${out}.tmp"
  local attempt
  for attempt in 1 2 3 4; do
    if curl -sL --retry 2 --retry-all-errors --max-time 45 "$url" > "$tmp" && [[ -s "$tmp" ]]; then
      mv "$tmp" "$out"
      return 0
    fi
    sleep "$attempt"
  done
  rm -f "$tmp"
  echo "failed to fetch after retries: $url" >&2
  return 1
}

valid_fred_csv() {
  local file="$1"
  local expected="$2"
  [[ -s "$file" ]] || return 1
  head -n 1 "$file" | grep -q "^observation_date,${expected}$" || return 1
  grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2},' "$file" || return 1
}

valid_json_chart() {
  local file="$1"
  python3 - "$file" <<'PY'
import json, sys
from pathlib import Path
path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text())
except Exception:
    raise SystemExit(1)
result = ((payload.get("chart") or {}).get("result") or [])
if not result:
    raise SystemExit(1)
timestamps = result[0].get("timestamp") or []
quote = ((result[0].get("indicators") or {}).get("quote") or [{}])[0]
closes = [item for item in (quote.get("close") or []) if item is not None]
if len(timestamps) < 6 or len(closes) < 6:
    raise SystemExit(1)
PY
}

fetch_fred_series() {
  local code="$1"
  local url="https://fred.stlouisfed.org/graph/fredgraph.csv?id=${code}"
  local out="$CACHE_DIR/${code}.csv"
  local tmp="${out}.tmp"
  if fetch "$url" "$out"; then
    if valid_fred_csv "$out" "$code"; then
      return 0
    fi
  fi
  rm -f "$tmp"
  return 1
}

fetch_sg10_from_mas() {
  local out="$CACHE_DIR/IRLTLT01SGM156N.csv"
  local tmp="${out}.tmp"
  python3 - "$tmp" <<'PY'
import csv
import sys
from pathlib import Path
import requests
from bs4 import BeautifulSoup

url = "https://eservices.mas.gov.sg/statistics/fdanet/BondOriginalMaturities.aspx?type=NX"
html = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}).text
soup = BeautifulSoup(html, "html.parser")
table = soup.find("table", id="ContentPlaceHolder1_OriginalMaturitiesTable")
if table is None:
    raise SystemExit(1)
rows = table.find_all("tr")
if len(rows) < 3:
    raise SystemExit(1)
yield_headers = []
header_cells = rows[1].find_all(["th", "td"])
for idx, cell in enumerate(header_cells[1:], start=1):
    if cell.get_text(" ", strip=True).lower() == "yield":
        yield_headers.append(idx)
if not yield_headers:
    raise SystemExit(1)
latest_yield_col = yield_headers[-1] + 1
series = []
for row in rows[2:]:
    cells = row.find_all("td")
    if len(cells) <= latest_yield_col:
        continue
    date_text = cells[0].get_text(" ", strip=True)
    value_text = cells[latest_yield_col].get_text(" ", strip=True)
    if not date_text or not value_text:
        continue
    try:
        parsed_date = __import__("datetime").datetime.strptime(date_text, "%d %b %Y").date().isoformat()
        value = float(value_text)
    except Exception:
        continue
    series.append((parsed_date, value))
if len(series) < 6:
    raise SystemExit(1)
series.sort()
out = Path(sys.argv[1])
with out.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.writer(handle)
    writer.writerow(["observation_date", "IRLTLT01SGM156N"])
    writer.writerows(series)
PY
  if valid_fred_csv "$tmp" "IRLTLT01SGM156N"; then
    mv "$tmp" "$out"
    return 0
  fi
  rm -f "$tmp"
  return 1
}

fetch_sti_proxy_fallback() {
  local out="$CACHE_DIR/stooq_sti_proxy.csv"
  local tmp="${out}.tmp"
  curl -sL --retry 2 --retry-all-errors --max-time 45 "https://stooq.com/q/d/l/?s=%5Esti&i=d" > "${tmp}.raw" || true
  python3 - "${tmp}.raw" "$tmp" <<'PY'
import csv
import sys
from pathlib import Path

raw = Path(sys.argv[1])
if not raw.exists():
    raise SystemExit(1)
text = raw.read_text(encoding="utf-8", errors="ignore")
lines = [line.strip() for line in text.splitlines() if line.strip()]
if len(lines) < 7 or not lines[0].lower().startswith("date,open,high,low,close"):
    raise SystemExit(1)
rows = []
for line in lines[1:]:
    parts = [part.strip() for part in line.split(",")]
    if len(parts) < 5:
        continue
    date_text, close_text = parts[0], parts[4]
    try:
        value = float(close_text)
    except Exception:
        continue
    rows.append((date_text, value))
if len(rows) < 6:
    raise SystemExit(1)
out = Path(sys.argv[2])
with out.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.writer(handle)
    writer.writerow(["observation_date", "STI_PROXY"])
    writer.writerows(rows)
PY
  if [[ -s "$tmp" ]]; then
    mv "$tmp" "$out"
    rm -f "${tmp}.raw"
    return 0
  fi
  rm -f "$tmp"
  rm -f "${tmp}.raw"
  return 1
}

fetch_fred_series "DGS10"
fetch_fred_series "T10Y2Y"
fetch_fred_series "T10YIE"
fetch_fred_series "SP500"
fetch_fred_series "VIXCLS"
fetch_fred_series "DCOILWTICO"
fetch_fred_series "DTWEXBGS"
fetch_fred_series "VXEEMCLS"
fetch_fred_series "DEXSIUS"
fetch_fred_series "BAMLH0A0HYM2"
fetch_fred_series "IRLTLT01EZM156N" || true
if ! fetch_fred_series "IRLTLT01SGM156N"; then
  fetch_sg10_from_mas || true
fi

fetch "$REGISTRY_URL" "$CACHE_DIR/mcp_servers.json"
fetch "https://registry.modelcontextprotocol.io/v0/servers" "$CACHE_DIR/mcp_registry_snapshot.json"

fetch "https://www.oaktreecapital.com/insights/memo/sea-change" "$CACHE_DIR/oaktree_sea_change.html"
fetch "https://www.fooledbyrandomness.com/FatTails.html" "$CACHE_DIR/taleb_fat_tails.html"
fetch "https://www.iras.gov.sg/taxes/individual-income-tax/basics-of-individual-income-tax/what-is-taxable-what-is-not/income-received-from-overseas" "$CACHE_DIR/iras_overseas_income.html"
fetch "https://www.irs.gov/individuals/international-taxpayers/federal-income-tax-withholding-and-reporting-on-other-kinds-of-us-source-income-paid-to-nonresident-aliens" "$CACHE_DIR/irs_withholding_nra.html"

# Big player proxies (public)
fetch "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=13F-HR&owner=include&count=10&output=atom" "$CACHE_DIR/sec_13f_atom.xml" || true
fetch "https://www.federalreserve.gov/newsevents/pressreleases.htm" "$CACHE_DIR/fed_press_releases.html" || true

# Stock index participation and ETF volume indicators (public endpoints)
fetch "https://stooq.com/q/d/l/?s=spy.us&i=d" "$CACHE_DIR/stooq_spy_volume.csv" || true
fetch "https://stooq.com/q/d/l/?s=qqq.us&i=d" "$CACHE_DIR/stooq_qqq_volume.csv" || true
# Yahoo kept as optional fallback; may rate-limit.
fetch "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?range=6mo&interval=1d" "$CACHE_DIR/yahoo_spx_chart.json" || true
fetch "https://query1.finance.yahoo.com/v8/finance/chart/SPY?range=6mo&interval=1d" "$CACHE_DIR/yahoo_spy_chart.json" || true
if fetch "https://query1.finance.yahoo.com/v8/finance/chart/%5ESTI?range=6mo&interval=1d" "$CACHE_DIR/yahoo_sti_chart.json"; then
  if ! valid_json_chart "$CACHE_DIR/yahoo_sti_chart.json"; then
    rm -f "$CACHE_DIR/yahoo_sti_chart.json"
    fetch_sti_proxy_fallback || true
  fi
else
  fetch_sti_proxy_fallback || true
fi
if fetch "https://query1.finance.yahoo.com/v8/finance/chart/VEA?range=6mo&interval=1d" "$CACHE_DIR/yahoo_vea_chart.json"; then
  if ! valid_json_chart "$CACHE_DIR/yahoo_vea_chart.json"; then
    rm -f "$CACHE_DIR/yahoo_vea_chart.json"
  fi
fi

echo "Refreshed live cache in $CACHE_DIR"
