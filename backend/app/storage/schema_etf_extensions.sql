-- ETF Data Extensions: Complete factsheet and market data coverage
-- All fields must have citations (no hallucinated data)

-- Holdings breakdown (daily from issuer CSV files)
CREATE TABLE IF NOT EXISTS etf_holdings (
  holding_id TEXT PRIMARY KEY,
  etf_symbol TEXT NOT NULL,
  asof_date TEXT NOT NULL,
  security_name TEXT NOT NULL,
  security_isin TEXT,
  security_ticker TEXT,
  weight_pct REAL NOT NULL,
  shares REAL,
  market_value REAL,
  sector TEXT,
  country TEXT,
  asset_class TEXT,
  retrieved_at TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_id TEXT NOT NULL,
  UNIQUE(etf_symbol, asof_date, security_isin)
);

CREATE INDEX IF NOT EXISTS idx_etf_holdings_symbol_date ON etf_holdings(etf_symbol, asof_date DESC);

-- Holdings summaries (factsheet/html derived when full holdings are unavailable)
CREATE TABLE IF NOT EXISTS etf_holdings_summaries (
  summary_id TEXT PRIMARY KEY,
  etf_symbol TEXT NOT NULL,
  asof_date TEXT NOT NULL,
  holdings_count REAL,
  top_10_concentration REAL,
  us_weight REAL,
  em_weight REAL,
  sector_concentration_proxy REAL,
  developed_market_exposure_summary TEXT,
  emerging_market_exposure_summary TEXT,
  top_country TEXT,
  coverage_class TEXT NOT NULL,
  retrieved_at TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_id TEXT NOT NULL,
  UNIQUE(etf_symbol, asof_date, source_id)
);

CREATE INDEX IF NOT EXISTS idx_etf_holdings_summaries_symbol_date
ON etf_holdings_summaries(etf_symbol, asof_date DESC);

-- Factsheet metrics (monthly from PDFs/issuer pages)
CREATE TABLE IF NOT EXISTS etf_factsheet_metrics (
  metric_id TEXT PRIMARY KEY,
  etf_symbol TEXT NOT NULL,
  asof_date TEXT NOT NULL,
  aum_usd REAL,
  aum_currency TEXT,
  aum_original REAL,
  tracking_difference_1y REAL,
  tracking_difference_3y REAL,
  tracking_difference_5y REAL,
  tracking_error_1y REAL,
  dividend_yield REAL,
  distribution_frequency TEXT,
  last_distribution_date TEXT,
  last_distribution_amount REAL,
  inception_date TEXT,
  benchmark_index TEXT,
  retrieved_at TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_id TEXT NOT NULL,
  factsheet_pdf_hash TEXT,
  UNIQUE(etf_symbol, asof_date)
);

CREATE INDEX IF NOT EXISTS idx_etf_factsheet_symbol_date ON etf_factsheet_metrics(etf_symbol, asof_date DESC);

-- Market data (daily from exchanges)
CREATE TABLE IF NOT EXISTS etf_market_data (
  market_data_id TEXT PRIMARY KEY,
  etf_symbol TEXT NOT NULL,
  exchange TEXT NOT NULL,
  asof_date TEXT NOT NULL,
  asof_time TEXT,
  last_price REAL,
  bid_price REAL,
  ask_price REAL,
  bid_ask_spread_abs REAL,
  bid_ask_spread_bps REAL,
  volume_day REAL,
  volume_30d_avg REAL,
  volume_90d_avg REAL,
  nav REAL,
  premium_discount_pct REAL,
  retrieved_at TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_id TEXT NOT NULL,
  UNIQUE(etf_symbol, exchange, asof_date, asof_time)
);

CREATE INDEX IF NOT EXISTS idx_etf_market_symbol_date ON etf_market_data(etf_symbol, asof_date DESC);

-- Data source inventory (track what sources are configured per ETF)
CREATE TABLE IF NOT EXISTS etf_data_sources (
  source_config_id TEXT PRIMARY KEY,
  etf_symbol TEXT NOT NULL,
  data_type TEXT NOT NULL, -- 'holdings', 'factsheet', 'market_data'
  source_id TEXT NOT NULL,
  source_url_template TEXT NOT NULL,
  fetch_method TEXT NOT NULL, -- 'csv_download', 'pdf_extract', 'api_call', 'html_scrape'
  parser_type TEXT,
  update_frequency TEXT NOT NULL, -- 'daily', 'weekly', 'monthly'
  last_successful_fetch TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(etf_symbol, data_type, source_id)
);

-- Fetch run log (audit trail for all ETF data ingestion)
CREATE TABLE IF NOT EXISTS etf_fetch_runs (
  run_id TEXT PRIMARY KEY,
  etf_symbol TEXT NOT NULL,
  data_type TEXT NOT NULL,
  source_id TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL, -- 'success', 'failed', 'partial'
  records_fetched INTEGER,
  error_message TEXT,
  source_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_etf_fetch_runs_symbol ON etf_fetch_runs(etf_symbol, started_at DESC);
