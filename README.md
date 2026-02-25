# Options Trading Platform (NIFTY/BANKNIFTY Volatility Mispricing)

This repository implements a phase-wise, modular system for a **3–5 day volatility mispricing strategy** on NSE index options.

## Current Status

✅ **Phase 1 implemented**: Data ingestion architecture.

✅ **Phase 2 implemented**: Deterministic ATM reconstruction from consolidated UDiFF daily options data.

Included in Phase 1:
- Instrument master ingestion from Kite Connect
- Historical 5-minute underlying ingestion (monthly chunked)
- Historical 5-minute options ingestion (monthly chunked)
- Fail-loud raw data validation checks

Included in Phase 2:
- Deterministic ATM selection per `TRADE_DATE` × `SYMBOL`
- `DTE >= 3` expiry selection with minimum DTE rule
- ATM strike tie-break toward lower strike
- CE/PE pair enforcement and straddle construction
- Monthly ATM parquet output: `atm_daily_YYYY_MM.parquet`

No modeling/backtesting/risk execution logic is included yet.

## Environment Setup

Create a `.env` file in the repository root:

```bash
KITE_API_KEY=your_api_key_here
KITE_ACCESS_TOKEN=your_access_token_here
```

The ingestion scripts load credentials from `.env` automatically via `python-dotenv`.
You can still override via CLI flags `--api-key` and `--access-token` when needed.

Install dependencies (example):

```bash
pip install pandas pyarrow kiteconnect python-dotenv
```

## Phase 1 CLI Commands

### 1) Build instrument master

```bash
python ingestion/instrument_master.py
```

### 2) Fetch underlying 5-minute candles

```bash
python ingestion/fetch_underlying.py --symbol NIFTY --start 2024-01-01 --end 2024-01-31
```

### 3) Fetch options 5-minute candles

```bash
python ingestion/fetch_options.py --symbol NIFTY --start 2024-01-01 --end 2024-01-31
```

### 4) Validate raw ingestion artifacts

```bash
python main.py --phase 1
```

## Phase 2 CLI Command

### Build deterministic daily ATM dataset from consolidated UDiFF parquet

```bash
python ingestion/atm_reconstruction.py \
  --input-path data/processed/nifty_banknifty_jan_2024.parquet \
  --output-dir data/processed \
  --min-dte 3
```

This produces:
- `data/processed/atm_daily_YYYY_MM.parquet`

## Output Artifacts

Phase 1:
- `data/metadata/instrument_tokens.parquet`
- `data/raw/underlying` (partitioned by `symbol/year`)
- `data/raw/options` (partitioned by `symbol/year/expiry_partition`)
- `data/raw/verified/*` (post-validation)
- `data/metadata/phase1_validation_report.json`

Phase 2:
- `data/processed/atm_daily_YYYY_MM.parquet`
