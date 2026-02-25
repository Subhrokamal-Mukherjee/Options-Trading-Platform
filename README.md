# Options Trading Platform (NIFTY/BANKNIFTY Volatility Mispricing)

This repository implements a phase-wise, modular system for a **3–5 day volatility mispricing strategy** on NSE index options.

## Current Status

✅ **Phase 1 implemented**: Data ingestion architecture only.

Included in Phase 1:
- Instrument master ingestion from Kite Connect
- Historical 5-minute underlying ingestion
- Historical 5-minute options ingestion
- Fail-loud raw data validation checks

No modeling/backtesting/risk execution logic is part of this phase.

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

## Output Artifacts

- `data/metadata/instrument_tokens.parquet`
- `data/raw/underlying` (partitioned)
- `data/raw/options` (partitioned)
- `data/raw/verified/*` (post-validation)
- `data/metadata/phase1_validation_report.json`
