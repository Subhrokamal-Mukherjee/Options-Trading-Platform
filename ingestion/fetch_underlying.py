"""Phase 1 underlying 5-minute historical ingestion.

Stores parquet partitioned by symbol/year for strict local reproducibility.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class UnderlyingIngestionConfig:
    """Input configuration for underlying candle ingestion."""

    api_key: str
    access_token: str
    instrument_tokens_path: Path
    output_root: Path
    symbol: str
    start: datetime
    end: datetime
    interval: str = "5minute"


def _build_kite_client(api_key: str, access_token: str):
    """Create authenticated Kite client."""
    try:
        from kiteconnect import KiteConnect
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "kiteconnect package is required for ingestion. Install via `pip install kiteconnect`."
        ) from exc

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def _load_underlying_token(instrument_tokens_path: Path, symbol: str) -> int:
    """Resolve index instrument token for symbol from master parquet."""
    master_df = pd.read_parquet(instrument_tokens_path)
    mask = (master_df["tradingsymbol"] == symbol) & (master_df["segment"] == "INDICES")
    selected = master_df.loc[mask]
    if selected.empty:
        raise ValueError(f"No INDICES token found for symbol={symbol} in {instrument_tokens_path}")
    return int(selected.iloc[0]["instrument_token"])


def fetch_underlying_candles(config: UnderlyingIngestionConfig) -> pd.DataFrame:
    """Fetch historical 5-minute underlying candles from Kite."""
    if config.interval != "5minute":
        raise ValueError("Phase 1 hard constraint violated: only 5minute interval is allowed")

    token = _load_underlying_token(config.instrument_tokens_path, config.symbol)
    kite = _build_kite_client(config.api_key, config.access_token)

    candles = kite.historical_data(
        instrument_token=token,
        from_date=config.start,
        to_date=config.end,
        interval=config.interval,
        continuous=False,
        oi=False,
    )
    df = pd.DataFrame(candles)
    if df.empty:
        raise ValueError("No underlying candles returned from Kite")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["symbol"] = config.symbol
    df["instrument_token"] = token
    return df.sort_values("date").reset_index(drop=True)


def save_underlying_partitioned(df: pd.DataFrame, output_root: Path) -> Path:
    """Save underlying candles with partitioning by symbol/year."""
    df = df.copy()
    df["year"] = df["date"].dt.year
    target = output_root / "underlying"
    df.to_parquet(target, partition_cols=["symbol", "year"], index=False)
    return target


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Fetch 5-minute underlying candles")
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--access-token", required=True)
    parser.add_argument("--instrument-tokens", default="data/metadata/instrument_tokens.parquet", type=Path)
    parser.add_argument("--output-root", default="data/raw", type=Path)
    parser.add_argument("--symbol", choices=["NIFTY", "BANKNIFTY"], required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    config = UnderlyingIngestionConfig(
        api_key=args.api_key,
        access_token=args.access_token,
        instrument_tokens_path=args.instrument_tokens,
        output_root=args.output_root,
        symbol=args.symbol,
        start=datetime.fromisoformat(args.start),
        end=datetime.fromisoformat(args.end),
    )

    df = fetch_underlying_candles(config)
    target = save_underlying_partitioned(df, config.output_root)
    logging.info("Saved %s rows to %s", len(df), target)


if __name__ == "__main__":
    main()
