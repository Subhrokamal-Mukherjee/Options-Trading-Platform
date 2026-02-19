"""Phase 1 options 5-minute historical ingestion.

Downloads option candles for configured symbol and expiries, stored partitioned by
symbol/year/expiry as parquet.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class OptionIngestionConfig:
    """Configuration for options historical download."""

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


def _load_option_contracts(path: Path, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Load relevant option contracts from instrument master."""
    master_df = pd.read_parquet(path)
    df = master_df.loc[
        (master_df["name"] == symbol)
        & (master_df["segment"] == "NFO-OPT")
        & (master_df["instrument_type"].isin(["CE", "PE"]))
    ].copy()
    if df.empty:
        raise ValueError(f"No option contracts found for {symbol}")

    df["expiry"] = pd.to_datetime(df["expiry"]).dt.tz_localize(None)
    range_mask = (df["expiry"] >= start) & (df["expiry"] <= end)
    filtered = df.loc[range_mask].copy()
    if filtered.empty:
        raise ValueError(f"No contracts in expiry range {start.date()} to {end.date()}")
    return filtered


def fetch_options_candles(config: OptionIngestionConfig) -> pd.DataFrame:
    """Fetch 5-minute candles for all selected option contracts."""
    if config.interval != "5minute":
        raise ValueError("Phase 1 hard constraint violated: only 5minute interval is allowed")

    contracts = _load_option_contracts(
        config.instrument_tokens_path, config.symbol, config.start, config.end
    )
    kite = _build_kite_client(config.api_key, config.access_token)

    all_frames: list[pd.DataFrame] = []
    for row in contracts.itertuples(index=False):
        candles = kite.historical_data(
            instrument_token=int(row.instrument_token),
            from_date=config.start,
            to_date=config.end,
            interval=config.interval,
            continuous=False,
            oi=True,
        )
        if not candles:
            continue

        frame = pd.DataFrame(candles)
        frame["date"] = pd.to_datetime(frame["date"], utc=True)
        frame["symbol"] = config.symbol
        frame["expiry"] = pd.to_datetime(row.expiry)
        frame["strike"] = float(row.strike)
        frame["option_type"] = str(row.instrument_type)
        frame["tradingsymbol"] = str(row.tradingsymbol)
        frame["instrument_token"] = int(row.instrument_token)
        all_frames.append(frame)

    if not all_frames:
        raise ValueError("No option candles returned from Kite for selected contracts")

    return pd.concat(all_frames, ignore_index=True).sort_values(["date", "tradingsymbol"]).reset_index(
        drop=True
    )


def save_options_partitioned(df: pd.DataFrame, output_root: Path) -> Path:
    """Persist options candles partitioned by symbol/year/expiry."""
    payload = df.copy()
    payload["year"] = payload["date"].dt.year
    payload["expiry_partition"] = payload["expiry"].dt.strftime("%Y-%m-%d")
    target = output_root / "options"
    payload.to_parquet(target, partition_cols=["symbol", "year", "expiry_partition"], index=False)
    return target


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Fetch 5-minute options candles")
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
    config = OptionIngestionConfig(
        api_key=args.api_key,
        access_token=args.access_token,
        instrument_tokens_path=args.instrument_tokens,
        output_root=args.output_root,
        symbol=args.symbol,
        start=datetime.fromisoformat(args.start),
        end=datetime.fromisoformat(args.end),
    )

    df = fetch_options_candles(config)
    target = save_options_partitioned(df, config.output_root)
    logging.info("Saved %s rows to %s", len(df), target)


if __name__ == "__main__":
    main()
