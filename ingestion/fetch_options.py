"""Credentials can be supplied via .env using KITE_API_KEY and KITE_ACCESS_TOKEN.

Phase 1 options 5-minute historical ingestion.

This module fetches option candles in monthly windows to prevent silent truncation
at Kite historical API limits. It applies strict fail-loud integrity checks and
uses deterministic overwrite writes so reruns remain idempotent.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]


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


def monthly_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Split a datetime range into non-overlapping calendar-month windows."""
    if start > end:
        raise ValueError(f"Invalid range: start ({start}) is after end ({end})")

    windows: list[tuple[datetime, datetime]] = []
    cursor = start

    while cursor <= end:
        month_start = cursor.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_month = (month_start + timedelta(days=32)).replace(day=1)
        month_end = next_month - timedelta(microseconds=1)

        window_start = cursor
        window_end = month_end if month_end < end else end
        windows.append((window_start, window_end))
        cursor = window_end + timedelta(microseconds=1)

    return windows


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


def _assert_options_integrity(df: pd.DataFrame, symbol: str) -> None:
    """Run fail-loud integrity checks after options chunk concatenation."""
    if df["date"].isna().any():
        raise RuntimeError(f"Null timestamps found in options dataset for symbol={symbol}")

    duplicate_count = int(df.duplicated(subset=["date", "tradingsymbol"]).sum())
    if duplicate_count > 0:
        raise RuntimeError(
            f"Duplicate rows found in options dataset for symbol={symbol}: {duplicate_count}"
        )

    for tradingsymbol, group in df.groupby("tradingsymbol", sort=False):
        if not group["date"].is_monotonic_increasing:
            raise RuntimeError(
                f"Timestamp chronology violation for tradingsymbol={tradingsymbol}"
            )


def fetch_options_candles(config: OptionIngestionConfig) -> pd.DataFrame:
    """Fetch 5-minute candles for all selected option contracts in monthly chunks."""
    if config.interval != "5minute":
        raise ValueError("Phase 1 hard constraint violated: only 5minute interval is allowed")

    contracts = _load_option_contracts(
        config.instrument_tokens_path, config.symbol, config.start, config.end
    )
    kite = _build_kite_client(config.api_key, config.access_token)

    windows = monthly_windows(config.start, config.end)
    logger = logging.getLogger(__name__)
    all_chunks: list[pd.DataFrame] = []

    for row in contracts.itertuples(index=False):
        contract_rows = 0
        for window_start, window_end in windows:
            candles = kite.historical_data(
                instrument_token=int(row.instrument_token),
                from_date=window_start,
                to_date=window_end,
                interval=config.interval,
                continuous=False,
                oi=True,
            )
            chunk_df = pd.DataFrame(candles)

            if len(chunk_df) == 0:
                logger.info(
                    "Options chunk | symbol=%s | tradingsymbol=%s | window_start=%s | window_end=%s | rows=0",
                    config.symbol,
                    row.tradingsymbol,
                    window_start,
                    window_end,
                )
                continue

            if len(chunk_df) >= 2000:
                raise RuntimeError(
                    f"Chunk reached API limit for {config.symbol} {window_start} to {window_end}. "
                    "Reduce window size."
                )

            chunk_df["date"] = pd.to_datetime(chunk_df["date"], utc=True)
            chunk_df["symbol"] = config.symbol
            chunk_df["expiry"] = pd.to_datetime(row.expiry)
            chunk_df["strike"] = float(row.strike)
            chunk_df["option_type"] = str(row.instrument_type)
            chunk_df["tradingsymbol"] = str(row.tradingsymbol)
            chunk_df["instrument_token"] = int(row.instrument_token)
            all_chunks.append(chunk_df)
            contract_rows += len(chunk_df)

            logger.info(
                "Options chunk | symbol=%s | tradingsymbol=%s | window_start=%s | window_end=%s | rows=%s",
                config.symbol,
                row.tradingsymbol,
                window_start,
                window_end,
                len(chunk_df),
            )

        if contract_rows == 0:
            logger.info(
                "Options contract had no candles | symbol=%s | tradingsymbol=%s",
                config.symbol,
                row.tradingsymbol,
            )

    if not all_chunks:
        raise ValueError("No option candles returned from Kite for selected contracts")

    df = pd.concat(all_chunks, ignore_index=True)
    df = df.drop_duplicates(subset=["date", "tradingsymbol"])\
        .sort_values(["tradingsymbol", "date"]).reset_index(drop=True)

    _assert_options_integrity(df, config.symbol)

    logger.info(
        "Options ingestion complete | symbol=%s | total_rows=%s | months_processed=%s | contracts=%s",
        config.symbol,
        len(df),
        len(windows),
        contracts["tradingsymbol"].nunique(),
    )

    return df.sort_values(["date", "tradingsymbol"]).reset_index(drop=True)


def save_options_partitioned(df: pd.DataFrame, output_root: Path) -> Path:
    """Persist options candles partitioned by symbol/year/expiry using overwrite semantics."""
    payload = df.copy()
    payload["year"] = payload["date"].dt.year
    payload["expiry_partition"] = payload["expiry"].dt.strftime("%Y-%m-%d")
    target = output_root / "options"

    if target.exists():
        shutil.rmtree(target)

    payload.to_parquet(target, partition_cols=["symbol", "year", "expiry_partition"], index=False)
    return target


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Fetch 5-minute options candles")
    parser.add_argument(
        "--api-key",
        default=os.getenv("KITE_API_KEY"),
        help="Kite API key, fallback to KITE_API_KEY from .env",
    )
    parser.add_argument(
        "--access-token",
        default=os.getenv("KITE_ACCESS_TOKEN"),
        help="Kite access token, fallback to KITE_ACCESS_TOKEN from .env",
    )
    parser.add_argument(
        "--instrument-tokens",
        default=BASE_DIR / "data" / "metadata" / "instrument_tokens.parquet",
        type=Path,
    )
    parser.add_argument("--output-root", default=BASE_DIR / "data" / "raw", type=Path)
    parser.add_argument("--symbol", choices=["NIFTY", "BANKNIFTY"], required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    if not args.api_key or not args.access_token:
        raise ValueError("Missing Kite API_KEY or ACCESS_TOKEN. Provide via .env or CLI.")

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
