"""Credentials can be supplied via .env using KITE_API_KEY and KITE_ACCESS_TOKEN.

Phase 1 underlying 5-minute historical ingestion.

This module uses monthly chunking to avoid Kite historical API truncation near the
~2000 candle limit. Each chunk is validated, then all chunks are merged with strict
post-ingestion assertions. Output writes use deterministic overwrite behavior so
re-runs are idempotent and do not accumulate stale partitions.
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


def _load_underlying_token(instrument_tokens_path: Path, symbol: str) -> int:
    """Resolve index instrument token for symbol from master parquet."""
    master_df = pd.read_parquet(instrument_tokens_path)
    mask = (master_df["tradingsymbol"] == symbol) & (master_df["segment"] == "INDICES")
    selected = master_df.loc[mask]
    if selected.empty:
        raise ValueError(f"No INDICES token found for symbol={symbol} in {instrument_tokens_path}")
    return int(selected.iloc[0]["instrument_token"])


def _assert_underlying_integrity(df: pd.DataFrame, symbol: str) -> None:
    """Run fail-loud integrity checks after underlying chunk concatenation."""
    if df["date"].isna().any():
        raise RuntimeError(f"Null timestamps found for symbol={symbol}")

    duplicate_count = int(df.duplicated(subset=["date"]).sum())
    if duplicate_count > 0:
        raise RuntimeError(f"Duplicate timestamps found for symbol={symbol}: {duplicate_count}")

    if not df["date"].is_monotonic_increasing:
        raise RuntimeError(f"Timestamp chronology violation for symbol={symbol}")


def fetch_underlying_candles(config: UnderlyingIngestionConfig) -> pd.DataFrame:
    """Fetch historical 5-minute underlying candles from Kite in monthly chunks."""
    if config.interval != "5minute":
        raise ValueError("Phase 1 hard constraint violated: only 5minute interval is allowed")

    token = _load_underlying_token(config.instrument_tokens_path, config.symbol)
    kite = _build_kite_client(config.api_key, config.access_token)

    windows = monthly_windows(config.start, config.end)
    all_chunks: list[pd.DataFrame] = []
    logger = logging.getLogger(__name__)

    for window_start, window_end in windows:
        candles = kite.historical_data(
            instrument_token=token,
            from_date=window_start,
            to_date=window_end,
            interval=config.interval,
            continuous=False,
            oi=False,
        )
        chunk_df = pd.DataFrame(candles)

        if len(chunk_df) == 0:
            logger.info(
                "Underlying chunk | symbol=%s | window_start=%s | window_end=%s | rows=0",
                config.symbol,
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
        chunk_df["instrument_token"] = token
        all_chunks.append(chunk_df)

        logger.info(
            "Underlying chunk | symbol=%s | window_start=%s | window_end=%s | rows=%s",
            config.symbol,
            window_start,
            window_end,
            len(chunk_df),
        )

    if not all_chunks:
        raise ValueError("No underlying candles returned from Kite")

    df = pd.concat(all_chunks, ignore_index=True)
    df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    _assert_underlying_integrity(df, config.symbol)

    logger.info(
        "Underlying ingestion complete | symbol=%s | total_rows=%s | months_processed=%s",
        config.symbol,
        len(df),
        len(windows),
    )
    return df


def save_underlying_partitioned(df: pd.DataFrame, output_root: Path) -> Path:
    """Save underlying candles with partitioning by symbol/year using overwrite semantics."""
    payload = df.copy()
    payload["year"] = payload["date"].dt.year
    target = output_root / "underlying"

    if target.exists():
        shutil.rmtree(target)

    payload.to_parquet(target, partition_cols=["symbol", "year"], index=False)
    return target


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Fetch 5-minute underlying candles")
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
