"""Credentials can be supplied via .env using KITE_API_KEY and KITE_ACCESS_TOKEN.

Phase 1 instrument master ingestion for NSE index options universe.

This module downloads the Kite instruments dump, filters to configured symbols,
and stores a normalized parquet artifact used by downstream ingestion scripts.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pandas as pd

REQUIRED_COLUMNS: Final[set[str]] = {
    "instrument_token",
    "exchange_token",
    "tradingsymbol",
    "name",
    "last_price",
    "expiry",
    "strike",
    "tick_size",
    "lot_size",
    "instrument_type",
    "segment",
    "exchange",
}


@dataclass(frozen=True)
class InstrumentMasterConfig:
    """Configuration for instrument master build."""

    api_key: str
    access_token: str
    output_path: Path
    symbols: tuple[str, ...] = ("NIFTY", "BANKNIFTY")


def _build_kite_client(api_key: str, access_token: str):
    """Create authenticated Kite client.

    Raises:
        RuntimeError: If kiteconnect is not installed.
    """
    try:
        from kiteconnect import KiteConnect
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "kiteconnect package is required for ingestion. Install via `pip install kiteconnect`."
        ) from exc

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def fetch_instrument_master(config: InstrumentMasterConfig) -> pd.DataFrame:
    """Fetch and filter NSE derivatives + index instruments.

    Args:
        config: Ingestion configuration.

    Returns:
        Normalized instrument dataframe.

    Raises:
        ValueError: If required columns are missing.
    """
    kite = _build_kite_client(config.api_key, config.access_token)
    raw_df = pd.DataFrame(kite.instruments())

    missing = REQUIRED_COLUMNS.difference(raw_df.columns)
    if missing:
        raise ValueError(f"Instrument dump missing required columns: {sorted(missing)}")

    mask_options = (
        raw_df["name"].isin(config.symbols)
        & raw_df["segment"].isin(["NFO-OPT", "NFO-FUT"])
    )
    mask_indices = (
        raw_df["tradingsymbol"].isin(config.symbols)
        & (raw_df["segment"] == "INDICES")
    )

    df = raw_df.loc[mask_options | mask_indices, list(REQUIRED_COLUMNS)].copy()
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
    df["ingested_at"] = pd.Timestamp.utcnow()

    return df.sort_values(["name", "segment", "expiry", "strike", "instrument_type"]).reset_index(
        drop=True
    )


def save_instrument_master(df: pd.DataFrame, output_path: Path) -> None:
    """Persist instrument master parquet to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Download and persist Kite instrument master")
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
        "--output-path",
        default="data/metadata/instrument_tokens.parquet",
        type=Path,
        help="Destination parquet path",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    if not args.api_key or not args.access_token:
        raise ValueError("Missing Kite API_KEY or ACCESS_TOKEN. Provide via .env or CLI.")
    config = InstrumentMasterConfig(
        api_key=args.api_key,
        access_token=args.access_token,
        output_path=args.output_path,
    )

    df = fetch_instrument_master(config)
    save_instrument_master(df, config.output_path)
    logging.info("Saved instrument master with %s rows to %s", len(df), config.output_path)


if __name__ == "__main__":
    main()
