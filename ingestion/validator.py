"""Phase 1 data validation checks for 5-minute ingestion artifacts.

Validation is fail-loud and blocks downstream phases upon data integrity violations.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

import pandas as pd


@dataclass(frozen=True)
class ValidationConfig:
    """Validation paths and constraints."""

    raw_root: Path
    verified_root: Path
    metadata_root: Path


class DataValidationError(RuntimeError):
    """Raised when Phase 1 validation fails."""


def _load_parquet_tree(path: Path) -> pd.DataFrame:
    """Load partitioned parquet tree into a dataframe."""
    if not path.exists():
        raise DataValidationError(f"Missing dataset path: {path}")
    df = pd.read_parquet(path)
    if df.empty:
        raise DataValidationError(f"Dataset is empty: {path}")
    return df


def _check_duplicates(df: pd.DataFrame, keys: list[str], dataset: str) -> None:
    dup_count = int(df.duplicated(keys).sum())
    if dup_count > 0:
        raise DataValidationError(f"{dataset} has {dup_count} duplicate rows on keys={keys}")


def _check_trading_hours(df: pd.DataFrame, dataset: str) -> None:
    ts = pd.to_datetime(df["date"], utc=True).dt.tz_convert("Asia/Kolkata")
    t = ts.dt.time
    start = time(9, 15)
    end = time(15, 30)
    bad = df.loc[(t < start) | (t > end)]
    if not bad.empty:
        raise DataValidationError(f"{dataset} contains {len(bad)} rows outside NSE hours 09:15-15:30")


def _check_5min_intervals_underlying(df: pd.DataFrame) -> None:
    ts = pd.to_datetime(df["date"], utc=True).dt.tz_convert("Asia/Kolkata")
    local = df.assign(local_ts=ts).sort_values(["symbol", "local_ts"]).copy()
    local["day"] = local["local_ts"].dt.date

    for (symbol, day), group in local.groupby(["symbol", "day"], sort=False):
        expected = pd.date_range(
            pd.Timestamp.combine(pd.Timestamp(day), time(9, 15), tzinfo=group["local_ts"].iloc[0].tz),
            pd.Timestamp.combine(pd.Timestamp(day), time(15, 30), tzinfo=group["local_ts"].iloc[0].tz),
            freq="5min",
        )
        actual = pd.DatetimeIndex(group["local_ts"].sort_values())
        missing = expected.difference(actual)
        if len(missing) > 0:
            raise DataValidationError(
                f"Underlying missing {len(missing)} 5-minute intervals for {symbol} on {day}"
            )


def _check_expiry_continuity(options_df: pd.DataFrame) -> None:
    payload = options_df.copy()
    payload["expiry"] = pd.to_datetime(payload["expiry"]).dt.date
    for symbol, group in payload.groupby("symbol"):
        expiries = sorted(group["expiry"].dropna().unique())
        if len(expiries) < 2:
            continue
        gaps = [
            (expiries[idx] - expiries[idx - 1]).days
            for idx in range(1, len(expiries))
        ]
        if any(gap > 10 for gap in gaps):
            raise DataValidationError(
                f"Expiry continuity failed for {symbol}; found gap > 10 days in weekly chain"
            )


def _check_strike_increments(options_df: pd.DataFrame) -> None:
    for symbol, step in [("NIFTY", 50), ("BANKNIFTY", 100)]:
        strikes = options_df.loc[options_df["symbol"] == symbol, "strike"]
        if strikes.empty:
            continue
        invalid = strikes[(strikes % step) != 0]
        if not invalid.empty:
            raise DataValidationError(
                f"Strike increment check failed for {symbol}: {len(invalid)} non-{step} strikes"
            )


def _check_no_forward_leakage(df: pd.DataFrame, keys: list[str], dataset: str) -> None:
    payload = df.sort_values(keys).copy()
    grouped = payload.groupby([k for k in keys if k != "date"], dropna=False)
    for group_key, group in grouped:
        dates = pd.to_datetime(group["date"], utc=True)
        if not dates.is_monotonic_increasing:
            raise DataValidationError(
                f"{dataset} chronology violation (potential leakage) in group={group_key}"
            )


def validate_phase1(config: ValidationConfig) -> dict[str, int]:
    """Run Phase 1 integrity checks and persist verified raw datasets."""
    underlying = _load_parquet_tree(config.raw_root / "underlying")
    options = _load_parquet_tree(config.raw_root / "options")

    _check_duplicates(underlying, ["symbol", "date"], "underlying")
    _check_duplicates(options, ["tradingsymbol", "date"], "options")

    _check_trading_hours(underlying, "underlying")
    _check_trading_hours(options, "options")

    _check_5min_intervals_underlying(underlying)
    _check_expiry_continuity(options)
    _check_strike_increments(options)

    _check_no_forward_leakage(underlying, ["symbol", "date"], "underlying")
    _check_no_forward_leakage(options, ["tradingsymbol", "date"], "options")

    verified_underlying = config.verified_root / "underlying"
    verified_options = config.verified_root / "options"
    config.verified_root.mkdir(parents=True, exist_ok=True)
    underlying.to_parquet(verified_underlying, partition_cols=["symbol"], index=False)
    options.to_parquet(verified_options, partition_cols=["symbol", "expiry_partition"], index=False)

    report = {
        "underlying_rows": int(len(underlying)),
        "options_rows": int(len(options)),
        "underlying_symbols": int(underlying["symbol"].nunique()),
        "options_symbols": int(options["symbol"].nunique()),
    }
    config.metadata_root.mkdir(parents=True, exist_ok=True)
    (config.metadata_root / "phase1_validation_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return report


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate Phase 1 raw ingestion artifacts")
    parser.add_argument("--raw-root", default=BASE_DIR / "data" / "raw", type=Path)
    parser.add_argument("--verified-root", default=BASE_DIR / "data" / "raw" / "verified", type=Path)
    parser.add_argument("--metadata-root", default=BASE_DIR / "data" / "metadata", type=Path)
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    report = validate_phase1(
        ValidationConfig(
            raw_root=args.raw_root,
            verified_root=args.verified_root,
            metadata_root=args.metadata_root,
        )
    )
    logging.info("Phase 1 validation passed: %s", report)


if __name__ == "__main__":
    main()
