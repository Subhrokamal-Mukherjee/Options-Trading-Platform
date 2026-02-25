"""Phase 2 deterministic ATM reconstruction from NSE UDiFF daily options data.

Builds one ATM straddle row per (TRADE_DATE, SYMBOL) using strict deterministic
selection rules and writes monthly parquet artifacts under data/processed.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

REQUIRED_COLUMNS: set[str] = {
    "FinInstrmTp",
    "TckrSymb",
    "XpryDt",
    "StrkPric",
    "OptnTp",
    "ClsPric",
    "UndrlygPric",
    "TradDt",
}


@dataclass(frozen=True)
class ATMReconstructionConfig:
    """Configuration for ATM reconstruction job."""

    input_path: Path
    output_dir: Path
    min_dte: int = 3


def _normalize_udiff_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw UDiFF schema and enforce expected dtypes."""
    frame = df.copy()
    frame.columns = frame.columns.str.strip()

    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"UDiFF input missing required columns: {sorted(missing)}")

    frame = frame.loc[
        (frame["FinInstrmTp"] == "IDO")
        & (frame["TckrSymb"].isin(["NIFTY", "BANKNIFTY"]))
        & (frame["OptnTp"].isin(["CE", "PE"]))
    ].copy()

    frame["TradDt"] = pd.to_datetime(frame["TradDt"], errors="coerce").dt.normalize()
    frame["XpryDt"] = pd.to_datetime(frame["XpryDt"], errors="coerce").dt.normalize()
    frame["StrkPric"] = pd.to_numeric(frame["StrkPric"], errors="coerce")
    frame["ClsPric"] = pd.to_numeric(frame["ClsPric"], errors="coerce")
    frame["UndrlygPric"] = pd.to_numeric(frame["UndrlygPric"], errors="coerce")

    frame = frame.dropna(subset=["TradDt", "XpryDt", "StrkPric", "ClsPric", "UndrlygPric"])

    if frame.empty:
        raise ValueError("No valid IDO CE/PE rows found after schema normalization")

    return frame.reset_index(drop=True)


def select_atm_for_day(day_df: pd.DataFrame, min_dte: int = 3) -> dict[str, object] | None:
    """Select deterministic ATM CE/PE pair for one symbol on one trade date.

    Rules:
    - Choose minimum expiry where DTE >= min_dte.
    - Choose strike minimizing abs(strike - spot), tie broken toward lower strike.
    - Require both CE and PE for selected expiry/strike; otherwise skip.
    """
    if day_df.empty:
        return None

    trade_date = pd.Timestamp(day_df["TradDt"].iloc[0]).normalize()
    symbol = str(day_df["TckrSymb"].iloc[0])

    candidate = day_df.copy()
    candidate["DTE"] = (candidate["XpryDt"] - trade_date).dt.days
    candidate = candidate.loc[candidate["DTE"] >= min_dte].copy()

    if candidate.empty:
        return None

    min_dte_value = int(candidate["DTE"].min())
    expiry = pd.Timestamp(candidate.loc[candidate["DTE"] == min_dte_value, "XpryDt"].min()).normalize()
    expiry_df = candidate.loc[candidate["XpryDt"] == expiry].copy()

    if expiry_df.empty:
        return None

    spot = float(expiry_df["UndrlygPric"].iloc[0])
    if pd.isna(spot) or spot <= 0:
        return None

    expiry_df["strike_distance"] = (expiry_df["StrkPric"] - spot).abs()
    min_distance = float(expiry_df["strike_distance"].min())
    atm_strike = float(expiry_df.loc[expiry_df["strike_distance"] == min_distance, "StrkPric"].min())

    atm_rows = expiry_df.loc[expiry_df["StrkPric"] == atm_strike].copy()
    ce_rows = atm_rows.loc[atm_rows["OptnTp"] == "CE"]
    pe_rows = atm_rows.loc[atm_rows["OptnTp"] == "PE"]

    if ce_rows.empty or pe_rows.empty:
        return None

    if len(ce_rows) > 1 or len(pe_rows) > 1:
        return None

    ce_price = float(ce_rows["ClsPric"].iloc[0])
    pe_price = float(pe_rows["ClsPric"].iloc[0])

    if pd.isna(ce_price) or pd.isna(pe_price):
        return None

    return {
        "TRADE_DATE": trade_date,
        "SYMBOL": symbol,
        "EXPIRY": expiry,
        "DTE": min_dte_value,
        "SPOT": spot,
        "ATM_STRIKE": atm_strike,
        "CE_PRICE": ce_price,
        "PE_PRICE": pe_price,
        "STRADDLE_PRICE": ce_price + pe_price,
    }


def build_atm_timeseries(df: pd.DataFrame, min_dte: int = 3) -> pd.DataFrame:
    """Build deterministic ATM timeseries with one row per symbol per trade date."""
    normalized = _normalize_udiff_schema(df)

    rows: list[dict[str, object]] = []
    for (trade_date, symbol), group in normalized.groupby(["TradDt", "TckrSymb"], sort=True):
        selected = select_atm_for_day(group, min_dte=min_dte)
        if selected is None:
            logging.getLogger(__name__).warning(
                "Skipping ATM row | trade_date=%s | symbol=%s | reason=selection_failed",
                trade_date.date(),
                symbol,
            )
            continue
        rows.append(selected)

    output_columns = [
        "TRADE_DATE",
        "SYMBOL",
        "EXPIRY",
        "DTE",
        "SPOT",
        "ATM_STRIKE",
        "CE_PRICE",
        "PE_PRICE",
        "STRADDLE_PRICE",
    ]

    result = pd.DataFrame(rows, columns=output_columns)
    if result.empty:
        raise ValueError("ATM reconstruction produced zero rows")

    result = result.sort_values(["TRADE_DATE", "SYMBOL"]).reset_index(drop=True)

    dup_count = int(result.duplicated(subset=["TRADE_DATE", "SYMBOL"]).sum())
    if dup_count > 0:
        raise RuntimeError(f"ATM reconstruction duplicate key rows found: {dup_count}")

    return result


def _resolve_output_path(output_dir: Path, atm_df: pd.DataFrame) -> Path:
    """Resolve monthly parquet output path from reconstructed ATM dataframe."""
    months = atm_df["TRADE_DATE"].dt.to_period("M").unique()
    if len(months) != 1:
        raise ValueError(
            "ATM output expects a single calendar month input; found months="
            f"{[str(m) for m in months]}"
        )

    period = months[0]
    return output_dir / f"atm_daily_{period.year}_{period.month:02d}.parquet"


def run_reconstruction(config: ATMReconstructionConfig) -> Path:
    """Execute Phase 2 ATM reconstruction and persist monthly parquet."""
    source_df = pd.read_parquet(config.input_path)
    atm_df = build_atm_timeseries(source_df, min_dte=config.min_dte)

    output_path = _resolve_output_path(config.output_dir, atm_df)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    atm_df.to_parquet(output_path, index=False)

    logger = logging.getLogger(__name__)
    logger.info("ATM reconstruction rows=%s", len(atm_df))
    logger.info("ATM unique_trade_dates=%s", atm_df["TRADE_DATE"].nunique())
    logger.info("ATM unique_expiries=%s", atm_df["EXPIRY"].nunique())
    logger.info("ATM dte_min=%s dte_max=%s", int(atm_df["DTE"].min()), int(atm_df["DTE"].max()))

    return output_path


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Phase 2 deterministic ATM reconstruction")
    parser.add_argument(
        "--input-path",
        type=Path,
        default=BASE_DIR / "data" / "processed" / "nifty_banknifty_jan_2024.parquet",
        help="Consolidated UDiFF parquet path",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BASE_DIR / "data" / "processed",
        help="Output directory for atm_daily_YYYY_MM.parquet",
    )
    parser.add_argument("--min-dte", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()

    output_path = run_reconstruction(
        ATMReconstructionConfig(
            input_path=args.input_path,
            output_dir=args.output_dir,
            min_dte=args.min_dte,
        )
    )
    logging.info("Saved ATM reconstruction to %s", output_path)


if __name__ == "__main__":
    main()
