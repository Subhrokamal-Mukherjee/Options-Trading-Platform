"""Entry-point for explicit phase-wise pipeline execution.

Currently supports Phase 1 orchestration only.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

from ingestion.validator import ValidationConfig, validate_phase1


def parse_args() -> argparse.Namespace:
    """Parse top-level CLI arguments."""
    parser = argparse.ArgumentParser(description="Weekly options volatility system pipeline")
    parser.add_argument("--phase", type=int, required=True, choices=[1])
    parser.add_argument("--raw-root", default=BASE_DIR / "data" / "raw", type=Path)
    parser.add_argument("--verified-root", default=BASE_DIR / "data" / "raw" / "verified", type=Path)
    parser.add_argument("--metadata-root", default=BASE_DIR / "data" / "metadata", type=Path)
    return parser.parse_args()


def main() -> None:
    """Execute selected phase."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()

    if args.phase == 1:
        report = validate_phase1(
            ValidationConfig(
                raw_root=args.raw_root,
                verified_root=args.verified_root,
                metadata_root=args.metadata_root,
            )
        )
        logging.info("Phase 1 complete: %s", report)


if __name__ == "__main__":
    main()
