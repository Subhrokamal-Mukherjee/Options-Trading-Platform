from pathlib import Path

# -------------------------------------------------------
# BASE DIRECTORY
# -------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

# -------------------------------------------------------
# FOLDER STRUCTURE DEFINITION
# -------------------------------------------------------

FOLDERS = [
    # Config
    "config",

    # Data
    "data/raw/underlying",
    "data/raw/options",
    "data/processed/aligned",
    "data/processed/atm_reconstructed",
    "data/processed/features",
    "data/metadata",

    # Core modules
    "ingestion",
    "feature_engineering",
    "targets",
    "models",
    "backtest",
    "risk",
    "execution",

    # Logs
    "logs"
]

# -------------------------------------------------------
# FILE TEMPLATES (EMPTY PLACEHOLDERS)
# -------------------------------------------------------

FILES = [
    "main.py",

    # Ingestion
    "ingestion/instrument_master.py",
    "ingestion/fetch_underlying.py",
    "ingestion/fetch_options.py",
    "ingestion/validator.py",

    # Feature Engineering
    "feature_engineering/build_features.py",

    # Targets
    "targets/realized_vol.py",

    # Models
    "models/xgb_regressor.py",
    "models/xgb_classifier.py",
    "models/walkforward.py",

    # Backtest
    "backtest/engine.py",
    "backtest/pnl.py",
    "backtest/metrics.py",

    # Risk
    "risk/risk_engine.py",

    # Execution
    "execution/paper_trade.py",

    # Config
    "config/instruments.yaml",
    "config/paths.yaml",
    "config/trading_params.yaml"
]

# -------------------------------------------------------
# CREATE DIRECTORIES
# -------------------------------------------------------

for folder in FOLDERS:
    path = BASE_DIR / folder
    path.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------
# CREATE EMPTY FILES
# -------------------------------------------------------

for file in FILES:
    file_path = BASE_DIR / file
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if not file_path.exists():
        file_path.touch()

print("Project structure successfully created.")
