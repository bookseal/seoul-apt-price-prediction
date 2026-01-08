# -*- coding: utf-8 -*-
"""
Configuration Module

Centralized management of paths, column names, and hyperparameters.
Avoid hardcoding by referencing settings from this file.
"""
from pathlib import Path
from typing import List


# =============================================================================
# Path Settings
# =============================================================================
# Project root directory (2 levels up from this file)
PROJECT_ROOT: Path = Path(__file__).parent.parent

# Data paths
DATA_DIR: Path = PROJECT_ROOT / "data" / "raw"
TRAIN_PATH: Path = DATA_DIR / "train.csv"
TEST_PATH: Path = DATA_DIR / "test.csv"
SAMPLE_SUBMISSION_PATH: Path = DATA_DIR / "sample_submission.csv"

# Sample data path (Parquet)
SAMPLE_PARQUET_PATH: Path = PROJECT_ROOT / "data" / "sample.parquet"

# Model save path
MODELS_DIR: Path = PROJECT_ROOT / "models"
MODEL_PATH: Path = MODELS_DIR / "linear_area_model.pkl"

# Output paths
OUTPUT_DIR: Path = PROJECT_ROOT / "output"
SUBMISSION_PATH: Path = OUTPUT_DIR / "submission.csv"

# Log paths
LOGS_DIR: Path = PROJECT_ROOT / "logs"
EXPERIMENT_LOG_PATH: Path = LOGS_DIR / "experiment_log.csv"


# =============================================================================
# Column Settings
# =============================================================================
# Feature columns for Level 2 (original Korean column names)
FEATURE_COLS: List[str] = ["전용면적(㎡)"]

# Target column (prediction target)
TARGET_COL: str = "target"

# Sample data English column mapping (for Level 1)
SAMPLE_COLS: dict = {
    "district": "District",
    "area_m2": "Exclusive Area",
    "price_10k_krw": "Price",
    "year": "Contract Year",
    "floor": "Floor"
}


# =============================================================================
# Model Hyperparameters
# =============================================================================
# Train/Validation split ratio
TEST_SIZE: float = 0.2

# Random seed (for reproducibility)
RANDOM_STATE: int = 42


# =============================================================================
# Visualization Settings
# =============================================================================
# Korean font path (Linux environment)
FONT_PATH: str = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"

# Figure save path
FIGURES_DIR: Path = PROJECT_ROOT / "output" / "figures"
