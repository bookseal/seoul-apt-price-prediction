# -*- coding: utf-8 -*-
"""
Unified Experiment Runner CLI

This script standardizes the model training process across different levels.
Currently supports:
- Level 2: Linear Regression (Single Feature)

Usage:
    python run_experiment.py --level 2
"""
import argparse
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from sklearn.model_selection import train_test_split

# Import from src
from src.config import (
    FEATURE_COLS, TARGET_COL, TEST_SIZE, RANDOM_STATE, MODEL_PATH
)
from src.data_loader import load_train_data
from src.model import train_linear_model, calculate_metrics, get_model_info
from src.utils import save_object, ensure_dir

def run_level_2(args):
    """Run experiment for Level 2 (Linear Regression)."""
    print(f"\n[Experiment] Level 2: Linear Regression")
    print("-" * 50)
    
    # 1. Load Data
    feature_col = FEATURE_COLS[0]  # Use 'Exclusive Area'
    print(f"Loading data with feature: {feature_col}")
    df = load_train_data(usecols=[feature_col, TARGET_COL])
    
    if args.limit:
        df = df.head(args.limit)
        print(f"  - Limited to {args.limit} rows for testing")
        
    df = df.dropna()
    
    # 2. Split Data
    X = df[[feature_col]].values
    y = df[TARGET_COL].values
    
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )
    print(f"Data Split: Train={len(X_train):,}, Val={len(X_val):,}")
    
    # 3. Train Model
    print("Training model...")
    model = train_linear_model(X_train, y_train)
    
    # 4. Evaluate
    y_pred = model.predict(X_val)
    metrics = calculate_metrics(y_val, y_pred)
    print(f"Evaluation Results:")
    print(f"  - RMSE: {metrics['rmse']:.2f}")
    print(f"  - MAE:  {metrics['mae']:.2f}")
    print(f"  - MAPE: {metrics['mape']:.2f}%")
    
    info = get_model_info(model)
    print(f"Model Info: {info}")
    
    # 5. Save (Optional)
    if args.save:
        save_path = PROJECT_ROOT / "models" / f"level_2_model.pkl"
        ensure_dir(save_path.parent)
        save_object(model, save_path)
        print(f"Model saved to: {save_path}")

def main():
    parser = argparse.ArgumentParser(description="Seoul Apt Price Prediction Experiment Runner")
    parser.add_argument("--level", type=int, required=True, help="Experiment Level (e.g., 2)")
    parser.add_argument("--save", action="store_true", help="Save the trained model")
    parser.add_argument("--limit", type=int, help="Limit dataset size for quick testing")
    
    args = parser.parse_args()
    
    if args.level == 2:
        run_level_2(args)
    else:
        print(f"Level {args.level} is not yet implemented in this CLI.")
        sys.exit(1)

if __name__ == "__main__":
    main()
