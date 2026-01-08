# -*- coding: utf-8 -*-
"""
Step 1: Simple Linear Regression Training Script

Trains a Linear Regression model using only 'Exclusive Area' as feature
to predict apartment transaction prices.

Usage:
    python train.py

Output:
    - models/linear_area_model.pkl: Trained model
    - output/figures/correlation_scatter.png: Correlation scatter plot
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import joblib
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

from src.config import (
    TRAIN_PATH, FEATURE_COLS, TARGET_COL,
    MODEL_PATH, TEST_SIZE, RANDOM_STATE
)
from src.data_loader import load_train_data
from src.utils import calculate_rmse, setup_korean_font, ensure_dir


def visualize_correlation(
    df: pd.DataFrame,
    feature_col: str,
    target_col: str,
    save_path: Path = None
) -> None:
    """
    Visualize correlation between feature and target with scatter plot.
    
    Args:
        df: DataFrame
        feature_col: Feature column name
        target_col: Target column name
        save_path: Path to save the plot (None = don't save)
    """
    setup_korean_font()
    correlation = df[feature_col].corr(df[target_col])
    
    sample_size = min(10000, len(df))
    sample_df = df.sample(n=sample_size, random_state=RANDOM_STATE)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(
        sample_df[feature_col], sample_df[target_col],
        alpha=0.3, s=10, c='steelblue'
    )
    
    z = np.polyfit(sample_df[feature_col], sample_df[target_col], 1)
    p = np.poly1d(z)
    x_line = np.linspace(sample_df[feature_col].min(), sample_df[feature_col].max(), 100)
    ax.plot(x_line, p(x_line), 'r-', linewidth=2, label='Regression Line')
    
    ax.set_xlabel('Exclusive Area (m²)', fontsize=12)
    ax.set_ylabel('Price (10K KRW)', fontsize=12)
    ax.set_title(f'Area vs Price (Correlation: {correlation:.4f})', fontsize=14)
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    if save_path:
        ensure_dir(save_path.parent)
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"[Visualization] Scatter plot saved: {save_path}")
    plt.close()


def train_model(X_train: np.ndarray, y_train: np.ndarray) -> LinearRegression:
    """
    Train Linear Regression model.
    
    Args:
        X_train: Training features
        y_train: Training target
    
    Returns:
        Trained model
    """
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    print(f"\n[Model Training Complete]")
    print(f"  - Coefficient: {model.coef_[0]:,.2f}")
    print(f"  - Intercept: {model.intercept_:,.2f}")
    print(f"  - Interpretation: +1m² area → +{model.coef_[0]:,.0f} (10K KRW)")
    
    return model


def evaluate_model(model: LinearRegression, X_val: np.ndarray, y_val: np.ndarray) -> float:
    """
    Evaluate model performance.
    
    Args:
        model: Trained model
        X_val: Validation features
        y_val: Validation target
    
    Returns:
        RMSE value
    """
    y_pred = model.predict(X_val)
    rmse = calculate_rmse(y_val, y_pred)
    
    print(f"\n[Model Evaluation]")
    print(f"  - Validation size: {len(y_val):,}")
    print(f"  - RMSE: {rmse:,.2f}")
    print(f"  - Mean price: {y_val.mean():,.2f}")
    print(f"  - RMSE / Mean: {rmse / y_val.mean() * 100:.2f}%")
    
    return rmse


def save_model(model: LinearRegression, path: Path) -> None:
    """Save trained model."""
    ensure_dir(path.parent)
    joblib.dump(model, path)
    print(f"\n[Model Saved] {path}")


def main() -> None:
    """
    Main execution function.
    
    Pipeline:
    1. Load data
    2. Visualize correlation
    3. Train/Validation split
    4. Train model
    5. Evaluate model
    6. Save model
    """
    print("=" * 60)
    print("Step 1: Simple Linear Regression Training")
    print("=" * 60)
    
    print("\n[1/6] Loading data")
    feature_col = FEATURE_COLS[0]
    train_df = load_train_data(usecols=[feature_col, TARGET_COL])
    
    n_missing = train_df.isnull().sum().sum()
    if n_missing > 0:
        print(f"  - Removing {n_missing} missing values")
        train_df = train_df.dropna()
    
    print("\n[2/6] Visualizing correlation")
    figure_path = PROJECT_ROOT / "output" / "figures" / "correlation_scatter.png"
    visualize_correlation(train_df, feature_col, TARGET_COL, figure_path)
    
    print("\n[3/6] Splitting data")
    X = train_df[[feature_col]].values
    y = train_df[TARGET_COL].values
    
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )
    print(f"  - Training: {len(X_train):,}")
    print(f"  - Validation: {len(X_val):,}")
    
    print("\n[4/6] Training model")
    model = train_model(X_train, y_train)
    
    print("\n[5/6] Evaluating model")
    rmse = evaluate_model(model, X_val, y_val)
    
    print("\n[6/6] Saving model")
    save_model(model, MODEL_PATH)
    
    print("\n" + "=" * 60)
    print("Training Complete!")
    print(f"  - Feature: {feature_col}")
    print(f"  - Final RMSE: {rmse:,.2f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
