# -*- coding: utf-8 -*-
"""
Utility Functions Module

Common functions for RMSE calculation, visualization settings, logging, etc.
"""
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from typing import Union, List
from pathlib import Path

from src.config import FONT_PATH


def calculate_rmse(
    y_true: Union[np.ndarray, List[float]],
    y_pred: Union[np.ndarray, List[float]]
) -> float:
    """
    Calculate RMSE (Root Mean Squared Error).
    
    Args:
        y_true: Array of actual values
        y_pred: Array of predicted values
    
    Returns:
        RMSE value
    
    Example:
        >>> rmse = calculate_rmse([100, 200, 300], [110, 190, 310])
        >>> print(f"RMSE: {rmse:.2f}")
    """
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    
    mse = np.mean((y_true - y_pred) ** 2)
    rmse = np.sqrt(mse)
    
    return rmse


def setup_korean_font() -> None:
    """
    Configure matplotlib to use Korean fonts.
    
    Note:
        Uses NanumGothic font in Linux environment.
        Falls back to default font if not available.
    """
    try:
        if Path(FONT_PATH).exists():
            font_prop = fm.FontProperties(fname=FONT_PATH)
            plt.rcParams['font.family'] = font_prop.get_name()
            plt.rcParams['axes.unicode_minus'] = False
            print(f"[Font Setup] Korean font applied: {FONT_PATH}")
        else:
            print(f"[Font Setup] Font file not found, using default")
    except Exception as e:
        print(f"[Font Setup] Error: {e}")


def format_number(value: float) -> str:
    """
    Format number for readability.
    
    Args:
        value: Number to format
    
    Returns:
        Formatted string
    
    Example:
        >>> format_number(1234567.89)
        '1,234,567.89'
    """
    return f"{value:,.2f}"


def ensure_dir(path: Path) -> None:
    """
    Create directory if it doesn't exist.
    
    Args:
        path: Directory path to create
    """
    path.mkdir(parents=True, exist_ok=True)
