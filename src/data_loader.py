# -*- coding: utf-8 -*-
"""
Data Loading Module

Functions to load train.csv, test.csv data and display basic information.
"""
import pandas as pd
from typing import Optional, List, Tuple

from src.config import TRAIN_PATH, TEST_PATH, SAMPLE_SUBMISSION_PATH


def load_train_data(
    path: Optional[str] = None,
    usecols: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Load training data.
    
    Args:
        path: CSV file path. Uses default if None.
        usecols: List of columns to load. Loads all if None.
    
    Returns:
        Training DataFrame
    
    Example:
        >>> df = load_train_data()
        >>> df = load_train_data(usecols=['전용면적(㎡)', 'target'])
    """
    file_path = path if path else TRAIN_PATH
    df = pd.read_csv(file_path, usecols=usecols)
    
    print(f"[Data Load] Training data: {len(df):,} rows x {len(df.columns)} cols")
    return df


def load_test_data(
    path: Optional[str] = None,
    usecols: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Load test data.
    
    Args:
        path: CSV file path. Uses default if None.
        usecols: List of columns to load. Loads all if None.
    
    Returns:
        Test DataFrame
    
    Example:
        >>> df = load_test_data()
        >>> df = load_test_data(usecols=['전용면적(㎡)'])
    """
    file_path = path if path else TEST_PATH
    df = pd.read_csv(file_path, usecols=usecols)
    
    print(f"[Data Load] Test data: {len(df):,} rows x {len(df.columns)} cols")
    return df


def load_sample_submission(path: Optional[str] = None) -> pd.DataFrame:
    """
    Load sample submission file.
    
    Args:
        path: CSV file path. Uses default if None.
    
    Returns:
        Sample submission DataFrame
    """
    file_path = path if path else SAMPLE_SUBMISSION_PATH
    df = pd.read_csv(file_path)
    
    print(f"[Data Load] Sample submission: {len(df):,} rows")
    return df


def get_data_summary(df: pd.DataFrame) -> Tuple[int, int, int]:
    """
    Get basic DataFrame information.
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        Tuple of (row count, column count, missing value count)
    """
    n_rows = len(df)
    n_cols = len(df.columns)
    n_missing = df.isnull().sum().sum()
    
    return n_rows, n_cols, n_missing
