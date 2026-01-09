# -*- coding: utf-8 -*-
"""
Model Utility Module

Provides functions for loading, predicting, evaluating, and training models.
Refactored to support reproducible experiments via CLI.
"""
import joblib
import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

from sklearn.base import BaseEstimator
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error

from src.config import MODEL_PATH


@st.cache_resource
def load_trained_model(model_path: Optional[Path] = None) -> Optional[BaseEstimator]:
    """
    Load a trained model from disk.
    
    Args:
        model_path: Path to model file. Defaults to MODEL_PATH.
        
    Returns:
        Trained model object or None if file not found.
    """
    path = model_path or MODEL_PATH
    if path.exists():
        return joblib.load(path)
    return None


def predict_price(model: BaseEstimator, features: List[float]) -> float:
    """
    Predict price for a given set of features.
    
    Args:
        model: Trained model object
        features: List of feature values (e.g., [area])
        
    Returns:
        Predicted price (10K KRW)
    """
    # Reshape for single sample prediction
    feature_array = np.array(features).reshape(1, -1)
    prediction = model.predict(feature_array)[0]
    return prediction


def calculate_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """
    Calculate regression performance metrics.
    
    Args:
        y_true: Ground truth values
        y_pred: Predicted values
        
    Returns:
        Dictionary containing RMSE, MAE, and MAPE
    """
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)
    
    # Avoid division by zero for MAPE
    with np.errstate(divide='ignore', invalid='ignore'):
        mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
        if np.isinf(mape) or np.isnan(mape):
            mape = 0.0
    
    return {
        "rmse": rmse,
        "mae": mae,
        "mape": mape
    }


def get_model_info(model: BaseEstimator) -> Dict[str, Any]:
    """
    Extract basic model information.
    
    Args:
        model: Trained model object
        
    Returns:
        Dictionary with model type and parameters (if applicable)
    """
    info = {"model_type": type(model).__name__}
    
    if hasattr(model, 'coef_'):
        info["coefficient"] = model.coef_[0]
    if hasattr(model, 'intercept_'):
        info["intercept"] = model.intercept_
            
    return info


def train_linear_model(X_train: np.ndarray, y_train: np.ndarray) -> LinearRegression:
    """
    Train a simple Linear Regression model.
    
    Args:
        X_train: Training features
        y_train: Training targets
        
    Returns:
        Trained LinearRegression model
    """
    model = LinearRegression()
    model.fit(X_train, y_train)
    return model
