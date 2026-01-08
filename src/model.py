# -*- coding: utf-8 -*-
"""
모델 유틸리티 모듈

학습된 모델의 로드, 예측, 평가를 위한 함수들을 제공합니다.
"""
import joblib
import numpy as np
import streamlit as st
from pathlib import Path
from typing import Optional, Tuple

from src.config import MODEL_PATH


@st.cache_resource
def load_trained_model(model_path: Optional[Path] = None):
    """
    학습된 모델을 로드합니다.
    
    Args:
        model_path: 모델 파일 경로. None이면 기본 경로 사용.
        
    Returns:
        학습된 모델 객체 또는 None (파일이 없는 경우)
    """
    path = model_path or MODEL_PATH
    if path.exists():
        return joblib.load(path)
    return None


def predict_price(model, area: float) -> float:
    """
    전용면적을 입력받아 가격을 예측합니다.
    
    Args:
        model: 학습된 모델 객체
        area: 전용면적 (㎡)
        
    Returns:
        예측 가격 (만원)
    """
    prediction = model.predict([[area]])[0]
    return prediction


def calculate_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """
    예측 성능 지표를 계산합니다.
    
    Args:
        y_true: 실제 값 배열
        y_pred: 예측 값 배열
        
    Returns:
        RMSE, MAE, MAPE를 포함한 딕셔너리
    """
    rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))
    mae = np.mean(np.abs(y_true - y_pred))
    mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
    
    return {
        "rmse": rmse,
        "mae": mae,
        "mape": mape
    }


def get_model_info(model) -> dict:
    """
    모델의 기본 정보를 추출합니다.
    
    Args:
        model: 학습된 모델 객체 (Linear Regression)
        
    Returns:
        모델 정보 딕셔너리 (계수, 절편 등)
    """
    return {
        "model_type": type(model).__name__,
        "coefficient": model.coef_[0] if hasattr(model, 'coef_') else None,
        "intercept": model.intercept_ if hasattr(model, 'intercept_') else None
    }
