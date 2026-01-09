import pytest
import numpy as np
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.model import calculate_metrics, get_model_info, train_linear_model, predict_price
from src.utils import ensure_dir

# --- Test src/model.py ---

def test_calculate_metrics():
    y_true = np.array([100, 200, 300])
    y_pred = np.array([110, 190, 300])
    
    metrics = calculate_metrics(y_true, y_pred)
    
    # MAE: (|10| + |-10| + |0|) / 3 = 20 / 3 = 6.66...
    assert metrics['mae'] == pytest.approx(6.666, 0.01)
    # MAPE: (|0.1| + |0.05| + 0) / 3 * 100 = 5%
    assert metrics['mape'] == pytest.approx(5.0, 0.1)

def test_train_linear_model():
    X = np.array([[1], [2], [3]])
    y = np.array([2, 4, 6])  # y = 2x
    
    model = train_linear_model(X, y)
    
    assert model.coef_[0] == pytest.approx(2.0, 0.01)
    assert model.intercept_ == pytest.approx(0.0, 0.01)

def test_predict_price():
    mock_model = MagicMock()
    mock_model.predict.return_value = [500]
    
    price = predict_price(mock_model, [100])
    assert price == 500
    mock_model.predict.assert_called_once()


# --- Test src/utils.py ---

def test_ensure_dir(tmp_path):
    target_dir = tmp_path / "subdir"
    assert not target_dir.exists()
    
    ensure_dir(target_dir)
    assert target_dir.exists()
