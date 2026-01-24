
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from src.io import load_sample_dataset

RANDOM_STATE = 42

def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))

def train_level8():
    df = load_sample_dataset()
    
    # 0. Prep (Match Level 8 main)
    np.random.seed(RANDOM_STATE)
    if 'year' not in df.columns:
        df['year'] = df['built_year'] if 'built_year' in df.columns else np.random.randint(1985, 2024, len(df))
    if 'floor' not in df.columns:
        df['floor'] = np.random.randint(1, 30, len(df))
        
    # 1. Cleaning (IQR 3.0 MATCHING LEVEL 7)
    df_clean = df.copy()
    for col in df_clean.select_dtypes(include=[np.number]).columns:
        df_clean[col] = df_clean[col].fillna(df_clean[col].median())
        
    for col in ['price_10k_krw', 'area_m2']:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 3.0 * IQR
        upper = Q3 + 3.0 * IQR
        df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]
        
    # 2. Features
    df_clean['building_age'] = 2024 - df_clean['year']
    
    X_base = df_clean[['area_m2', 'year', 'floor']].values
    y = df_clean['price_10k_krw'].values
    
    X_train, X_test, y_train, y_test = train_test_split(X_base, y, test_size=0.2, random_state=RANDOM_STATE)
    
    # Baseline
    m_base = LinearRegression()
    m_base.fit(X_train, y_train)
    rmse_base = calculate_rmse(y_test, m_base.predict(X_test))
    print(f"L7 Baseline RMSE: {rmse_base:.0f}")
    
    # Log Target
    m_log = LinearRegression()
    m_log.fit(X_train, np.log1p(y_train))
    y_pred_log = np.expm1(m_log.predict(X_test))
    rmse_log = calculate_rmse(y_test, y_pred_log)
    print(f"Log Target RMSE: {rmse_log:.0f}")
    
    # Polynomial (d=2)
    poly = PolynomialFeatures(degree=2, include_bias=False)
    X_train_poly = poly.fit_transform(X_train)
    X_test_poly = poly.transform(X_test)
    
    m_poly = LinearRegression()
    m_poly.fit(X_train_poly, y_train)
    rmse_poly = calculate_rmse(y_test, m_poly.predict(X_test_poly))
    print(f"Polynomial RMSE: {rmse_poly:.0f}")

if __name__ == "__main__":
    train_level8()
