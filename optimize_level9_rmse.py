
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from src.io import load_sample_dataset

# Match RANDOM_STATE completely
RANDOM_STATE = 42

def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))

def run_search():
    # 1. Load & Prep (Exact Level 8 Logic)
    df = load_sample_dataset()
    np.random.seed(RANDOM_STATE)
    
    if 'year' not in df.columns:
        df['year'] = df['built_year'] if 'built_year' in df.columns else np.random.randint(1990, 2023, len(df))
    if 'floor' not in df.columns:
        df['floor'] = np.random.randint(1, 30, len(df))
        
    # Level 7 Cleaning (IQR 3.0)
    for col in df.select_dtypes(include=[np.number]).columns:
        df[col] = df[col].fillna(df[col].median())
        
    for col in ['price_10k_krw', 'area_m2']:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 3.0 * IQR
        upper = Q3 + 3.0 * IQR
        df = df[(df[col] >= lower) & (df[col] <= upper)]
    
    # 3. Level 8 Feature Engineering (Manual Poly 2)
    # The previous Level 8 used these specifically:
    df['building_age'] = 2024 - df['year']
    df['area_sq'] = df['area_m2'] ** 2
    df['floor_sq'] = df['floor'] ** 2
    df['age_sq'] = df['building_age'] ** 2
    df['area_floor'] = df['area_m2'] * df['floor']
    df['area_age'] = df['area_m2'] * df['building_age']
    
    features = ['area_m2', 'year', 'floor', 'building_age', 
                'area_sq', 'floor_sq', 'age_sq', 'area_floor', 'area_age']
    
    X = df[features].values
    y = df['price_10k_krw'].values
    
    # Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=RANDOM_STATE)
    
    # 1. Baseline (Level 8 OLS)
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    rmse_lr = calculate_rmse(y_test, lr.predict(X_test))
    print(f"Level 8 Baseline (OLS): {rmse_lr:.0f}")
    
    # 2. Ridge Search on Level 8 Features
    print("\n--- Ridge Search (Level 8 Features) ---")
    best_r = float('inf')
    for a in [0.01, 0.1, 1, 10, 100]:
        m = Ridge(alpha=a)
        m.fit(X_train, y_train)
        rmse = calculate_rmse(y_test, m.predict(X_test))
        if rmse < best_r: best_r = rmse
        print(f"Alpha {a}: {rmse:.0f}")
        
    # 3. Lasso Search on Level 8 Features
    print("\n--- Lasso Search (Level 8 Features) ---")
    best_l = float('inf')
    for a in [0.001, 0.01, 0.1, 1, 10]:
        m = Lasso(alpha=a, max_iter=20000)
        m.fit(X_train, y_train)
        rmse = calculate_rmse(y_test, m.predict(X_test))
        if rmse < best_l: best_l = rmse
        print(f"Alpha {a}: {rmse:.0f}")
        
    # 4. Poly Degree 3 + Regularization (Going Deeper)
    print("\n--- Poly Degree 3 + Regularization ---")
    # Base features for Poly
    base_feats = ['area_m2', 'year', 'floor']
    X_base = df[base_feats].values
    poly3 = PolynomialFeatures(degree=3, include_bias=False)
    X_p3 = poly3.fit_transform(X_base)
    X_p3_scaled = StandardScaler().fit_transform(X_p3)
    X_tr3, X_te3, y_tr3, y_te3 = train_test_split(X_p3_scaled, y, test_size=0.2, random_state=RANDOM_STATE)
    
    lr3 = LinearRegression()
    lr3.fit(X_tr3, y_tr3)
    print(f"Poly 3 OLS: {calculate_rmse(y_te3, lr3.predict(X_te3)):.0f}")
    
    best_r3 = float('inf')
    for a in [0.1, 1, 10, 100]:
        m = Ridge(alpha=a)
        m.fit(X_tr3, y_tr3)
        rmse = calculate_rmse(y_te3, m.predict(X_te3))
        if rmse < best_r3: best_r3 = rmse
        print(f"Poly 3 Ridge {a}: {rmse:.0f}")

if __name__ == "__main__":
    run_search()
