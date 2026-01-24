
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error
from src.io import load_sample_dataset

# Match RANDOM_STATE
RANDOM_STATE = 42

def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))

def run_optimization():
    # 1. Load & Prep (Level 8 Logic)
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
    
    # Level 8 Features
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
    
    print(f"Baseline (Level 8 Features, No Reg):")
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    rmse_lr = calculate_rmse(y_test, lr.predict(X_test))
    print(f"RMSE: {rmse_lr:.0f}")
    
    print("\nRidge Optimization:")
    best_ridge_rmse = float('inf')
    best_alpha = 0
    for alpha in [0.01, 0.1, 1, 10, 100, 1000]:
        ridge = Ridge(alpha=alpha)
        ridge.fit(X_train, y_train)
        rmse = calculate_rmse(y_test, ridge.predict(X_test))
        if rmse < best_ridge_rmse:
            best_ridge_rmse = rmse
            best_alpha = alpha
        print(f"Alpha {alpha}: {rmse:.0f}")
    print(f"Best Ridge: {best_ridge_rmse:.0f} (Alpha {best_alpha})")
    
    print("\nLasso Optimization:")
    best_lasso_rmse = float('inf')
    best_alpha = 0
    for alpha in [0.01, 0.1, 1, 10, 100]:
        lasso = Lasso(alpha=alpha, max_iter=20000)
        lasso.fit(X_train, y_train)
        rmse = calculate_rmse(y_test, lasso.predict(X_test))
        if rmse < best_lasso_rmse:
            best_lasso_rmse = rmse
            best_alpha = alpha
        print(f"Alpha {alpha}: {rmse:.0f}")
    print(f"Best Lasso: {best_lasso_rmse:.0f} (Alpha {best_alpha})")

if __name__ == "__main__":
    run_optimization()
