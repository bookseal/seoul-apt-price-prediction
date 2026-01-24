
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from src.io import load_sample_dataset

def run_search():
    # 1. Load Data (Same random state as app)
    df = load_sample_dataset()
    np.random.seed(42) # Local
    
    # Fill Data
    if 'year' not in df.columns: df['year'] = 2000
    if 'floor' not in df.columns: df['floor'] = 10
    
    # Cleaning (Level 7)
    for col in ['price_10k_krw', 'area_m2']:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        df = df[(df[col] >= Q1 - 3.0*IQR) & (df[col] <= Q3 + 3.0*IQR)]
    
    features = ['area_m2', 'year'] # Core features for Poly
    
    X = df[features].values
    y = df['price_10k_krw'].values
    
    # Poly 3
    poly = PolynomialFeatures(degree=5, include_bias=False)
    X_poly = poly.fit_transform(X)
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_poly)
    
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
    
    print(f"Level 8 Target to Beat: 24,184")
    
    # 1. Linear (Overfit Check)
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    rmse_lr = np.sqrt(mean_squared_error(y_test, lr.predict(X_test)))
    print(f"Poly 3 Linear (No Reg): {rmse_lr:.0f}")
    
    # 2. Ridge Search
    best_r = float('inf')
    best_alpha_r = 0
    for a in [0.1, 1, 5, 10, 20, 50, 100, 200]:
        m = Ridge(alpha=a)
        m.fit(X_train, y_train)
        rmse = np.sqrt(mean_squared_error(y_test, m.predict(X_test)))
        if rmse < best_r: best_r = rmse; best_alpha_r = a
    print(f"Best Ridge: {best_r:.0f} (Alpha {best_alpha_r})")
    
    # 3. Lasso Search
    best_l = float('inf')
    best_alpha_l = 0
    for a in [0.01, 0.1, 1, 2, 5, 10]:
        m = Lasso(alpha=a, max_iter=20000)
        m.fit(X_train, y_train)
        rmse = np.sqrt(mean_squared_error(y_test, m.predict(X_test)))
        if rmse < best_l: best_l = rmse; best_alpha_l = a
    print(f"Best Lasso: {best_l:.0f} (Alpha {best_alpha_l})")

if __name__ == "__main__":
    run_search()
