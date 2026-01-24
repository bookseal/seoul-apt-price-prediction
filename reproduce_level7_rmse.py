
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from src.io import load_sample_dataset

# Match RANDOM_STATE
RANDOM_STATE = 42

def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))

def clean_data_logic(df):
    df_clean = df.copy()
    
    # Fill nulls with median
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df_clean[col] = df_clean[col].fillna(df_clean[col].median())
        
    # Remove outliers IQR 1.5
    for col in ['price_10k_krw', 'area_m2']:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]
        
    return df_clean

def run_test():
    # Load
    df = load_sample_dataset()
    
    # Ensure columns exist (same as Level 7 main)
    if 'year' not in df.columns:
        df['year'] = df['built_year'] if 'built_year' in df.columns else 2000
    if 'floor' not in df.columns:
        np.random.seed(42)
        df['floor'] = np.random.randint(1, 30, len(df))
        
    # Clean
    df_clean = clean_data_logic(df)
    
    # Train features
    features = ['area_m2', 'year', 'floor']
    
    X = df_clean[features].values
    y = df_clean['price_10k_krw'].values
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=RANDOM_STATE)
    
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    rmse = calculate_rmse(y_test, model.predict(X_test))
    print(f"Current Level 7 RMSE: {rmse}")
    return rmse

if __name__ == "__main__":
    run_test()
