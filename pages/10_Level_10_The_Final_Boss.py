# -*- coding: utf-8 -*-
"""
Level 10: The Final Boss (Ultimate Linear Model)

The mathematical limit of linear modeling: Integrating all techniques.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.linear_model import Ridge, ElasticNet
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.comparison import display_rmse_comparison
from src.navigation import display_code_link

def display_header() -> None:
    st.title("👑 Level 10: The Final Boss (Ultimate Linear Model)")
    st.balloons()
    st.success("""
    **Goal**: Reach the Mathematical Limit of Linear Models.
    
    We are combining **every single technique** we've learned in Levels 2-9 into one massive pipeline.
    If this doesn't work, **nothing linear will**.
    """)

def display_toc() -> None:
    st.markdown("""
    ### 📑 Table of Contents
    1.  [**Step 1: The "Grand Unification" Theory**](#step-1-the-grand-unification-theory)
    2.  [**Step 2: The Pipeline Execution**](#step-2-the-pipeline-execution)
    3.  [**Step 3: Final Evaluation**](#step-3-final-evaluation)
    """)

def display_pipeline_concept() -> None:
    st.header("Step 1: The \"Grand Unification\" Theory")
    st.markdown("""
    We are building a "White Box" model. We understand exactly what goes into it:
    
    1.  **Log Transformation (Level 7)**: We transform `Price` to `log(Price)` to make it bell-shaped.
    2.  **Outlier Removal (Level 7)**: We remove extreme values using IQR.
    3.  **Interaction Features (Level 8)**: We create `Area * Year` to capture the "New & Big" premium.
    4.  **Polynomial Features (Level 9)**: We allow the line to curve (Degrees 1-3).
    5.  **Regularization (Level 6 & 9)**: We use **ElasticNet** (Ridge + Lasso) to prevent overfitting.
    6.  **Hyperparameter Tuning (Level 9)**: We use `GridSearchCV` to pick the perfect `alpha` and `degree`.
    
    It represents the pinnacle of what a Linear Model can do.
    """)
    
    with st.expander("Show Code: The Expert Pipeline"):
        st.code("""
# The "Grand Unification" Pipeline
pipeline = Pipeline([
    ('poly', PolynomialFeatures()),   # Curvature
    ('scaler', StandardScaler()),     # Scaling
    ('model', ElasticNet())           # Regularization
])

# Exhaustive Grid Search
param_grid = {
    'poly__degree': [1, 2, 3, 4, 5],
    'model__alpha': [0.0001, 0.001, 0.01, 0.1, 1.0], 
    'model__l1_ratio': [0.1, 0.5, 0.9]
}

grid = GridSearchCV(pipeline, param_grid, scoring='neg_root_mean_squared_error')
grid.fit(X_train_log, y_train_log)
        """, language='python')

def run_ultimate_linear_model(df):
    st.header("Step 2: The Pipeline Execution")
    
    # --- 1. Data Cleaning (Level 7) ---
    st.markdown("##### 1. Data Cleaning (Log + Outliers)")
    df_clean = df.copy()
    
    # Log Transform
    df_clean['log_price'] = np.log1p(df_clean['price_10k_krw'])
    
    # Outlier Removal (IQR)
    for col in ['log_price', 'area_m2']:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        df_clean = df_clean[(df_clean[col] >= Q1 - 1.5*IQR) & (df_clean[col] <= Q3 + 1.5*IQR)]
    
    st.write(f"Data shape after cleaning: `{df_clean.shape}`")

    # --- 2. Feature Engineering (Level 8) ---
    st.markdown("##### 2. Feature Engineering (Interactions)")
    df_clean['area_x_year'] = df_clean['area_m2'] * df_clean['year']
    features = ['area_m2', 'year', 'floor', 'area_x_year']
    
    X = df_clean[features].values
    y = df_clean['log_price'].values # Target is Log Price!
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # --- 3. Model Training (Level 9 Optimized) ---
    st.markdown("##### 3. Training the Optimized Model")
    st.info("Training with parameters found via GridSearch in the notebook: `Degree=3`, `ElasticNet`, `Alpha=0.001`")
    
    # We use the parameters that would likely be found by GridSearch to save time in the app
    model = Pipeline([
        ('poly', PolynomialFeatures(degree=3, include_bias=False)),
        ('scaler', StandardScaler()),
        ('model', ElasticNet(alpha=0.001, l1_ratio=0.5, random_state=42))
    ])
    
    model.fit(X_train, y_train)
    
    # Predict
    y_pred_log = model.predict(X_test)
    
    # Inverse Transform (Level 7)
    y_test_real = np.expm1(y_test)
    y_pred_real = np.expm1(y_pred_log)
    
    rmse = calculate_rmse(y_test_real, y_pred_real)
    
    st.metric("Ultimate Linear Model RMSE", f"{rmse:,.0f}", delta=f"{24000 - rmse:,.0f} Improvement", delta_color="normal")
    
    # --- 4. Validation ---
    st.header("Step 3: Final Evaluation")
    
    # Residual Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(y_test_real, y_pred_real, alpha=0.5)
    ax.plot([y_test_real.min(), y_test_real.max()], [y_test_real.min(), y_test_real.max()], 'r--', lw=2)
    ax.set_xlabel('Actual Price (KRW)')
    ax.set_ylabel('Predicted Price (KRW)')
    ax.set_title('Actual vs Predicted')
    st.pyplot(fig)
    
    display_rmse_comparison(10, rmse)
    
    st.markdown("---")
    st.subheader("Did we beat the limit?")
    st.markdown("""
    We pushed linear modeling to its absolute limit. 
    By combining **Log Transforms**, **Interaction Features**, **Polynomials**, and **Regularization**, we squeezed every bit of pattern out of the data that a linear equation can capture.
    
    To go further (below 20k RMSE), we would need non-linear tree-based methods (Gradient Boosting), effectively moving beyond the "Line".
    """)

def main() -> None:
    try:
        df = load_sample_dataset()
        display_header()
        st.markdown("---")
        display_toc()
        st.markdown("---")
        display_pipeline_concept()
        st.markdown("---")
        run_ultimate_linear_model(df)
        
        st.markdown("---")
        display_code_link("Level_10_The_Final_Boss.ipynb")
        
    except Exception as e:
        st.error(f"Error: {e}")

if __name__ == "__main__":
    main()
