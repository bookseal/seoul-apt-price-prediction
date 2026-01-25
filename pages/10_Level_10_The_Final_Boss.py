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
from sklearn.linear_model import Ridge
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
    **Goal**: Reach the Mathematical Limit of Linear Models (RMSE < 25,000).
    
    We have extensively tested Log-Transforms, ElasticNets, and Lassos.
    **The Winner is a raw, brutal force of polynomial math.**
    """)

def display_toc() -> None:
    st.markdown("""
    ### 📑 Table of Contents
    1.  [**Step 1: The Winning Strategy**](#step-1-the-winning-strategy)
    2.  [**Step 2: The Pipeline Execution**](#step-2-the-pipeline-execution)
    3.  [**Step 3: Final Evaluation**](#step-3-final-evaluation)
    """)

def display_pipeline_concept() -> None:
    st.header("Step 1: The Winning Strategy")
    st.markdown("""
    After rigorous experimentation (Level 9), we found the optimal configuration to minimize **Real Price RMSE**:
    
    1.  **Target Variable**: **Direct Price** (No Log Transform). 
        *   *Why?* Log-transform minimizes *percentage error*, which helps cheaper apartments but punishes expensive ones less in absolute terms. To win the RMSE game, we must target the raw numbers directly.
    2.  **Cleaning (Level 7)**: Strict Outlier Removal (**IQR 1.5**) on the Raw Price.
    3.  **Feature Engineering (Level 5 & 8)**: Interaction Terms (`Area * Year`) are crucial.
    4.  **Model Complexity (Level 9)**: **Polynomial Degree 5**. This is extreme curvature.
    5.  **Regularization**: **Ridge** (L2). Lasso knocks out features, but we need *every bit of signal* from those polynomials. Ridge keeps them but tames them.
    """)
    
    with st.expander("Show Code: The Champion Pipeline"):
        st.code("""
# The Champion Pipeline
pipeline = Pipeline([
    ('poly', PolynomialFeatures(degree=5, include_bias=False)), # Extreme Curvature
    ('scaler', StandardScaler()),                               # Essential for Ridge
    ('model', Ridge(alpha=0.0001))                              # Minimal penalty, maximum learning
])
        """, language='python')

def run_ultimate_linear_model(df):
    st.header("Step 2: The Pipeline Execution")
    
    # --- 1. Data Cleaning (Level 7) ---
    st.markdown("##### 1. Data Cleaning (Target: Raw Price)")
    df_clean = df.copy()
    
    # Target Selection
    target_col = 'price_10k_krw'
    
    # Outlier Removal (IQR 1.5 on Raw Price)
    # This was the key finding: Cleaning the raw distribution aggressively helps Ridge the most.
    for col in [target_col, 'area_m2']:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]
    
    st.write(f"Data shape after cleaning: `{df_clean.shape}`")

    # --- 2. Feature Engineering (Level 8) ---
    st.markdown("##### 2. Feature Engineering (Interactions)")
    df_clean['area_x_year'] = df_clean['area_m2'] * df_clean['year']
    features = ['area_m2', 'year', 'floor', 'area_x_year']
    
    X = df_clean[features].values
    y = df_clean[target_col].values # Direct Target!
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # --- 3. Model Training ---
    st.markdown("##### 3. Training the Optimized Model")
    st.info("Pipeline: `Degree=5` + `Ridge(alpha=0.0001)`")
    
    model = Pipeline([
        ('poly', PolynomialFeatures(degree=5, include_bias=False)),
        ('scaler', StandardScaler()),
        ('model', Ridge(alpha=0.0001, random_state=42))
    ])
    
    with st.spinner("Fitting 5th Degree Polynomials... (This is math heavy!)"):
        model.fit(X_train, y_train)
    
    # Predict
    y_pred = model.predict(X_test)
    
    # No Inverse Transform needed (Direct Target)
    rmse = calculate_rmse(y_test, y_pred)
    
    st.metric("Ultimate Linear Model RMSE", f"{rmse:,.0f}", delta=f"{24000 - rmse:,.0f} Improvement vs Level 9", delta_color="normal")
    
    # --- 4. Validation ---
    st.header("Step 3: Final Evaluation")
    
    # Residual Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(y_test, y_pred, alpha=0.5, color='#4CAF50')
    ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2)
    ax.set_xlabel('Actual Price (10k KRW)')
    ax.set_ylabel('Predicted Price (10k KRW)')
    ax.set_title('Actual vs Predicted (Direct Optimization)')
    ax.grid(True, alpha=0.3)
    st.pyplot(fig)
    
    display_rmse_comparison(10, rmse)
    
    st.markdown("---")
    st.subheader("💡 Analysis: Why did this beat Log-Transform?")
    st.markdown("""
    In Level 7, we learned that Log-Transform fixes skewed distributions. So why drop it?
    
    1.  **The Metric Matters**: We are optimizing for **RMSE (Root Mean Squared Error)**, which measures absolute error.
    2.  **The Cost of Log**: Log-transform optimizes for *relative* error. It tries hard not to be wrong by 10% on a cheap apartment, but doesn't care if it's wrong by 10% on a luxury apartment.
    3.  **The Luxury Penalty**: A 10% error on a 2 Billion KRW apartment adds a **huge** amount to the total RMSE score. 
    4.  **The Solution**: By training on the **Raw Price**, the model is forced to care deeply about minimizing the absolute errors on the expensive apartments, driving the total RMSE down.
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
