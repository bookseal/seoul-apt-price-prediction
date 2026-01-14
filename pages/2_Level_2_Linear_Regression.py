# -*- coding: utf-8 -*-
"""
Level 2: Linear Regression (SGD Version)

Predict Price using Area + Gradient Descent (SGD).
"The Principles Way" - We control the learning!
"""
import streamlit as st
import random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from src.io import load_sample_dataset
from src.model import load_trained_model, get_model_info, calculate_metrics
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison

def display_header() -> None:
    """Display Level 2 intro."""
    st.title("📐 Level 2: Linear Regression")
    
    st.markdown("""
    **📋 Table of Contents**
    
    1. [The Method](#the-method)
    2. [Check Correlation](#data-area-vs-price)
    3. [Game: Fit the Line!](#game-fit-the-line)
    4. [Training Lab (SGD)](#training-lab-sgd)
    """)
    
    st.success("""
    **Goal**: Predict apartment price using machine learning (SGD).
    """)

def display_pipeline_overview() -> None:
    """Show the end-to-end ML pipeline for Level 2."""
    st.header("🔄 Level 2 Pipeline Overview")
    st.info("We will use **Gradient Descent** (SGD) to learn the best line step-by-step!")

def display_method() -> None:
    """Explain Linear Regression."""
    st.header("🧮 The Method")
    st.latex(r"\text{Price} = w \times \text{Area} + b")
    st.markdown("Feature: **Area**, Target: **Price**")

def display_data_insight(df: pd.DataFrame) -> None:
    """Show feature-target relationship."""
    st.header("📊 Data: Area vs Price")
    
    # Simple Scatter
    sample = df.sample(n=min(3000, len(df)), random_state=RANDOM_STATE)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(sample['area_m2'], sample['price_10k_krw'], alpha=0.3, s=10, c='steelblue')
    ax.set_title("Area vs Price")
    st.pyplot(fig)

def display_manual_fit_game(df: pd.DataFrame) -> None:
    """Game: Manually fit the line."""
    st.header("🎮 Game: Fit the Line!")
    
    st.markdown("""
    **Challenge**: Can you do what the computer does?
    Adjust the sliders to make the red line fit the blue dots.
    """)
    
    sample = df.sample(n=min(300, len(df)), random_state=RANDOM_STATE)
    X = sample['area_m2'].values
    y = sample['price_10k_krw'].values
    
    col1, col2 = st.columns(2)
    with col1:
        w_guess = st.slider("Weight (w)", 0, 2000, 500, step=10)
    with col2:
        b_guess = st.slider("Bias (b)", -50000, 50000, 0, step=1000)
        
    y_pred = w_guess * X + b_guess
    rmse = np.sqrt(np.mean((y - y_pred)**2))
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(X, y, alpha=0.3, c='steelblue', s=20)
    
    line_x = np.array([X.min(), X.max()])
    line_y = w_guess * line_x + b_guess
    ax.plot(line_x, line_y, 'r-', linewidth=3, label=f'RMSE={rmse:,.0f}')
    ax.legend()
    st.pyplot(fig)
    
    if rmse < 45000:
        st.success(f"🎉 Great fit!")

def display_training_lab(df: pd.DataFrame):
    """Interactive Training Lab with SGD."""
    st.header("🎓 Training Lab (SGD)")
    
    st.markdown("""
    Now let's use **Stochastic Gradient Descent** to train the model automatically!
    
    **⚠️ Note on Scaling**: 
    Area (Small) vs Price (Huge) -> We MUST scale data using `StandardScaler` for SGD to work.
    """)
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Hyperparameters")
        lr = st.select_slider("Learning Rate", options=[0.0001, 0.001, 0.01, 0.1], value=0.01)
        epochs = st.slider("Epochs", 1, 100, 10)
        run = st.button("🚀 Train Model", type="primary")
        
    with col2:
        if run:
            scaler_X = StandardScaler()
            scaler_y = StandardScaler()
            
            X = df[['area_m2']].values
            y = df[['price_10k_krw']].values
            
            X_scaled = scaler_X.fit_transform(X)
            y_scaled = scaler_y.fit_transform(y).ravel()
            
            model = SGDRegressor(eta0=lr, max_iter=epochs, random_state=RANDOM_STATE)
            model.fit(X_scaled, y_scaled)
            
            # Predict & RMSE
            y_pred_scaled = model.predict(X_scaled)
            y_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).ravel()
            rmse = np.sqrt(mean_squared_error(y, y_pred))
            
            st.metric("Final RMSE", f"{rmse:,.0f}")
            
            # Plot
            fig, ax = plt.subplots(figsize=(8, 4))
            sample_idx = np.random.choice(len(X), 300, replace=False)
            ax.scatter(X[sample_idx], y[sample_idx], alpha=0.3)
            
            line_x_raw = np.linspace(X.min(), X.max(), 100).reshape(-1, 1)
            line_x_sc = scaler_X.transform(line_x_raw)
            line_y_sc = model.predict(line_x_sc)
            line_y_raw = scaler_y.inverse_transform(line_y_sc.reshape(-1, 1))
            
            ax.plot(line_x_raw, line_y_raw, 'r-', linewidth=2, label="SGD Model")
            ax.legend()
            st.pyplot(fig)
            st.success("Training Complete!")

def main():
    display_header()
    df = load_sample_dataset()
    st.markdown("---")
    display_pipeline_overview()
    st.markdown("---")
    display_method()
    st.markdown("---")
    display_data_insight(df)
    st.markdown("---")
    display_manual_fit_game(df)
    st.markdown("---")
    display_training_lab(df)
    st.markdown("---")
    display_code_link("Level_2_Linear_Regression.ipynb")
    display_next_level_teaser(2)

if __name__ == "__main__":
    main()
