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
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from src.io import load_sample_dataset
# Removed load_trained_model dependence as we train live now
from src.model import get_model_info, calculate_metrics
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison


def display_header() -> None:
    """Display Level 2 introduction."""
    st.title("📐 Level 2: Linear Regression")
    
    st.markdown("""
    **📋 Table of Contents**
    
    1. [🧮 The Method](#the-method)
    2. [📊 Check Correlation](#data-area-vs-price)
    3. [🎓 Interactive Simulator](#step-3-training-interactive-simulator)
    4. [🚀 Automated Training (SGD)](#step-3-5-automated-training-sgd)
    5. [🤖 Trained Model Info](#step-4-trained-model)
    6. [📏 Performance Evaluation](#model-performance)
    7. [🔮 Prediction Demo](#try-it-yourself)
    """)
    
    st.success("""
    **Goal**: Predict apartment price using machine learning (SGD).
    """)

def display_pipeline_overview() -> None:
    """Show the end-to-end ML pipeline for Level 2."""
    st.header("🔄 Level 2 Pipeline Overview")
    st.info("We will use **Gradient Descent** (SGD) to learn the best line step-by-step!")

def display_why_level2() -> None:
    """Explain problems with Level 1 and motivation for Level 2."""
    st.header("🤔 Wait... What's Wrong with Level 1?")
    
    st.markdown("""
    Level 1 worked! But think about these problems...
    """)
    
    col1, col2 = st.columns(2)
    with col1:
        st.error("**Problem 1: We Decided the Formula**")
        st.caption("We just guessed 'Median'. Is that really the best?")
    with col2:
        st.error("**Problem 2: No 'Learning'**")
        st.caption("Level 1 doesn't improve from data. It just calculates.")

    st.success("""
    **Level 2 Solution: Let the Computer Learn!**
    
    Instead of **us** deciding the values, we let the **computer find the optimal w and b** using Gradient Descent.
    """)

def display_method() -> None:
    """Explain Linear Regression."""
    st.header("🧮 The Method")
    st.latex(r"\text{Price} = w \times \text{Area} + b")
    st.markdown("Feature: **Area**, Target: **Price**")
    
    with st.expander("Why 'Linear Regression'?"):
        st.markdown("""
        - **Linear**: We draw a straight line.
        - **Regression**: We predict a number (Price).
        """)

def display_data_insight(df: pd.DataFrame) -> None:
    """Show feature-target relationship."""
    st.header("📊 Data: Area vs Price")
    
    # Sample for visualization
    sample = df.sample(n=min(3000, len(df)), random_state=RANDOM_STATE)
    
    # Scatter plot
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(sample['area_m2'], sample['price_10k_krw'], 
               alpha=0.3, s=10, c='steelblue', label='Data points')
    
    ax.set_xlabel('Exclusive Area (m²)')
    ax.set_ylabel('Price (10K KRW)')
    ax.set_title('Area vs Price')
    st.pyplot(fig, use_container_width=True)
    plt.close()

def get_loss_surface(X, y, param_range):
    """Calculate loss surface for visualization."""
    w_range = np.linspace(param_range['w_min'], param_range['w_max'], 20)
    b_range = np.linspace(param_range['b_min'], param_range['b_max'], 20)
    W, B = np.meshgrid(w_range, b_range)
    Z = np.zeros_like(W)
    
    for i in range(W.shape[0]):
        for j in range(W.shape[1]):
            pred = W[i, j] * X + B[i, j]
            Z[i, j] = np.mean((pred - y) ** 2)
            
    return W, B, Z

def display_training_process(df: pd.DataFrame) -> None:
    """Interactive Gradient Descent Simulator."""
    st.header("🎓 Step 3: Training (Interactive Simulator)")
    
    st.markdown("""
    **Experiencing Gradient Descent**
    
    Let's train the model ourselves! We will use **Gradient Descent** to find the best `w` and `b`.
    """)
    
    with st.expander("🏔️ What is Gradient Descent? (The Mountain Hiker Analogy)", expanded=True):
        st.markdown("""
        Imagine you are on a **mountain at night** (blindfolded). You want to reach the **village at the bottom** (Lowest Error).
        
        1.  **Feel the slope**: You tap the ground with your foot to see which way connects "down".
        2.  **Take a step**: You take a step in the downhill direction.
        3.  **Repeat**: You keep doing this until the ground is flat (you reached the bottom!).
        """)
    
    # Simulation Data
    sample = df.sample(n=50, random_state=42)
    X = sample['area_m2'].values
    y = sample['price_10k_krw'].values
    
    optimal_w, optimal_b = np.polyfit(X, y, 1)
    
    # Session State
    if 'gd_w' not in st.session_state:
        st.session_state['gd_w'] = random.uniform(0, 2000)
    if 'gd_b' not in st.session_state:
        st.session_state['gd_b'] = random.uniform(-50000, 50000)
    if 'gd_epoch' not in st.session_state:
        st.session_state['gd_epoch'] = 0
    if 'gd_history' not in st.session_state:
        st.session_state['gd_history'] = []

    # Controls
    col_lr, col_btn = st.columns([1, 2])
    
    with col_lr:
        step_speed = st.radio("Step Size (Learning Rate)",
            ["🐢 Little Steps (Careful)", "🐇 Big Steps (Fast)"], index=0)
        
        if step_speed == "🐢 Little Steps (Careful)":
            st.session_state['gd_lr'] = 0.00001
        else:
            st.session_state['gd_lr'] = 0.00005
            
    with col_btn:
        st.write("") 
        st.write("") 
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Step (1x)"):
                y_pred = st.session_state['gd_w'] * X + st.session_state['gd_b']
                error = y_pred - y
                w_grad = (2/len(X)) * np.sum(error * X)
                b_grad = (2/len(X)) * np.sum(error)
                st.session_state['gd_w'] -= st.session_state['gd_lr'] * w_grad
                st.session_state['gd_b'] -= st.session_state['gd_lr'] * b_grad * 100 
                st.session_state['gd_epoch'] += 1
                st.session_state['gd_history'].append((st.session_state['gd_w'], st.session_state['gd_b']))
        with c2:
            if st.button("Fast (10x)"):
                for _ in range(10):
                    y_pred = st.session_state['gd_w'] * X + st.session_state['gd_b']
                    error = y_pred - y
                    w_grad = (2/len(X)) * np.sum(error * X)
                    b_grad = (2/len(X)) * np.sum(error)
                    st.session_state['gd_w'] -= st.session_state['gd_lr'] * w_grad
                    st.session_state['gd_b'] -= st.session_state['gd_lr'] * b_grad * 100
                st.session_state['gd_epoch'] += 10
                st.session_state['gd_history'].append((st.session_state['gd_w'], st.session_state['gd_b']))
        with c3:
            if st.button("Reset"):
                st.session_state['gd_w'] = random.uniform(0, 2000)
                st.session_state['gd_b'] = random.uniform(-50000, 50000)
                st.session_state['gd_epoch'] = 0
                st.session_state['gd_history'] = []
    
    # Stats
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Epoch: {st.session_state['gd_epoch']}**")
    with col2:
        cur_mse = np.mean((st.session_state['gd_w'] * X + st.session_state['gd_b'] - y) ** 2)
        st.metric("Current RMSE", f"{np.sqrt(cur_mse):,.0f}")

    # Visualization
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Plot 1: Loss Surface
    w_min, w_max = -500, 2500
    b_min, b_max = -100000, 100000
    W, B, Z = get_loss_surface(X, y, {'w_min': w_min, 'w_max': w_max, 'b_min': b_min, 'b_max': b_max})
    cp = ax1.contourf(W, B, np.sqrt(Z), levels=20, cmap='viridis_r')
    fig.colorbar(cp, ax=ax1, label='RMSE')
    if st.session_state['gd_history']:
        path = np.array(st.session_state['gd_history'])
        ax1.plot(path[:, 0], path[:, 1], 'w-', alpha=0.5)
    ax1.plot(st.session_state['gd_w'], st.session_state['gd_b'], 'ro', markersize=10, markeredgecolor='white', label='Current')
    ax1.plot(optimal_w, optimal_b, 'b*', markersize=15, label='Optimal')
    ax1.set_title("Error Mountain (Loss Surface)")
    ax1.legend()
    
    # Plot 2: Regression Line
    ax2.scatter(X, y, alpha=0.3, c='steelblue', s=15)
    line_x = np.array([X.min(), X.max()])
    line_y = st.session_state['gd_w'] * line_x + st.session_state['gd_b']
    ax2.plot(line_x, line_y, 'r-', linewidth=3, label='Your Model')
    opt_y = optimal_w * line_x + optimal_b
    ax2.plot(line_x, opt_y, 'k--', alpha=0.3, label='Best Possible')
    ax2.set_title("Resulting Model Line")
    ax2.legend()
    st.pyplot(fig)


def display_sgd_training_lab(df: pd.DataFrame):
    """Interactive Training Lab with SGD (Automated)."""
    st.header("🚀 Step 3.5: Automated Training (SGD)")
    
    st.markdown("""
    The simulator above showed how it works step-by-step.
    Now, let's use the real **SGDRegressor** to train it instantly on the FULL dataset!
    
    **⚠️ Note on Scaling**: 
    Area (0~200) vs Price (0~300,000) -> We MUST scale data using `StandardScaler` for SGD to work efficiently.
    """)
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Hyperparameters")
        lr = st.select_slider("Learning Rate (eta0)", options=[0.0001, 0.001, 0.01, 0.1], value=0.01)
        epochs = st.slider("Epochs (max_iter)", 1, 100, 50)
        run = st.button("START TRAINING", type="primary")
        
    if run:
        scaler_X = StandardScaler()
        scaler_y = StandardScaler()
        
        X = df[['area_m2']].values
        y = df[['price_10k_krw']].values
        
        X_scaled = scaler_X.fit_transform(X)
        y_scaled = scaler_y.fit_transform(y).ravel()
        
        with st.spinner("Training SGDRegressor..."):
            model = SGDRegressor(eta0=lr, max_iter=epochs, random_state=RANDOM_STATE)
            model.fit(X_scaled, y_scaled)
            
            # Use 'st.session_state' to save for later sections
            st.session_state['sgd_model'] = model
            st.session_state['scaler_X'] = scaler_X
            st.session_state['scaler_y'] = scaler_y
        
        # Immediate Result Display
        y_pred_scaled = model.predict(X_scaled)
        y_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).ravel()
        rmse = np.sqrt(mean_squared_error(y, y_pred))
        
        col2.success(f"Training Complete!")
        col2.metric("Final RMSE", f"{rmse:,.0f}")
        
        # Quick Plot
        with col2:
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


def get_trained_components():
    """Helper to retrieve model/scalers from session state."""
    model = st.session_state.get('sgd_model')
    scaler_X = st.session_state.get('scaler_X')
    scaler_y = st.session_state.get('scaler_y')
    return model, scaler_X, scaler_y


def display_model_info(df: pd.DataFrame) -> None:
    """Show trained model parameters and training details."""
    st.header("🤖 Step 4: Trained Model")
    
    model, scaler_X, scaler_y = get_trained_components()
    
    if model is None:
        st.warning("⚠️ No trained model found. Please run 'Step 3.5: Automated Training' above!")
        return
    
    st.info(f"""
    **Learned Parameters (Scaled Space)**:
    - Weight: {model.coef_[0]:.4f}
    - Bias: {model.intercept_[0]:.4f}
    
    *Note: Since we used Standard Scaling, these values apply to the scaled data. The real equation is handled automatically by the inverse transform!*
    """)


def display_evaluation(df: pd.DataFrame) -> None:
    """Show model performance metrics."""
    st.header("📏 Model Performance")
    
    model, scaler_X, scaler_y = get_trained_components()
    
    if model is None:
        st.info("Train the model to see evaluation.")
        return
    
    # Evaluate on sample
    sample = df.sample(n=min(10000, len(df)), random_state=RANDOM_STATE)
    X = sample[['area_m2']].values
    y_true = sample['price_10k_krw'].values
    
    X_scaled = scaler_X.transform(X)
    y_pred_scaled = model.predict(X_scaled)
    y_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).ravel()
    
    metrics = calculate_metrics(y_true, y_pred)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("RMSE", f"{metrics['rmse']:,.0f}")
        st.caption("Average error (10K KRW)")
    
    with col2:
        st.metric("MAE", f"{metrics['mae']:,.0f}")
        st.caption("Mean Absolute Error")
    
    with col3:
        rel_err = metrics['rmse'] / y_true.mean() * 100
        st.metric("Relative Error", f"{rel_err:.1f}%")
    
    # Actual vs Predicted explanation and plot
    st.markdown("### 📈 Actual vs Predicted")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(y_true[:1000], y_pred[:1000], alpha=0.3, s=15, c='steelblue')
    max_val = max(y_true.max(), y_pred.max())
    ax.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Perfect Prediction')
    ax.set_xlabel('Actual Price')
    ax.set_ylabel('Predicted Price')
    ax.legend()
    st.pyplot(fig, use_container_width=True)
    plt.close()


def display_demo(df: pd.DataFrame) -> None:
    """Interactive prediction demo."""
    st.header("🔮 Try It Yourself")
    
    model, scaler_X, scaler_y = get_trained_components()
    
    if model is None:
        st.info("Train the model first!")
        return
    
    selected_area = st.slider("Exclusive Area (m²)", 
                               min_value=10, max_value=200, value=84)
    
    # Predict
    X_input = np.array([[selected_area]])
    X_input_scaled = scaler_X.transform(X_input)
    y_pred_scaled = model.predict(X_input_scaled)
    predicted_price = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).ravel()[0]
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Input Area", f"{selected_area} m²")
    with col2:
        st.metric("Predicted Price", f"{predicted_price:,.0f} (10K KRW)")


def display_comparison() -> None:
    """Compare with Level 1."""
    st.header("⚖️ Level 1 vs Level 2")
    
    st.markdown("""
    **The Shocking Truth**: Level 1 (Heuristic) might actually be **better** than this simple Level 2 model!
    
    Why?
    - **Level 1** used **Location** (Gu/Dong).
    - **Level 2** is only using **Area**.
    
    Even the smartest algorithm (SGD) cannot beat better data (Location).
    
    **This is why we need Level 3!**
    Level 3 will combine **Machine Learning** (Algorithm) with **District Info** (Data).
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_why_level2()
        st.markdown("---")
        display_method()
        st.markdown("---")
        display_data_insight(df)
        st.markdown("---")
        display_training_process(df)  # Manual
        st.markdown("---")
        display_sgd_training_lab(df)  # Automated
        st.markdown("---")
        display_model_info(df)
        st.markdown("---")
        display_evaluation(df)
        st.markdown("---")
        display_demo(df)
        st.markdown("---")
        display_comparison()
        
        display_code_link("Level_2_Linear_Regression.ipynb")
        display_next_level_teaser(2)
        
    except Exception as e:
        st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
