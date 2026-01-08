# -*- coding: utf-8 -*-
"""
Level 2: Linear Regression (Single Feature)

First ML model using only exclusive area to predict price.
Formula: Price = weight × Area + bias
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from src.io import load_sample_dataset
from src.model import load_trained_model, get_model_info, calculate_metrics
from src.config import RANDOM_STATE


def display_header() -> None:
    """Display Level 2 introduction."""
    st.title("📐 Level 2: Linear Regression")
    
    st.success("""
    **Goal**: Predict apartment price using machine learning.
    
    We use **Linear Regression** - the simplest ML algorithm!
    """)


def display_method() -> None:
    """Explain Linear Regression."""
    st.header("🧮 The Method")
    
    st.markdown("""
    ### Linear Regression
    
    Find the **best straight line** that fits the data.
    """)
    
    st.latex(r"\text{Price} = w \times \text{Area} + b")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **w (weight)**: How much price increases per m²
        
        **b (bias)**: Base price when area = 0
        """)
    
    with col2:
        st.markdown("""
        **Training**: Find w and b that minimize prediction errors
        
        **RMSE**: Measures average prediction error
        """)
    
    with st.expander("🤔 Why Linear Regression?"):
        st.markdown("""
        - Simple and interpretable
        - Fast to train
        - Good baseline for comparison
        - Easy to explain: "Each m² adds X won to price"
        """)


def display_data_insight(df: pd.DataFrame) -> None:
    """Show feature-target relationship."""
    st.header("📊 Data: Area vs Price")
    
    # Sample for visualization
    sample = df.sample(n=min(3000, len(df)), random_state=RANDOM_STATE)
    
    # Correlation
    corr = sample['area_m2'].corr(sample['price_10k_krw'])
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.metric("Correlation", f"{corr:.3f}")
        st.markdown("""
        **Strong positive correlation!**
        
        Larger area → Higher price
        
        Linear Regression should work well.
        """)
    
    with col2:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.scatter(sample['area_m2'], sample['price_10k_krw'], 
                   alpha=0.3, s=10, c='steelblue')
        ax.set_xlabel('Exclusive Area (m²)')
        ax.set_ylabel('Price (10K KRW)')
        ax.set_title(f'Area vs Price (r = {corr:.3f})')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)
        plt.close()


def display_model_info() -> None:
    """Show trained model parameters."""
    st.header("🤖 Trained Model")
    
    model = load_trained_model()
    
    if model is None:
        st.warning("⚠️ No trained model found. Run `python train.py` first.")
        return
    
    info = get_model_info(model)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Weight (w)", f"{info['coefficient']:,.2f}")
        st.caption("Price increase per 1 m²")
    
    with col2:
        st.metric("Bias (b)", f"{info['intercept']:,.2f}")
        st.caption("Base price")
    
    st.info(f"""
    **Model Equation**:
    
    Price = **{info['coefficient']:,.2f}** × Area + **{info['intercept']:,.2f}**
    
    **Interpretation**: Each additional m² increases price by ~{info['coefficient']:,.0f} (10K KRW)
    """)


def display_evaluation(df: pd.DataFrame) -> None:
    """Show model performance metrics."""
    st.header("📏 Model Performance")
    
    model = load_trained_model()
    
    if model is None:
        return
    
    # Evaluate on sample
    sample = df.sample(n=min(10000, len(df)), random_state=RANDOM_STATE)
    X = sample['area_m2'].values.reshape(-1, 1)
    y_true = sample['price_10k_krw'].values
    y_pred = model.predict(X)
    
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
    
    # Actual vs Predicted plot
    with st.expander("📈 Actual vs Predicted"):
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.scatter(y_true[:1000], y_pred[:1000], alpha=0.3, s=10)
        max_val = max(y_true.max(), y_pred.max())
        ax.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Perfect')
        ax.set_xlabel('Actual Price')
        ax.set_ylabel('Predicted Price')
        ax.set_title('Actual vs Predicted')
        ax.legend()
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)
        plt.close()


def display_demo(df: pd.DataFrame) -> None:
    """Interactive prediction demo."""
    st.header("🔮 Try It Yourself")
    
    model = load_trained_model()
    
    if model is None:
        st.warning("Train the model first!")
        return
    
    info = get_model_info(model)
    
    # Input
    selected_area = st.slider("Exclusive Area (m²)", 
                               min_value=10, max_value=200, value=84)
    
    # Predict
    predicted_price = model.predict([[selected_area]])[0]
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Input Area", f"{selected_area} m²")
    
    with col2:
        st.metric("Predicted Price", f"{predicted_price:,.0f} (10K KRW)")
    
    # Show calculation
    st.info(f"""
    **Calculation**:
    
    Price = {info['coefficient']:,.2f} × {selected_area} + {info['intercept']:,.2f}
    
    = {info['coefficient'] * selected_area:,.2f} + {info['intercept']:,.2f}
    
    = **{predicted_price:,.0f}** (10K KRW) ≈ **{predicted_price/10000:.1f} 억원**
    """)


def display_comparison() -> None:
    """Compare with Level 1."""
    st.header("⚖️ Level 1 vs Level 2")
    
    st.markdown("""
    | Aspect | Level 1 (Heuristic) | Level 2 (Linear Regression) |
    |--------|---------------------|----------------------------|
    | Method | District median × Area | w × Area + b |
    | Uses District? | ✅ Yes | ❌ No |
    | Uses Area? | ✅ Yes | ✅ Yes |
    | ML? | ❌ No | ✅ Yes |
    | Interpretable? | ✅ Very | ✅ Yes |
    """)
    
    st.warning("""
    **Limitation**: Level 2 ignores district!
    
    A 100m² apartment in Gangnam costs much more than in other areas,
    but our model predicts the same price.
    
    **Next Level**: Add more features (district, floor, year)!
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_method()
        st.markdown("---")
        display_data_insight(df)
        st.markdown("---")
        display_model_info()
        st.markdown("---")
        display_evaluation(df)
        st.markdown("---")
        display_demo(df)
        st.markdown("---")
        display_comparison()
        
    except Exception as e:
        st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
