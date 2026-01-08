# -*- coding: utf-8 -*-
"""
Level 2.3: Model Evaluation

Learn how to measure model performance.
Understanding RMSE and residual analysis.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from src.io import load_sample_dataset
from src.model import load_trained_model, calculate_metrics
from src.config import RANDOM_STATE


def display_learning_goals() -> None:
    """Display learning objectives for this chapter."""
    st.info("""
    **🎯 What You'll Learn**
    - What is RMSE and how to interpret it
    - Understanding residuals (prediction errors)
    - Evaluating model performance visually
    """)


def display_header() -> None:
    """Render page title and introduction."""
    st.title("📏 2.3 Model Evaluation")
    
    st.markdown("""
    ### How Good is Our Model?
    
    After training a model, we need to measure its performance.
    The key question: **How far off are our predictions from actual values?**
    """)


def display_rmse_explanation() -> None:
    """Explain RMSE with visual guide."""
    st.subheader("📐 RMSE (Root Mean Squared Error)")
    
    st.markdown("""
    RMSE is the standard metric for regression problems. 
    Think of it as the **average prediction error**.
    """)
    
    # Step by step
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div style="text-align: center; padding: 15px; 
                    background: rgba(33, 150, 243, 0.1); border-radius: 8px;">
        <div style="font-size: 18px; margin-bottom: 5px;">1️⃣</div>
        <div style="font-weight: bold; font-size: 12px;">Error</div>
        <div style="font-size: 11px;">Actual - Predicted</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 15px; 
                    background: rgba(156, 39, 176, 0.1); border-radius: 8px;">
        <div style="font-size: 18px; margin-bottom: 5px;">2️⃣</div>
        <div style="font-weight: bold; font-size: 12px;">Square</div>
        <div style="font-size: 11px;">Error² (no negatives)</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="text-align: center; padding: 15px; 
                    background: rgba(255, 152, 0, 0.1); border-radius: 8px;">
        <div style="font-size: 18px; margin-bottom: 5px;">3️⃣</div>
        <div style="font-weight: bold; font-size: 12px;">Mean</div>
        <div style="font-size: 11px;">Average of squares</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div style="text-align: center; padding: 15px; 
                    background: rgba(76, 175, 80, 0.1); border-radius: 8px;">
        <div style="font-size: 18px; margin-bottom: 5px;">4️⃣</div>
        <div style="font-weight: bold; font-size: 12px;">Root</div>
        <div style="font-size: 11px;">√ back to original unit</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.latex(r"RMSE = \sqrt{\frac{1}{n} \sum_{i=1}^{n} (y_i - \hat{y}_i)^2}")
    
    st.markdown("""
    **Why square then take root?**
    - Squaring removes negative signs (errors of +100 and -100 should both count)
    - Root brings units back to original scale (10K KRW)
    """)


def display_model_metrics(df: pd.DataFrame) -> None:
    """Show model performance metrics."""
    st.subheader("📊 Our Model's Performance")
    
    model = load_trained_model()
    
    if model is None:
        st.warning("⚠️ No trained model found. Run `python train.py` first.")
        return
    
    # Sample data for evaluation
    sample_df = df.sample(n=min(10000, len(df)), random_state=RANDOM_STATE)
    X = sample_df['area_m2'].values.reshape(-1, 1)
    y_true = sample_df['price_10k_krw'].values
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
        relative_error = metrics['rmse'] / y_true.mean() * 100
        st.metric("RMSE / Mean", f"{relative_error:.1f}%")
        st.caption("Relative error")
    
    st.markdown(f"""
    **Interpretation**:
    - On average, predictions are off by about **{metrics['rmse']:,.0f}** (10K KRW)
    - That's approximately **{metrics['rmse']/100:.1f} million KRW** error
    - Relative to average price, error is **{relative_error:.1f}%**
    """)


def display_residual_analysis(df: pd.DataFrame) -> None:
    """Show residual analysis plots."""
    st.subheader("🔍 Residual Analysis")
    
    model = load_trained_model()
    
    if model is None:
        return
    
    sample_df = df.sample(n=min(5000, len(df)), random_state=RANDOM_STATE)
    X = sample_df['area_m2'].values.reshape(-1, 1)
    y_true = sample_df['price_10k_krw'].values
    y_pred = model.predict(X).flatten()
    residuals = y_true - y_pred
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Actual vs Predicted**")
        fig1, ax1 = plt.subplots(figsize=(6, 5))
        ax1.scatter(y_true, y_pred, alpha=0.3, s=10)
        
        max_val = max(y_true.max(), y_pred.max())
        ax1.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Perfect')
        
        ax1.set_xlabel('Actual Price')
        ax1.set_ylabel('Predicted Price')
        ax1.set_title('Actual vs Predicted')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        st.pyplot(fig1)
        plt.close()
    
    with col2:
        st.markdown("**Residual Distribution**")
        fig2, ax2 = plt.subplots(figsize=(6, 5))
        ax2.hist(residuals, bins=50, color='steelblue', alpha=0.7, edgecolor='white')
        ax2.axvline(x=0, color='red', linestyle='--', linewidth=2)
        ax2.set_xlabel('Residual (Actual - Predicted)')
        ax2.set_ylabel('Frequency')
        ax2.set_title('Residual Distribution')
        st.pyplot(fig2)
        plt.close()
    
    with st.expander("🤔 How to read these plots?"):
        st.markdown("""
        **Actual vs Predicted:**
        - Points near the red line = good predictions
        - Scattered points = high error
        - Our model works better for lower prices
        
        **Residual Distribution:**
        - Centered around 0 = unbiased model
        - Wide spread = high variance in errors
        - Skewed = systematic bias (over/under predicting)
        """)


def display_quiz() -> None:
    """Model evaluation quiz."""
    st.markdown("---")
    st.subheader("✅ Knowledge Check")
    
    with st.expander("Quiz: Evaluation Concepts"):
        st.markdown("""
        1. What does RMSE = 5000 mean for apartment prices?
        2. Why do we square errors before averaging?
        3. What should an ideal residual distribution look like?
        """)
        
        if st.button("Show Answers"):
            st.success("""
            1. Average prediction error is **50 million KRW** (5000 × 10K)
            2. To **remove negative signs** and **penalize large errors** more
            3. **Centered at 0** and **symmetric** (bell-shaped)
            """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        display_header()
        display_learning_goals()
        st.markdown("---")
        display_rmse_explanation()
        st.markdown("---")
        display_model_metrics(df)
        st.markdown("---")
        display_residual_analysis(df)
        display_quiz()
    except Exception as e:
        st.error(f"Failed to load data: {e}")


if __name__ == "__main__":
    main()
