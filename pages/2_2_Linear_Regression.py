# -*- coding: utf-8 -*-
"""
Level 2.2: Linear Regression

Learn the fundamentals of Linear Regression.
Understand how the model makes predictions.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from src.io import load_sample_dataset
from src.model import load_trained_model, get_model_info
from src.config import RANDOM_STATE


def display_learning_goals() -> None:
    """Display learning objectives for this chapter."""
    st.info("""
    **🎯 What You'll Learn**
    - What is Linear Regression?
    - Understanding the prediction formula
    - Interpreting model coefficients
    """)


def display_header() -> None:
    """Render page title and introduction."""
    st.title("📐 2.2 Linear Regression")
    
    st.markdown("""
    ### The Simplest ML Model
    
    **Linear Regression** finds the best straight line through your data.
    It predicts the target variable using a linear equation.
    """)


def display_formula() -> None:
    """Explain the linear regression formula."""
    st.subheader("📝 The Formula")
    
    st.latex(r"y = w \cdot x + b")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **y** = Predicted price
        
        What we want to predict
        """)
    
    with col2:
        st.markdown("""
        **w** = Weight (slope)
        
        How much price changes per m²
        """)
    
    with col3:
        st.markdown("""
        **b** = Bias (intercept)
        
        Base price when area = 0
        """)
    
    st.markdown("""
    > **Training** = Finding the best values for **w** and **b** that minimize prediction errors
    """)


def display_model_visualization(df: pd.DataFrame) -> None:
    """Visualize the trained model."""
    st.subheader("📊 Our Trained Model")
    
    model = load_trained_model()
    
    if model is None:
        st.warning("⚠️ No trained model found. Run `python train.py` first.")
        return
    
    model_info = get_model_info(model)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Weight (w)", f"{model_info['coefficient']:,.2f}")
        st.caption("Price increase per 1 m²")
    
    with col2:
        st.metric("Bias (b)", f"{model_info['intercept']:,.2f}")
        st.caption("Base price (10K KRW)")
    
    # Visualization
    sample_df = df.sample(n=min(3000, len(df)), random_state=RANDOM_STATE)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.scatter(sample_df['area_m2'], sample_df['price_10k_krw'],
               alpha=0.2, s=10, c='steelblue', label='Data')
    
    # Model prediction line
    x_range = np.linspace(10, 200, 100)
    y_pred = model.predict(x_range.reshape(-1, 1))
    ax.plot(x_range, y_pred, 'r-', linewidth=3, label='Model Prediction')
    
    ax.set_xlabel('Exclusive Area (m²)', fontsize=12)
    ax.set_ylabel('Price (10K KRW)', fontsize=12)
    ax.set_title('Linear Regression: Area → Price', fontsize=14)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    st.pyplot(fig)
    plt.close()


def display_interpretation() -> None:
    """Explain how to interpret the model."""
    st.subheader("🔍 Model Interpretation")
    
    model = load_trained_model()
    
    if model is None:
        return
    
    coef = model.coef_[0]
    intercept = model.intercept_
    
    st.markdown(f"""
    Our model's prediction formula:
    
    **Price = {coef:,.2f} × Area + {intercept:,.2f}**
    
    **What this means:**
    - Every additional **1 m²** increases price by **{coef:,.0f}** (10K KRW)
    - That's about **{coef/100:,.1f} million KRW per m²**
    """)
    
    # Interactive example
    st.markdown("---")
    st.markdown("**Try it yourself:**")
    
    area = st.slider("Exclusive Area (m²)", min_value=20, max_value=200, value=84)
    predicted_price = model.predict([[area]])[0]
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Input Area", f"{area} m²")
    with col2:
        st.metric("Predicted Price", f"{predicted_price:,.0f} (10K KRW)")
    
    st.latex(f"{coef:.2f} \\times {area} + {intercept:.2f} = {predicted_price:.2f}")


def display_quiz() -> None:
    """Linear regression quiz."""
    st.markdown("---")
    st.subheader("✅ Knowledge Check")
    
    with st.expander("Quiz: Linear Regression Concepts"):
        st.markdown("""
        1. What does the 'weight' (w) represent in linear regression?
        2. What does 'training' a model mean?
        3. If w = 500, what happens when area increases by 10 m²?
        """)
        
        if st.button("Show Answers"):
            st.success("""
            1. **Slope** - How much the prediction changes per unit increase in feature
            2. **Finding optimal w and b** values that minimize prediction errors
            3. **Price increases by 5,000** (500 × 10 = 5,000 in 10K KRW)
            """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        display_header()
        display_learning_goals()
        st.markdown("---")
        display_formula()
        st.markdown("---")
        display_model_visualization(df)
        st.markdown("---")
        display_interpretation()
        display_quiz()
    except Exception as e:
        st.error(f"Failed to load data: {e}")


if __name__ == "__main__":
    main()
