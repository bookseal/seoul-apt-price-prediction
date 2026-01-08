# -*- coding: utf-8 -*-
"""
Level 2.1: Feature Selection

Learn how to choose the right features for your model.
Understanding correlation analysis between features and target.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from src.io import load_sample_dataset
from src.config import RANDOM_STATE


def display_learning_goals() -> None:
    """Display learning objectives for this chapter."""
    st.info("""
    **🎯 What You'll Learn**
    - What is a 'feature' in machine learning?
    - How to analyze correlation between features and target
    - Choosing the simplest feature for our first model
    """)


def display_header() -> None:
    """Render page title and introduction."""
    st.title("🎯 2.1 Feature Selection")
    
    st.markdown("""
    ### What is a Feature?
    
    In machine learning, a **feature** is an input variable used to make predictions.
    
    For apartment price prediction, potential features include:
    - Exclusive area (㎡)
    - District (location)
    - Floor number
    - Built year
    
    **Our strategy**: Start with ONE feature, then gradually add more.
    """)


def display_correlation_analysis(df: pd.DataFrame) -> None:
    """Show correlation analysis between area and price."""
    st.subheader("📈 Correlation Analysis")
    
    st.markdown("""
    **Correlation coefficient** measures the linear relationship between two variables.
    - Range: -1 to +1
    - +1: Perfect positive correlation
    - 0: No linear relationship
    - -1: Perfect negative correlation
    """)
    
    # Calculate correlation
    correlation = df['area_m2'].corr(df['price_10k_krw'])
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.metric("Area ↔ Price Correlation", f"{correlation:.4f}")
        st.markdown("""
        **Interpretation**:
        - Strong positive correlation!
        - Larger area → Higher price
        - Good candidate for prediction
        """)
    
    with col2:
        # Scatter plot
        sample_df = df.sample(n=min(5000, len(df)), random_state=RANDOM_STATE)
        
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.scatter(sample_df['area_m2'], sample_df['price_10k_krw'], 
                   alpha=0.3, s=10, c='steelblue')
        
        # Regression line
        z = np.polyfit(sample_df['area_m2'], sample_df['price_10k_krw'], 1)
        p = np.poly1d(z)
        x_line = np.linspace(sample_df['area_m2'].min(), sample_df['area_m2'].max(), 100)
        ax.plot(x_line, p(x_line), 'r-', linewidth=2, label='Trend Line')
        
        ax.set_xlabel('Exclusive Area (m²)')
        ax.set_ylabel('Price (10K KRW)')
        ax.set_title(f'Area vs Price (r = {correlation:.4f})')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        st.pyplot(fig)
        plt.close()


def display_feature_choice() -> None:
    """Explain why we choose area as first feature."""
    st.subheader("🎯 Our First Feature: Exclusive Area")
    
    st.markdown("""
    For our **first model**, we'll use only **Exclusive Area (㎡)**.
    
    **Why start simple?**
    
    | Reason | Explanation |
    |--------|-------------|
    | **High correlation** | Strong relationship with price |
    | **Easy to interpret** | "Bigger = More Expensive" makes sense |
    | **No preprocessing** | Already numeric, no encoding needed |
    | **Baseline model** | Sets benchmark for improvement |
    """)
    
    with st.expander("🤔 Why not use all features at once?"):
        st.markdown("""
        1. **Harder to debug**: If results are bad, you won't know which feature is the problem
        2. **Overfitting risk**: More features can lead to memorizing noise
        3. **Complexity**: Categorical features (district) need extra encoding steps
        4. **Start Simple, Scale Smart**: Build understanding incrementally
        """)


def display_quiz() -> None:
    """Feature selection quiz."""
    st.markdown("---")
    st.subheader("✅ Knowledge Check")
    
    with st.expander("Quiz: Feature Selection Concepts"):
        st.markdown("""
        1. What correlation coefficient indicates NO linear relationship?
        2. Why is area a good first feature for price prediction?
        3. What does "Start Simple, Scale Smart" mean?
        """)
        
        if st.button("Show Answers"):
            st.success("""
            1. **0** - No linear relationship between variables
            2. **High correlation** with price, easy to interpret, already numeric
            3. Build the simplest working model first, then gradually improve it
            """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        display_header()
        display_learning_goals()
        st.markdown("---")
        display_correlation_analysis(df)
        st.markdown("---")
        display_feature_choice()
        display_quiz()
    except Exception as e:
        st.error(f"Failed to load data: {e}")


if __name__ == "__main__":
    main()
