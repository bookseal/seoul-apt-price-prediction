# -*- coding: utf-8 -*-
"""
Level 2.4: Prediction Demo

Interactive price prediction using the trained model.
See your model in action!
"""
import streamlit as st
import pandas as pd
from src.io import load_sample_dataset
from src.model import load_trained_model, get_model_info


def display_learning_goals() -> None:
    """Display learning objectives for this chapter."""
    st.info("""
    **🎯 What You'll Learn**
    - How to use a trained model for predictions
    - Understanding model limitations
    - Ideas for improvement
    """)


def display_header() -> None:
    """Render page title and introduction."""
    st.title("🔮 2.4 Prediction Demo")
    
    st.markdown("""
    ### Try Your Model!
    
    Now let's use our trained Linear Regression model to predict apartment prices.
    Enter an area value and see the prediction instantly.
    """)


def display_prediction_interface() -> None:
    """Show interactive prediction interface."""
    st.subheader("🏠 Price Predictor")
    
    model = load_trained_model()
    
    if model is None:
        st.warning("⚠️ No trained model found. Run `python train.py` first.")
        return
    
    model_info = get_model_info(model)
    
    # Input
    area = st.slider(
        "Exclusive Area (m²)",
        min_value=10,
        max_value=300,
        value=84,
        step=1
    )
    
    # Prediction
    predicted_price = model.predict([[area]])[0]
    
    # Display
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Input Area", f"{area} m²")
    
    with col2:
        st.metric("Predicted Price", f"{predicted_price:,.0f} (10K KRW)")
    
    # Price in different units
    st.markdown(f"""
    **Price breakdown:**
    - **{predicted_price:,.0f}** × 10,000 KRW = **{predicted_price * 10000:,.0f} KRW**
    - Approximately **{predicted_price / 100:.1f} billion KRW** 
    - Or about **${predicted_price * 10000 / 1300:,.0f} USD** (at 1,300 KRW/USD)
    """)
    
    # Formula
    st.markdown("---")
    st.subheader("📐 Calculation")
    
    coef = model_info['coefficient']
    intercept = model_info['intercept']
    
    st.latex(f"Price = {coef:.2f} \\times {area} + {intercept:.2f}")
    st.latex(f"= {coef * area:.2f} + {intercept:.2f}")
    st.latex(f"= {predicted_price:.2f}")


def display_model_limitations() -> None:
    """Explain model limitations."""
    st.markdown("---")
    st.subheader("⚠️ Model Limitations")
    
    st.markdown("""
    Our simple model has several limitations:
    
    | Limitation | Explanation |
    |------------|-------------|
    | **Single feature** | Only uses area, ignores location, floor, age |
    | **Linear assumption** | Assumes straight-line relationship |
    | **District blindness** | Gangnam vs other districts priced the same |
    | **No outlier handling** | Extreme values affect predictions |
    """)
    
    with st.expander("🚀 How to improve?"):
        st.markdown("""
        **Level 3+ improvements:**
        
        1. **Add more features**: District, floor, built year
        2. **Feature engineering**: Price per m², district encoding
        3. **Better algorithms**: Random Forest, XGBoost, LightGBM
        4. **Hyperparameter tuning**: Optimize model settings
        5. **Ensemble methods**: Combine multiple models
        
        *These topics will be covered in future levels!*
        """)


def display_level2_complete() -> None:
    """Display Level 2 completion message."""
    st.markdown("---")
    st.success("""
    🎉 **Level 2 Complete!**
    
    Congratulations! You've built your first ML model!
    
    **What you learned:**
    - Feature selection based on correlation
    - Linear Regression fundamentals
    - Model evaluation with RMSE
    - Making predictions with trained models
    
    **Next steps:**
    - Level 3: Add more features & try tree-based models
    - Level 4: Advanced techniques & hyperparameter tuning
    
    Keep learning! 🚀
    """)


def display_comparison_table(df: pd.DataFrame) -> None:
    """Show model vs heuristic comparison."""
    st.subheader("📊 Model vs Simple Heuristic")
    
    model = load_trained_model()
    
    if model is None:
        return
    
    # Compare with district median
    st.markdown("""
    Let's compare our model with a simple heuristic (district median price per m²):
    """)
    
    sample_areas = [30, 60, 84, 100, 150]
    
    results = []
    for area in sample_areas:
        ml_pred = model.predict([[area]])[0]
        results.append({
            "Area (m²)": area,
            "ML Prediction": f"{ml_pred:,.0f}",
        })
    
    st.dataframe(pd.DataFrame(results), use_container_width=True)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        display_header()
        display_learning_goals()
        st.markdown("---")
        display_prediction_interface()
        display_model_limitations()
        st.markdown("---")
        display_comparison_table(df)
        display_level2_complete()
    except Exception as e:
        st.error(f"Failed to load data: {e}")


if __name__ == "__main__":
    main()
