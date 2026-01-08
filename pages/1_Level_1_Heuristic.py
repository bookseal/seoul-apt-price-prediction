# -*- coding: utf-8 -*-
"""
Level 1: Heuristic Prediction (No ML)

The simplest end-to-end prediction using district median price per m².
Formula: Predicted Price = Median Price per m² (by district) × Area
"""
import streamlit as st
import pandas as pd
from src.io import load_sample_dataset


def display_header() -> None:
    """Display Level 1 introduction."""
    st.title("🎯 Level 1: Heuristic Prediction")
    
    st.success("""
    **Goal**: Predict apartment price using the simplest possible method.
    
    No machine learning needed - just basic math!
    """)


def display_method() -> None:
    """Explain the heuristic method."""
    st.header("📐 The Method")
    
    st.markdown("""
    ### Simple Logic
    
    1. Calculate the **median price per m²** for each district
    2. Multiply by the apartment's **area**
    
    That's it! No training, no algorithms.
    """)
    
    st.latex(r"\text{Price} = \text{Median Price per m}^2 \text{ (district)} \times \text{Area}")
    
    with st.expander("🤔 Why does this work?"):
        st.markdown("""
        - **Location matters most** in real estate
        - Apartments in the same district have similar price per m²
        - Median is robust to outliers (unlike mean)
        
        This is our **baseline** - any ML model should beat this!
        """)


def display_data_preview(df: pd.DataFrame) -> None:
    """Show data overview."""
    st.header("📊 Data Overview")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Rows", f"{len(df):,}")
    col2.metric("Districts", f"{df['district'].nunique()}")
    col3.metric("Year Range", f"{df['year'].min()}-{df['year'].max()}")
    
    with st.expander("View sample data"):
        st.dataframe(df.head(10), use_container_width=True)


def display_district_stats(df: pd.DataFrame) -> None:
    """Show median price per m² by district."""
    st.header("📍 Price by District")
    
    # Calculate price per m²
    df_calc = df.copy()
    df_calc['price_per_m2'] = df_calc['price_10k_krw'] / df_calc['area_m2']
    
    # Get median by district
    district_stats = df_calc.groupby('district')['price_per_m2'].median().sort_values(ascending=False)
    
    st.bar_chart(district_stats)
    
    st.caption("Median price per m² (10K KRW) by district")


def predict_heuristic(df: pd.DataFrame, district: str, area: float) -> float:
    """
    Calculate heuristic price prediction.
    
    Args:
        df: Sample dataset
        district: Selected district
        area: Exclusive area in m²
    
    Returns:
        Predicted price in 10K KRW
    """
    df_calc = df.copy()
    df_calc['price_per_m2'] = df_calc['price_10k_krw'] / df_calc['area_m2']
    median_price_per_m2 = df_calc[df_calc['district'] == district]['price_per_m2'].median()
    return median_price_per_m2 * area


def display_demo(df: pd.DataFrame) -> None:
    """Interactive prediction demo."""
    st.header("🔮 Try It Yourself")
    
    col1, col2 = st.columns(2)
    
    with col1:
        districts = sorted(df['district'].unique())
        selected_district = st.selectbox("Select District", districts)
    
    with col2:
        selected_area = st.slider("Exclusive Area (m²)", 
                                   min_value=10, max_value=200, value=84)
    
    # Predict
    predicted_price = predict_heuristic(df, selected_district, selected_area)
    
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("District", selected_district)
    
    with col2:
        st.metric("Area", f"{selected_area} m²")
    
    with col3:
        st.metric("Predicted Price", f"{predicted_price:,.0f} (10K KRW)")
    
    # Show calculation
    df_calc = df.copy()
    df_calc['price_per_m2'] = df_calc['price_10k_krw'] / df_calc['area_m2']
    median_ppm2 = df_calc[df_calc['district'] == selected_district]['price_per_m2'].median()
    
    st.info(f"""
    **Calculation**:
    - Median price/m² in {selected_district}: **{median_ppm2:,.0f}**
    - Area: **{selected_area}** m²
    - Result: {median_ppm2:,.0f} × {selected_area} = **{predicted_price:,.0f}** (10K KRW)
    - ≈ **{predicted_price/100:.1f} billion KRW**
    """)


def display_limitations() -> None:
    """Show method limitations."""
    st.header("⚠️ Limitations")
    
    st.warning("""
    **This method ignores:**
    - Floor number (higher floors often cost more)
    - Building age (newer = more expensive)
    - Specific apartment complex
    - Market trends over time
    
    **Next Level**: Use machine learning to improve predictions!
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_method()
        st.markdown("---")
        display_data_preview(df)
        st.markdown("---")
        display_district_stats(df)
        st.markdown("---")
        display_demo(df)
        st.markdown("---")
        display_limitations()
        
    except Exception as e:
        st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
