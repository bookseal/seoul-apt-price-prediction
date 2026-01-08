# -*- coding: utf-8 -*-
"""
Level 1.1: Explore the Data

Introduction to Seoul apartment transaction data for beginners.
Goal: Understand data structure and basic statistics.
"""
import streamlit as st
import pandas as pd
from src.io import load_sample_dataset


def display_learning_goals() -> None:
    """Display learning objectives for this chapter."""
    st.info("""
    **🎯 What You'll Learn**
    - How to examine dataset structure (rows, columns, types)
    - Understanding basic statistics (mean, median, std)
    - Building intuition through data preview
    """)


def display_data_header() -> None:
    """Render page title and dataset introduction."""
    st.title("📂 1.1 Explore the Data")
    
    st.markdown("""
    ### Our Dataset
    
    This is Seoul apartment **real transaction price** data.
    It contains actual sale prices, making it ideal for real estate market analysis.
    """)
    
    with st.expander("📋 Column Descriptions"):
        st.markdown("""
        | Column | Description | Example |
        |--------|-------------|---------|
        | `district` | Seoul district name | Gangnam-gu, Seocho-gu |
        | `area_m2` | Exclusive area (㎡) | 84.5 |
        | `price_10k_krw` | Transaction price (10K KRW) | 120000 |
        | `year` | Contract year | 2023 |
        | `floor` | Floor number | 15 |
        """)


def display_dataset_summary(df: pd.DataFrame) -> None:
    """Display basic dataset information summary."""
    st.subheader("📊 Dataset Summary")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Rows", f"{len(df):,}")
    col2.metric("Districts", f"{df['district'].nunique()}")
    col3.metric("Year Range", f"{df['year'].min()} - {df['year'].max()}")
    
    st.markdown("---")
    
    st.subheader("📈 Basic Statistics")
    st.markdown("> **Tip**: `mean` = average, `std` = standard deviation, `50%` = median")
    st.dataframe(df.describe(), use_container_width=True)


def display_data_preview(df: pd.DataFrame) -> None:
    """Show first few rows of the data."""
    st.subheader("🔍 Data Preview")
    
    n_rows = st.slider("Number of rows to display", min_value=5, max_value=50, value=10)
    st.dataframe(df.head(n_rows), use_container_width=True)


def display_quiz() -> None:
    """Simple quiz to check understanding."""
    st.markdown("---")
    st.subheader("✅ Knowledge Check")
    
    with st.expander("Quiz: Answer based on the data above"):
        st.markdown("""
        1. How many districts are in this dataset?
        2. What is the unit of `price_10k_krw`?
        3. What is the year range of the data?
        """)
        
        if st.button("Show Answers"):
            st.success("""
            1. 25 districts
            2. 10,000 KRW (10K Korean Won)
            3. Check the 'Year Range' metric above!
            """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        display_data_header()
        display_learning_goals()
        st.markdown("---")
        display_dataset_summary(df)
        st.markdown("---")
        display_data_preview(df)
        display_quiz()
    except Exception as e:
        st.error(f"Failed to load data: {e}")


if __name__ == "__main__":
    main()
