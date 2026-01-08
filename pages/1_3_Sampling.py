# -*- coding: utf-8 -*-
"""
Level 1.3: Understanding Sampling

Learn efficient methods to handle large datasets.
Understand the concept and necessity of Stratified Sampling.
"""
import streamlit as st
import pandas as pd
from src.io import load_sample_dataset


def display_learning_goals() -> None:
    """Display learning objectives for this chapter."""
    st.info("""
    **🎯 What You'll Learn**
    - Why we use samples instead of full data
    - Random Sampling vs Stratified Sampling
    - Benefits of Parquet file format
    """)


def display_header() -> None:
    """Render page title and introduction."""
    st.title("🎲 1.3 Understanding Sampling")
    
    st.markdown("""
    ### Why Do We Need Sampling?
    
    Our original data has **1.1 million+ rows**.
    Loading this every time makes the app slow and uses lots of memory.
    
    So we create a **100K row** sample!
    """)


def display_sampling_comparison() -> None:
    """Compare random and stratified sampling."""
    st.subheader("🔄 Sampling Methods Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 15px; background: rgba(244, 67, 54, 0.1); 
                    border-radius: 8px; border-left: 4px solid #F44336;">
        <div style="font-weight: bold; color: #F44336; margin-bottom: 8px;">
        ❌ Simple Random Sampling
        </div>
        <div style="font-size: 13px;">
        • Randomly picks from entire dataset<br/>
        • Small groups may be missed<br/>
        • Distribution may be distorted
        </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="padding: 15px; background: rgba(76, 175, 80, 0.1); 
                    border-radius: 8px; border-left: 4px solid #4CAF50;">
        <div style="font-weight: bold; color: #4CAF50; margin-bottom: 8px;">
        ✅ Stratified Sampling
        </div>
        <div style="font-size: 13px;">
        • Maintains proportions by group<br/>
        • All districts/years represented<br/>
        • Statistical integrity preserved
        </div>
        </div>
        """, unsafe_allow_html=True)


def display_stratified_example(df: pd.DataFrame) -> None:
    """Show real example of stratified sample distribution."""
    st.subheader("📊 Our Sample Data Distribution")
    
    st.markdown("""
    Our sample maintains proportions by **(district × year)** combinations.
    No district or year is missing!
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Rows by District (Top 10)**")
        district_counts = df['district'].value_counts().head(10)
        st.bar_chart(district_counts)
    
    with col2:
        st.markdown("**Rows by Year**")
        year_counts = df['year'].value_counts().sort_index()
        st.line_chart(year_counts)


def display_parquet_benefits() -> None:
    """Explain Parquet file format benefits."""
    st.subheader("📦 What is Parquet?")
    
    st.markdown("""
    Parquet is a **columnar storage** file format.
    
    | Comparison | CSV | Parquet |
    |------------|-----|---------|
    | Storage | Row-based | Column-based |
    | File Size | Large | Small (compressed) |
    | Read Speed | Slow | Fast (50-100x) |
    | Type Preservation | ❌ | ✅ |
    """)
    
    with st.expander("🤔 Why is columnar storage faster?"):
        st.markdown("""
        If you only need the `price_10k_krw` column:
        
        - **CSV**: Read all rows, extract needed column → Slow
        - **Parquet**: Read only that column directly → Fast
        
        In data analysis, we usually use only some columns,
        so Parquet is much more efficient!
        """)


def display_pipeline_diagram() -> None:
    """Show data pipeline visually."""
    st.subheader("🔄 Our Data Pipeline")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.markdown("""
        <div style="text-align: center; padding: 15px; 
                    background: rgba(255, 152, 0, 0.15); border-radius: 8px;">
        <div style="font-size: 24px;">📊</div>
        <div style="font-weight: bold; font-size: 11px;">Raw CSV</div>
        <div style="font-size: 10px; color: gray;">1.1M rows</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("<div style='text-align: center; padding-top: 20px;'>→</div>", 
                    unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="text-align: center; padding: 15px; 
                    background: rgba(33, 150, 243, 0.15); border-radius: 8px;">
        <div style="font-size: 24px;">⚙️</div>
        <div style="font-weight: bold; font-size: 11px;">Stratified</div>
        <div style="font-size: 10px; color: gray;">district × year</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("<div style='text-align: center; padding-top: 20px;'>→</div>", 
                    unsafe_allow_html=True)
    
    with col5:
        st.markdown("""
        <div style="text-align: center; padding: 15px; 
                    background: rgba(76, 175, 80, 0.15); border-radius: 8px;">
        <div style="font-size: 24px;">📦</div>
        <div style="font-weight: bold; font-size: 11px;">Parquet</div>
        <div style="font-size: 10px; color: gray;">100K rows</div>
        </div>
        """, unsafe_allow_html=True)


def display_level1_complete() -> None:
    """Display Level 1 completion message."""
    st.markdown("---")
    st.success("""
    🎉 **Level 1 Complete!**
    
    Congratulations! You now understand:
    - Data structure and basic statistics
    - Discovering patterns with EDA
    - Efficient data processing methods
    
    **Next**: In Level 2, let's build our first ML model! 🚀
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        display_header()
        display_learning_goals()
        st.markdown("---")
        display_sampling_comparison()
        st.markdown("---")
        display_stratified_example(df)
        st.markdown("---")
        display_parquet_benefits()
        st.markdown("---")
        display_pipeline_diagram()
        display_level1_complete()
    except Exception as e:
        st.error(f"Failed to load data: {e}")


if __name__ == "__main__":
    main()
