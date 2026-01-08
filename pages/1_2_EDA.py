# -*- coding: utf-8 -*-
"""
Level 1.2: Exploratory Data Analysis (EDA)

Visualize data distributions and patterns.
EDA is about 'listening' to what the data says before building models.
"""
import streamlit as st
from src.io import load_sample_dataset
from src.plots import plot_median_price_by_district, plot_price_histogram


def display_learning_goals() -> None:
    """Display learning objectives for this chapter."""
    st.info("""
    **🎯 What You'll Learn**
    - Understanding price distribution with histograms
    - Analyzing regional characteristics via median prices
    - Understanding data 'Skewness' concept
    """)


def display_eda_header() -> None:
    """Render page title and EDA introduction."""
    st.title("📊 1.2 Exploratory Data Analysis")
    
    st.markdown("""
    ### What is EDA?
    
    **Exploratory Data Analysis** is the process of 'listening' to your data
    before building models. We discover patterns through charts and statistics.
    """)


def display_price_distribution(df) -> None:
    """Visualize price distribution with histogram."""
    st.subheader("💰 Price Distribution (Histogram)")
    
    st.markdown("""
    > **Histogram**: Shows how data is distributed across value ranges
    
    Apartment prices typically show a **right-skewed** pattern.
    Most apartments are low-to-mid priced, but some ultra-expensive ones create a long tail.
    """)
    
    fig = plot_price_histogram(df)
    st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("🤔 Why is it right-skewed?"):
        st.markdown("""
        1. **Lower bound exists, no upper bound**: Prices can't go below 0,
           but can reach billions
        2. **Premium apartments**: Gangnam, Seocho districts pull the average up
        3. **For skewed data, median is more representative than mean!**
        """)


def display_district_analysis(df) -> None:
    """Visualize median prices by district."""
    st.subheader("📍 Price Comparison by District")
    
    st.markdown("""
    > **Real Estate Rule #1**: Location, Location, Location!
    
    Seoul's 25 districts have vastly different prices.
    We use **Median** here - it's less affected by extreme outliers.
    """)
    
    fig = plot_median_price_by_district(df)
    st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("💡 Think about the chart"):
        st.markdown("""
        - Which district is the most expensive?
        - Which district is the cheapest?
        - What do expensive districts have in common? (Hint: Gangnam 3)
        """)


def display_quiz() -> None:
    """EDA-related quiz."""
    st.markdown("---")
    st.subheader("✅ Knowledge Check")
    
    with st.expander("Quiz: EDA Concepts"):
        st.markdown("""
        1. In right-skewed data, which is larger: mean or median?
        2. Why do we prefer median for real estate data?
        3. Why should we do EDA before modeling?
        """)
        
        if st.button("Show Answers"):
            st.success("""
            1. **Mean is larger** - expensive apartments pull up the average
            2. **Less sensitive to outliers**, making it more representative
            3. **Understanding data characteristics** helps choose the right model
            """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        display_eda_header()
        display_learning_goals()
        st.markdown("---")
        display_price_distribution(df)
        st.markdown("---")
        display_district_analysis(df)
        display_quiz()
    except Exception as e:
        st.error(f"Failed to load data: {e}")


if __name__ == "__main__":
    main()
