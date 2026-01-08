import streamlit as st
import pandas as pd
from src.io import load_sample_dataset


def display_data_header() -> None:
    """
    Render the page title and a brief vision for the dataset.

    Visual Guide:
    [ Header ] -> [ Context ] -> [ Data Table ] -> [ Statistics ]
    """
    st.title("📂 Data: The Foundation of Prediction")
    
    # Explain Stratified Sampling Simply
    st.markdown("### 🎯 What is Stratified Sampling?")
    st.markdown("""
    Imagine you have 1.1 million apartment sales, but you want a smaller, faster dataset that still represents all neighborhoods fairly.
    
    **Regular Random Sampling** might accidentally skip small neighborhoods.  
    **Stratified Sampling** ensures every district and year gets proportional representation.
    """)
    
    # Visual comparison
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 15px; background: rgba(244, 67, 54, 0.1); border-radius: 8px; border-left: 4px solid #F44336;">
        <div style="font-weight: bold; color: #F44336; margin-bottom: 8px;">❌ Random Sampling</div>
        <div style="font-size: 13px;">• Might miss small districts<br/>• Year distribution could be skewed<br/>• Some patterns lost</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="padding: 15px; background: rgba(76, 175, 80, 0.1); border-radius: 8px; border-left: 4px solid #4CAF50;">
        <div style="font-weight: bold; color: #4CAF50; margin-bottom: 8px;">✅ Stratified Sampling</div>
        <div style="font-size: 13px;">• Every district represented<br/>• Year distribution preserved<br/>• Statistical integrity intact</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Simple flow diagram
    st.markdown("#### 📊 How We Built the Sample")
    
    flow_col1, flow_col2, flow_col3, flow_col4 = st.columns([3, 1, 3, 1])
    
    with flow_col1:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: rgba(255, 152, 0, 0.1); border-radius: 8px; border: 2px solid #FF9800;">
        <div style="font-size: 20px; margin-bottom: 5px;">📊</div>
        <div style="font-weight: bold; font-size: 13px; color: #FF9800;">Original Data</div>
        <div style="font-size: 11px; margin-top: 5px;">1.1M rows<br/>25 districts × 17 years</div>
        </div>
        """, unsafe_allow_html=True)
    
    with flow_col2:
        st.markdown("<div style='text-align: center; padding-top: 25px; font-size: 20px; color: #1f77b4;'>→</div>", unsafe_allow_html=True)
    
    with flow_col3:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: rgba(33, 150, 243, 0.1); border-radius: 8px; border: 2px solid #2196F3;">
        <div style="font-size: 20px; margin-bottom: 5px;">⚙️</div>
        <div style="font-weight: bold; font-size: 13px; color: #2196F3;">Group & Sample</div>
        <div style="font-size: 11px; margin-top: 5px;">Each district-year pair<br/>gets proportional share</div>
        </div>
        """, unsafe_allow_html=True)
    
    with flow_col4:
        st.markdown("<div style='text-align: center; padding-top: 25px; font-size: 20px; color: #1f77b4;'>→</div>", unsafe_allow_html=True)
    
    st.markdown("""
    <div style="text-align: center; padding: 15px; background: rgba(76, 175, 80, 0.1); border-radius: 8px; border: 2px solid #4CAF50; margin-top: 10px;">
    <div style="font-size: 20px; margin-bottom: 5px;">✨</div>
    <div style="font-weight: bold; font-size: 13px; color: #4CAF50;">Balanced Sample</div>
    <div style="font-size: 11px; margin-top: 5px;">100k rows • Same distributions • 260× smaller</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    st.markdown(
        """
    **Result:** A fast, lightweight dataset that maintains the statistical integrity of Seoul's real estate market.
    """
    )


def display_dataset_summary(df: pd.DataFrame) -> None:
    """
    Display key metrics and the sampling logic.

    ASCII Infographic:
    [ Population: 1.1M ] --- (Stratified Sampling) ---> [ Sample: 100k ]
                                    |
                            [ Key: District x Year ]
    """
    st.subheader("📊 Dataset Summary")

    col1, col2, col3 = st.columns(3)
    col1.metric("Sample Size", f"{len(df):,}")
    col2.metric("Total Districts", f"{df['district'].nunique()}")
    col3.metric("Year Range", f"{df['year'].min()} - {df['year'].max()}")

    with st.expander("Why Stratified Sampling?"):
        st.write(
            """
        We grouped the data by **District** and **Contract Year** to ensure that 
        small but important areas are not lost during the reduction process.
        """
        )


def render_data_page() -> None:
    """
    Orchestrate the rendering process of the Data page.

    Pipeline:
    1. Load Data (with Caching)
    2. Display English Header
    3. Show Sampled Rows
    4. Provide Descriptive Statistics
    """
    try:
        # 1. Load Data (with Caching)
        df = load_sample_dataset()

        # 2. Render Header & Summary
        display_data_header()
        display_dataset_summary(df)

        # 3. Show Raw Data Sample
        st.subheader("🔍 Data Preview (First 10 Rows)")
        st.dataframe(df.head(10), use_container_width=True)

        # 4. Show Statistics
        st.subheader("📈 Numerical Statistics")
        st.write(df.describe())

    except Exception as e:
        st.error(f"Error loading dataset: {e}")
        st.info(
            "Please ensure the sample dataset is generated via the sampling pipeline."
        )


def main() -> None:
    """Entry point for this page."""
    render_data_page()


if __name__ == "__main__":
    main()
