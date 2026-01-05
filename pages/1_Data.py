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
    st.markdown("""
    To ensure a fast and interactive experience, we use a **Stratified Sample** of the original 1.1M+ records. This allows us to keep the app lightweight 
    while maintaining the statistical integrity of Seoul's real estate market.
    """)

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
        st.write("""
        We grouped the data by **District** and **Contract Year** to ensure that 
        small but important areas are not lost during the reduction process.
        """)

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
		st.info("Please ensure the sample dataset is generated via the sampling pipeline.")
	
def main() -> None:
	"""Entry point for this page."""
	render_data_page()

if __name__ == "__main__":
	main()