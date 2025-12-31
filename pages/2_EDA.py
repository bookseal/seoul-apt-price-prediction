import streamlit as st

from src.io import load_sample_dataset
from src_plots import plot_median_price_by_district, plot_price_histogram

def render_eda_page() -> None:
	"""Render minial but meaningful EDA charts for the sample dataset."""
	st.title("EDA (Exploratory Data Analysis)")

def main() -> None:
	"""Entry point for this page."""
	render_eda_page()

main()