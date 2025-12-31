import streamlit as st

def render_home() -> None:
	"""Render the Home page explaning the portfolio MVP."""
	st.set_page_config(
		page_title="Seoul Apt Price Prediction (Portfolio)",
		layout="wide",
	)
	st.title("Seoul Aprtment Price Prediction - Portfolio MVP")
	st.markdown(
		"""
This is a visual, end-to-end exhibition of a machine learning competition project.

**Target audiences**
- Aspiring AI engineers: learn the workflow (EDA -> feature -> modeling -> evaluation -> reflection)
-General audience: understand the story with visuals and plain explanations
- My future self: track progress and decisions as a growth log

**MVP approach (C-plan)**
- We deploy a lightweight stratified sample dataset (Parquet).
- We do NOT deploy the full raw dataset to keep the app fast and reliable.
"""
	)
	st.info("Use the left sidebar to navigate: Data / EDA / Model / Demo / Retro.")

def main() -> None:
	"""Entry point for Streamlit."""
	render_home()

if __name__ == "__main__":
	main()