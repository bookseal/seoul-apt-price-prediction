import pandas as pd
import streamlit as st

@st.cache_data
def load_sample_dataset(path: str = "data/sample.parquet") -> pd.DataFrame:
	"""
	Load the lightweight sample dataset stored as Parquet.

	Why caching?
	- Streamlit reruns scripts on UI interactions.
	- Caching prevents re-reading the same file repeatedly.
	"""
	return pd.read_parquet(path)