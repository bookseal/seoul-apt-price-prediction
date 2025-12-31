import pandas as pd
import plotly.express as px

def ensure_district_column(df: pd.DataFrame) -> pd.DataFrame:
	"""
	Ensure the DataFrame has a 'district' column parsed from 'sigungu'.

	Expected format example:
	- sigungu: "서울특별시 강남구 개포동"
	- district: "강남구"
	"""
	if "district" in df.columns:
		return df
	
	out = df.copy()
	if "singungu" not in out.columns:
		out["district"] = "Unknown"
		return out
	
	out["district"] = out["singungu"].astype(str).str.split().str[1].fillna("Unknown")
	return out

def plot_price_histogram(df: pd.DataFrame):
	"""
	Plot the distribution of transaction prices.

	This helps check:
	- overall skewness
	- tails / extreme values
	"""
	return px.histogram(
		df,
		x="price_10k_krw",
		nbins=60,
		title="Transaction Price Distribution (10,000 KRW unit)",
	)

def plot_median_price_by_district(df: pd.DataFrame):
	"""
	Plot median transaction price per district.

	This shows:
	- relative price levels across districts
	"""
	df2 = ensure_district_column(df)
	g = (
		df2.groupby("district", as_index=False)["price_10k_krw"]
		.median()
		.sort_values("price_10k_krw", ascending=False)
	)
	return px.bar(
		g,
		x="district",
		y="price_10k_krw",
		title="Median Transaction Price by District (10,000 KRW unit)",
	)
