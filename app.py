import streamlit as st

def render_home() -> None:
	"""Render the Home page explaining the portfolio MVP."""
	st.set_page_config(
		page_title="Seoul Apt Price Prediction (Portfolio)",
		layout="wide",
	)
	st.title("🏙️ Seoul Apartment Price Prediction - Portfolio MVP")
	
	st.markdown("""
## 📖 The Story
Imagine you're a data engineer tasked with building a *lean, deployable* ML app that estimates Seoul apartment prices. 
The challenge? The dataset has **1.1M+ rows**, but you need to ship fast without sacrificing accuracy or insights.

**The Solution:** Stratified sampling + Parquet compression + interactive exploration.

---

## 🔄 End-to-End Pipeline
	""")
	
	# Visual flow using columns (dark-mode compatible colors)
	col1, col2, col3, col4, col5 = st.columns(5)
	
	with col1:
		st.markdown("""
		<div style="text-align: center; padding: 20px; background: rgba(255, 152, 0, 0.15); border-radius: 8px; border-left: 4px solid #FF9800;">
		<div style="font-size: 24px; margin-bottom: 8px;">📊</div>
		<div style="font-weight: bold; font-size: 12px; color: #FF9800;">Raw CSV</div>
		<div style="font-size: 10px; color: rgba(255, 152, 0, 0.7); margin-top: 4px;">1.1M rows</div>
		</div>
		""", unsafe_allow_html=True)
	
	with col2:
		st.markdown("""
		<div style="text-align: center; padding: 20px;">
		<div style="font-size: 20px; color: #00BCD4;">↓</div>
		<div style="font-size: 11px; color: #00BCD4; font-weight: bold;">Stratified<br/>Sampling</div>
		</div>
		""", unsafe_allow_html=True)
	
	with col3:
		st.markdown("""
		<div style="text-align: center; padding: 20px; background: rgba(0, 188, 212, 0.15); border-radius: 8px; border-left: 4px solid #00BCD4;">
		<div style="font-size: 24px; margin-bottom: 8px;">📦</div>
		<div style="font-weight: bold; font-size: 12px; color: #00BCD4;">Parquet</div>
		<div style="font-size: 10px; color: rgba(0, 188, 212, 0.7); margin-top: 4px;">100k rows</div>
		</div>
		""", unsafe_allow_html=True)
	
	with col4:
		st.markdown("""
		<div style="text-align: center; padding: 20px;">
		<div style="font-size: 20px; color: #4CAF50;">↓</div>
		<div style="font-size: 11px; color: #4CAF50; font-weight: bold;">Fast<br/>I/O</div>
		</div>
		""", unsafe_allow_html=True)
	
	with col5:
		st.markdown("""
		<div style="text-align: center; padding: 20px; background: rgba(76, 175, 80, 0.15); border-radius: 8px; border-left: 4px solid #4CAF50;">
		<div style="font-size: 24px; margin-bottom: 8px;">🚀</div>
		<div style="font-weight: bold; font-size: 12px; color: #4CAF50;">Streamlit</div>
		<div style="font-size: 10px; color: rgba(76, 175, 80, 0.7); margin-top: 4px;">Live App</div>
		</div>
		""", unsafe_allow_html=True)
	
	st.markdown("""
---

## 🎯 What You'll Explore

| Page | Purpose | Key Insight |
|------|---------|------------|
| **📊 Data** | Load and inspect the 100k sample | Fast Parquet I/O, stratified distribution |
| **🔍 EDA** | Price distribution & district analysis | Right-skewed prices, geographic patterns |
| **🤖 Model** | Placeholder for ML integration | Where XGBoost/scikit-learn lives |
| **🎮 Demo** | Interactive price estimator | Real-time heuristic (district median × area) |
| **💭 Retro** | Document decisions & trade-offs | Reflection on MVP strategy |

---

## 🎓 Why This Approach?

**The Traditional Problem:**
- Load 1.1M rows → slow startup, memory bloat, deployment friction
- Deploy raw CSV → brittle, doesn't scale

**Our C-Plan Solution:**
1. **Stratified sampling** by district & year → preserves statistical distribution
2. **Parquet format** → columnar I/O, ~50–100× faster than CSV
3. **Git-tracked sample** → reproducible, auditable, version-controlled
4. **Lightweight deployment** → fits Streamlit Cloud, instant cold starts

**Result:** A fast, maintainable, educational MVP that showcases engineering mindset.

---

## 🚀 Quick Navigation

	""")
	
	col1, col2, col3 = st.columns(3)
	with col1:
		st.markdown("**Explore Data**")
		st.page_link("pages/1_Data.py", label="📊 Data – sample overview")
		st.page_link("pages/2_EDA.py", label="🔍 EDA – distribution insights")
	
	with col2:
		st.markdown("**Try Features**")
		st.page_link("pages/3_Model.py", label="🤖 Model – architecture plan")
		st.page_link("pages/4_Demo.py", label="🎮 Demo – live estimator")
	
	with col3:
		st.markdown("**Reflect & Learn**")
		st.page_link("pages/5_Retro.py", label="💭 Retro – decisions & trade-offs")
		st.link_button("📚 GitHub", "https://github.com/bookseal/seoul-apt-price-prediction")
	
	st.divider()
	
	st.markdown("""
### 🔗 Live Demo
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://bookseal-seoul-apt-price-prediction.streamlit.app)

**Built with:** Python · Streamlit · Pandas · PyArrow · Plotly

---

*Portfolio project showcasing lean ML engineering, strategic sampling, and end-to-end deployment.*
	""")

def main() -> None:
	"""Entry point for Streamlit."""
	render_home()

if __name__ == "__main__":
	main()