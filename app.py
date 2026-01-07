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

## 🔄 End-to-End Data Flow
	""")
	
	# Mermaid diagram showing the architecture
	st.markdown("""
```mermaid
graph LR
    A["📊 Raw CSV<br/>(1.1M rows)"] -->|Stratified Sampling| B["📦 Parquet Sample<br/>(100k rows)"]
    B -->|Fast I/O| C["🎨 Streamlit App"]
    C --> D["📈 Data Page"]
    C --> E["🔍 EDA Visuals"]
    C --> F["🤖 Model Demo"]
    C --> G["💭 Retro Reflection"]
    D --> H["✅ Deploy to Cloud"]
    E --> H
    F --> H
    G --> H
    H -->|Live App| I["🚀 bookseal-seoul-apt-price-prediction.streamlit.app"]
    style A fill:#fff4e6
    style B fill:#e8f4f8
    style C fill:#f0f0f0
    style I fill:#c8e6c9
```
	""")
	
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

## 🚀 Navigation

Use the left sidebar to explore the app:
	""")
	
	col1, col2, col3 = st.columns(3)
	with col1:
		st.markdown("""
**Explore Data**
- [📊 Data](./1_Data) – sample overview
- [🔍 EDA](./2_EDA) – distribution insights
		""")
	with col2:
		st.markdown("""
**Try Features**
- [🤖 Model](./3_Model) – architecture plan
- [🎮 Demo](./4_Demo) – live estimator
		""")
	with col3:
		st.markdown("""
**Reflect & Learn**
- [💭 Retro](./5_Retro) – decisions & trade-offs
- [📚 README](https://github.com/bookseal/seoul-apt-price-prediction) – full details
		""")
	
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