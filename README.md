# 🏢 Seoul Apartment Price Prediction

A portfolio-friendly, production-lean ML app to explore, sample, and preview Seoul apartment prices. It focuses on fast UX, clean engineering, and deployability on Streamlit Cloud.

## 🚀 What You Get
- Fast Streamlit app with a stratified 100k Parquet sample (from 1.1M+ rows)
- Clear EDA visuals (distribution, district medians)
- A demo page that estimates price using a heuristic placeholder (model slot-in)
- Reproducible sampling pipeline and lightweight tests

## 📦 Repository Structure
```
app.py                       # Streamlit entry (Home)
pages/
	1_Data.py                 # Data summary, preview, stats
	2_EDA.py                  # Distribution + district analysis
	3_Model.py                # Modeling plan placeholder
	4_Demo.py                 # Interactive price estimate demo
	5_Retro.py                # Decisions, trade-offs, next steps (exportable)
src/
	io.py                     # Cached Parquet loader
	plots.py                  # Plotly charts used in EDA
	sampling.py               # Stratified sampling pipeline
data/
	sample.parquet            # 100k stratified sample for the app
tests/
	2026-01-02_test_sampling.py
	2026-01-02_12-07-19_analyze_compression_factors.py
	README.md
.streamlit/config.toml       # Streamlit runtime config
requirements.txt             # Minimal deps for Streamlit Cloud
```

## 🧪 Data Sampling Pipeline (1.1M ➜ 100k)
We avoid loading the 1.1M-row CSV at runtime. Instead, we create a representative Parquet sample using stratified sampling by `(district, year)`.

Key ideas:
- Parquet is columnar → dramatically faster I/O on Streamlit
- Stratified sampling preserves distribution across 25 districts and 17 years
- Small, git-tracked asset (`~951KB`) keeps the app responsive in the cloud

Generate a sample locally (if you need to re-build):
```bash
python src/sampling.py
```
This reads `data/raw/train.csv`, renames columns to English, builds a 100k stratified sample, and writes `data/sample.parquet`.

Tests are provided to validate size, distribution preservation, and compression factors:
```bash
python tests/2026-01-02_test_sampling.py
python tests/2026-01-02_12-07-19_analyze_compression_factors.py
```

## 📊 App Pages
- Data: overview of the sample, quick metrics, preview, and stats
- EDA: price distribution histogram and median price by district
- Model: modeling plan placeholder (slot for XGBoost/scikit-learn)
- Demo: interactive mock inference using district median price-per-㎡
- Retro: documented decisions/trade-offs; export as Markdown

## ▶️ Run Locally
```bash
pip install -r requirements.txt
streamlit run app.py
```

## ☁️ Deploy on Streamlit Cloud
1) Connect repo `bookseal/seoul-apt-price-prediction` in Streamlit Cloud.
2) App file: `app.py`, Branch: `main`.
3) Optional: ensure `.streamlit/config.toml` is present.
4) Click Deploy. First boot installs `requirements.txt` and serves the app.

Optional pin for runtime (add at repo root):
```
runtime.txt
python-3.11
```

## 🧰 Tech Stack
- Python, Streamlit, Pandas, PyArrow, Plotly
- (Planned) scikit-learn / XGBoost for modeling

## 🙌 Credits
Built by Ki-chan as a portfolio-style, solution-architected ML app.