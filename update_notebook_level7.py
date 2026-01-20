import json
import nbformat

notebook_path = '/data/ephemeral/home/workspace/seoul-apt-price-prediction/notebooks/Level_7_Data_Cleaning.ipynb'

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

# 1. Update Cell 3 (Data Generation) - Index 2
cell_source_data_gen = """import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

url = "https://github.com/bookseal/seoul-apt-price-prediction/raw/main/data/sample.parquet"
df = pd.read_parquet(url)

# --- 1. Create Messy Data (Simulation) ---
# We use the EXACT same logic as the Streamlit App to reproduce the ~62k RMSE.
np.random.seed(42)
n = len(df)

# Add year and floor if not present (Simulation)
if 'year' not in df.columns:
    df['year'] = np.random.randint(1985, 2024, n)
if 'floor' not in df.columns:
    df['floor'] = np.random.randint(1, 30, n)

# Create missing values (5% random)
mask_null_floor = np.random.random(n) < 0.05
df.loc[mask_null_floor, 'floor'] = np.nan

mask_null_year = np.random.random(n) < 0.03
df.loc[mask_null_year, 'year'] = np.nan

# Create outliers (1% extreme values)
mask_outlier_price = np.random.random(n) < 0.01
df.loc[mask_outlier_price, 'price_10k_krw'] = df.loc[mask_outlier_price, 'price_10k_krw'] * np.random.uniform(5, 10, sum(mask_outlier_price))

mask_outlier_area = np.random.random(n) < 0.01
df.loc[mask_outlier_area, 'area_m2'] = df.loc[mask_outlier_area, 'area_m2'] * np.random.uniform(5, 10, sum(mask_outlier_area))

print("Messy Data Created with matching Logic!")
df.info()"""

nb.cells[2].source = cell_source_data_gen

# 2. Add New Cell (Messy Data Evaluation) - Insert after Index 2
cell_source_messy_eval = """# --- 2. Check Fairness: How bad is the Messy Data? ---
# Let's train a model on this "garbage" data to see the baseline error.
# We drop NaNs just to make the code run, but we keep the Outliers.

df_messy_train = df.dropna(subset=['price_10k_krw', 'area_m2', 'year', 'floor'])

X_messy = df_messy_train[['area_m2', 'year', 'floor']].values
y_messy = df_messy_train['price_10k_krw'].values

X_train_m, X_test_m, y_train_m, y_test_m = train_test_split(X_messy, y_messy, test_size=0.2, random_state=42)

model_m = LinearRegression()
model_m.fit(X_train_m, y_train_m)

rmse_m = np.sqrt(mean_squared_error(y_test_m, model_m.predict(X_test_m)))
print(f"🛑 RMSE with Messy Data (Outliers included): {rmse_m:,.0f}")
print("This explains why we need Level 7 Data Cleaning!")"""

new_cell = nbformat.v4.new_code_cell(cell_source_messy_eval)
nb.cells.insert(3, new_cell)

with open(notebook_path, 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)

print("Notebook updated successfully!")
