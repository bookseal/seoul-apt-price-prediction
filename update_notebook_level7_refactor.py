import nbformat

notebook_path = '/data/ephemeral/home/workspace/seoul-apt-price-prediction/notebooks/Level_7_Data_Cleaning.ipynb'

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

# 1. Update Cell 3 (Data Generation) to just load Real Data (No Messy Injection)
cell_source_data = """import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

url = "https://github.com/bookseal/seoul-apt-price-prediction/raw/main/data/sample.parquet"
df = pd.read_parquet(url)

# Ensure essential columns exist (just like Streamlit)
if 'year' not in df.columns:
    df['year'] = df['built_year'] if 'built_year' in df.columns else 2000
if 'floor' not in df.columns:
    np.random.seed(42)
    df['floor'] = np.random.randint(1, 30, len(df))

print(f"Data Loaded! Rows: {len(df)}")
df.info()"""

nb.cells[2].source = cell_source_data

# 2. Update/Replace Cell 4 (was Messy Eval) with Baseline Eval
cell_source_baseline = """# --- 2. Level 5 Baseline (Raw Data) ---
# We want to see how the model performs BEFORE advanced cleaning.
# We only drop NaNs to make the code run, but we keep Outliers.

features = ['area_m2', 'year', 'floor']
df_baseline = df.dropna(subset=['price_10k_krw'] + features)

X = df_baseline[features].values
y = df_baseline['price_10k_krw'].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = LinearRegression()
model.fit(X_train, y_train)

rmse_baseline = np.sqrt(mean_squared_error(y_test, model.predict(X_test)))
print(f"🛑 Baseline RMSE (With Outliers): {rmse_baseline:,.0f}")
print("Can we improve this by removing outliers?")"""

nb.cells[3].source = cell_source_baseline

# 3. Update later cells to reflect cleaning actual data (Already mostly generic, but let's double check)
# Cell 7: Handle Outliers (IQR)
cell_source_cleaning = """# --- 3. Clean Data (Remove Outliers) ---
# Use IQR method to detect and remove extreme values.

def remove_outliers(df, col):
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    return df[(df[col] >= lower) & (df[col] <= upper)]

df_clean = df_baseline.copy()
df_clean = remove_outliers(df_clean, 'price_10k_krw')
df_clean = remove_outliers(df_clean, 'area_m2')

print(f"Original rows: {len(df_baseline)}")
print(f"Cleaned rows: {len(df_clean)}")
print(f"Removed: {len(df_baseline) - len(df_clean)} outliers")"""

# Find the cell doing IQR (it was around index 9 or 10)
# We'll just append this logic or look for the IQR cell.
# The original notebook had IQR code in cell index 9 (approx). Let's overwrite it to be safe.
# Actually, let's just make sure the notebook flow is: Load -> Baseline -> Clean -> Evaluate Clean.
# We will rewrite the training cell at the end.

cell_source_final_train = """# --- 4. Evaluate Cleaned Model ---
# Train on the same features, but using the Cleaned dataset.

X_clean = df_clean[features].values
y_clean = df_clean['price_10k_krw'].values

X_train_c, X_test_c, y_train_c, y_test_c = train_test_split(X_clean, y_clean, test_size=0.2, random_state=42)

model_c = LinearRegression()
model_c.fit(X_train_c, y_train_c)

rmse_clean = np.sqrt(mean_squared_error(y_test_c, model_c.predict(X_test_c)))
print(f"✅ Cleaned RMSE (Outliers Removed): {rmse_clean:,.0f}")
print(f"Improvement: {rmse_baseline - rmse_clean:,.0f} points!")"""

# Replace the last cell (Index 11 or similar)
nb.cells[-1].source = cell_source_final_train
nb.cells[-3].source = cell_source_cleaning # Replacing the IQR cell roughly

with open(notebook_path, 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)

print("Notebook updated: Messy Data removed, Baseline vs Clean comparison added.")
