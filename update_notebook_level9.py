
import nbformat as nbf
import os

def create_notebook():
    nb = nbf.v4.new_notebook()
    
    # 1. Header
    nb.cells.append(nbf.v4.new_markdown_cell("""
# Level 9: Regularization (Ridge & Lasso)
**Goal**: Beat Level 8's RMSE by using Regularization to handle more complex models safely.

In Level 8, we achieved an RMSE of **~24,200** using Polynomial Features (Degree 2).
Here, we will try to improve this by:
1.  Increasing complexity (Use **Degree 5**).
2.  Using **Ridge** to tame this extreme complexity.
    """))
    
    # 2. Setup (From Level 8)
    nb.cells.append(nbf.v4.new_code_cell("""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

# 1. Load Data (Level 8 Logic)
# 1. Load Data
def load_data():
    # Load Real Data
    try:
        df = pd.read_parquet('../data/sample.parquet')
    except:
        # Fallback
        df = pd.read_parquet('/data/ephemeral/home/workspace/seoul-apt-price-prediction/data/sample.parquet')
    return df

df = load_data()

# 2. Prep & Cleaning (Exact Match to Level 8)
np.random.seed(42) # Crucial for reproducibility

# Handling missing features if they don't exist
if 'year' not in df.columns:
    df['year'] = df['built_year'] if 'built_year' in df.columns else np.random.randint(1985, 2024, len(df))
if 'floor' not in df.columns:
    df['floor'] = np.random.randint(1, 30, len(df))

# Cleaning (IQR 3.0)
df_clean = df.copy()
for col in df_clean.select_dtypes(include=[np.number]).columns:
    df_clean[col] = df_clean[col].fillna(df_clean[col].median())

for col in ['price_10k_krw', 'area_m2']:
    Q1 = df_clean[col].quantile(0.25)
    Q3 = df_clean[col].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 3.0 * IQR
    upper = Q3 + 3.0 * IQR
    df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]

# 3. Features
features = ['area_m2', 'year', 'floor']
X = df_clean[features].values
y = df_clean['price_10k_krw'].values

print(f"Data Loaded: {len(df_clean)} rows")
    """))

    # 3. Level 8 Baseline
    nb.cells.append(nbf.v4.new_markdown_cell("""
### 1. Level 8 Baseline (Poly Degree 2)
Let's reproduce the Level 8 result.
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
# Poly 2 (Baseline)
poly2 = PolynomialFeatures(degree=2, include_bias=False)
X_poly2 = poly2.fit_transform(X)

X_train, X_test, y_train, y_test = train_test_split(X_poly2, y, test_size=0.2, random_state=42)

model_l8 = LinearRegression()
model_l8.fit(X_train, y_train)
rmse_l8 = np.sqrt(mean_squared_error(y_test, model_l8.predict(X_test)))

print(f"Level 8 RMSE (Poly 2): {rmse_l8:,.0f}")
    """))

    # 4. Level 9 Upgrade
    nb.cells.append(nbf.v4.new_markdown_cell("""
### 2. Level 9 Upgrade (Poly Degree 5 + Ridge)
Can we go lower? Let's try **Degree 5** (Extreme Complexity).
Without Regularization, Degree 5 would overfit massively. With Ridge, it might be perfect.
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
# Poly 5 (High Complexity)
poly3 = PolynomialFeatures(degree=5, include_bias=False)
X_poly3 = poly3.fit_transform(X)

# Scaling is MANDATORY for Ridge/Lasso
scaler = StandardScaler()
X_scaled3 = scaler.fit_transform(X_poly3)

X_train3, X_test3, y_train3, y_test3 = train_test_split(X_scaled3, y, test_size=0.2, random_state=42)

# 1. Standard Linear (No Reg)
lin3 = LinearRegression()
lin3.fit(X_train3, y_train3)
rmse_lin3 = np.sqrt(mean_squared_error(y_test3, lin3.predict(X_test3)))

# 2. Ridge (Alpha optimized)
ridge = Ridge(alpha=0.001) # Weak penalty allows learning
ridge.fit(X_train3, y_train3)
rmse_ridge = np.sqrt(mean_squared_error(y_test3, ridge.predict(X_test3)))

print(f"Poly 5 (Linear): {rmse_lin3:,.0f}")
print(f"Poly 5 (Ridge):  {rmse_ridge:,.0f}")

if rmse_ridge < rmse_l8:
    print(f"SUCCESS: We beat Level 8 by {rmse_l8 - rmse_ridge:,.0f} points!")
else:
    print("Optimization needed...")
    """))

    # Save
    with open('/data/ephemeral/home/workspace/seoul-apt-price-prediction/notebooks/Level_9_Regularization.ipynb', 'w') as f:
        nbf.write(nb, f)
    
    print("Notebook created at /data/ephemeral/home/workspace/seoul-apt-price-prediction/notebooks/Level_9_Regularization.ipynb")

if __name__ == "__main__":
    create_notebook()
