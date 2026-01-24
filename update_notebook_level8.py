import nbformat as nbf
import os

def create_level8_notebook():
    nb = nbf.v4.new_notebook()
    
    # 1. Title & Intro
    nb.cells.append(nbf.v4.new_markdown_cell("""
# ⚗️ Level 8: Feature Engineering

**[📖 Read the Streamlit Manual](https://bookseal-seoul-apt-price-prediction.streamlit.app/Level_8_Feature_Engineering)**

In Level 7, we cleaned the data (Messy -> Clean).
Now, can we improve the model further by **creating new features**?

**Goal**: Compare "Cleaned Data" vs "Cleaned + Feature Engineered Data".
    """))
    
    # 2. Load & Clean (Level 7 Logic)
    nb.cells.append(nbf.v4.new_markdown_cell("""
### 1. Load & Clean Data (From Level 7)
We start with the **SAME cleaning process** as Level 7.
We must compare apples to apples!
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler, MinMaxScaler, PolynomialFeatures

# 1. Load
url = "https://github.com/bookseal/seoul-apt-price-prediction/raw/main/data/sample.parquet"
df = pd.read_parquet(url)

# 2. Ensure Columns
if 'year' not in df.columns:
    df['year'] = df['built_year'] if 'built_year' in df.columns else 2000
if 'floor' not in df.columns:
    np.random.seed(42)
    df['floor'] = np.random.randint(1, 30, len(df))

# 3. Fill NaNs
df['area_m2'].fillna(df['area_m2'].median(), inplace=True)
df = df.dropna(subset=['price_10k_krw'])

# 4. Remove Outliers (IQR 3.0 - Level 7 Notebook Logic)
def remove_outliers(df, col):
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 3.0 * IQR
    upper = Q3 + 3.0 * IQR
    return df[(df[col] >= lower) & (df[col] <= upper)]

df_clean = df.copy()
df_clean = remove_outliers(df_clean, 'price_10k_krw')
df_clean = remove_outliers(df_clean, 'area_m2')

print(f"Data Loaded & Cleaned! Rows: {len(df_clean)}")
    """))
    
    # 3. Baseline Model (Level 7)
    nb.cells.append(nbf.v4.new_markdown_cell("""
### 2. Level 7 Baseline
Train a simple model on the cleaned data. This is the **score to beat**.
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
features_base = ['area_m2', 'year', 'floor']
X_base = df_clean[features_base].values
y = df_clean['price_10k_krw'].values

X_train, X_test, y_train, y_test = train_test_split(X_base, y, test_size=0.2, random_state=42)

model_base = LinearRegression()
model_base.fit(X_train, y_train)
rmse_base = np.sqrt(mean_squared_error(y_test, model_base.predict(X_test)))

print(f"📉 Level 7 Baseline RMSE: {rmse_base:,.0f}")
    """))
    
    # 4. Feature Creation
    nb.cells.append(nbf.v4.new_markdown_cell("""
### 3. Feature Creation
Let's add domain knowledge features:
- **Building Age**: `2024 - year`
- **Is New**: `year > 2015`
- **Price per m2**: (We can't use Price for training! But we can find proxies?)
    - *Wait, we can't use Price per m2 as a FEATURE because we don't know Price at prediction time!*
    - Good catch. We only create features from Inputs.
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
df_fe = df_clean.copy()

# Create Features
df_fe['building_age'] = 2024 - df_fe['year']
df_fe['is_new'] = (df_fe['year'] > 2015).astype(int)
df_fe['floor_high'] = (df_fe['floor'] > 15).astype(int) 

# Select Features
features_fe = ['area_m2', 'year', 'floor', 'building_age', 'is_new', 'floor_high']
X_fe = df_fe[features_fe].values
y_fe = df_fe['price_10k_krw'].values

X_train_fe, X_test_fe, y_train_fe, y_test_fe = train_test_split(X_fe, y_fe, test_size=0.2, random_state=42)

model_fe = LinearRegression()
model_fe.fit(X_train_fe, y_train_fe)
rmse_fe = np.sqrt(mean_squared_error(y_test_fe, model_fe.predict(X_test_fe)))

print(f"🛠️ Feature Engineered RMSE: {rmse_fe:,.0f}")
print(f"Improvement: {rmse_base - rmse_fe:,.0f}")
    """))
    
    # 5. Log Transformation
    nb.cells.append(nbf.v4.new_markdown_cell("""
### 4. Log Transformation
Price is skewed. Let's predict `log(Price)` instead.
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
# Use Cleaned Data
y_log_train = np.log1p(y_train)

# Train on Base Features with Log Target
model_log = LinearRegression()
model_log.fit(X_train, y_log_train)

# Predict
pred_log = model_log.predict(X_test)
pred_real = np.expm1(pred_log) # Reverse log

rmse_log = np.sqrt(mean_squared_error(y_test, pred_real))

print(f"📐 Log Transform RMSE: {rmse_log:,.0f}")
    """))
    
    # 6. Polynomial Features
    nb.cells.append(nbf.v4.new_markdown_cell("""
### 5. Polynomial Features
Let the model learn curves ($x^2$).
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
poly = PolynomialFeatures(degree=2, include_bias=False)
X_poly_train = poly.fit_transform(X_train)
X_poly_test = poly.transform(X_test)

model_poly = LinearRegression()
model_poly.fit(X_poly_train, y_train)

rmse_poly = np.sqrt(mean_squared_error(y_test, model_poly.predict(X_poly_test)))

print(f"📈 Polynomial (Degree 2) RMSE: {rmse_poly:,.0f}")
    """))

    # Save
    base_dir = "/data/ephemeral/home/workspace/seoul-apt-price-prediction/notebooks"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        
    file_path = os.path.join(base_dir, "Level_8_Feature_Engineering.ipynb")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)
        
    print(f"Notebook created at {file_path}")

if __name__ == "__main__":
    create_level8_notebook()
