import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))

# Load Data
try:
    df = pd.read_parquet('data/sample.parquet')
except:
    df = pd.read_parquet('/data/ephemeral/home/workspace/seoul-apt-price-prediction/data/sample.parquet')

# Basic Fill
numeric_cols = df.select_dtypes(include=[np.number]).columns
for col in numeric_cols: df[col] = df[col].fillna(df[col].median())
if 'year' not in df.columns: df['year'] = 2000
if 'floor' not in df.columns: df['floor'] = 10

# Level 10 Logic
df['log_price'] = np.log1p(df['price_10k_krw'])

# IQR 1.5 Cleaning (Same as Level 10 nb)
for col in ['log_price', 'area_m2']:
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    df = df[(df[col] >= Q1 - 1.5*IQR) & (df[col] <= Q3 + 1.5*IQR)]

df['area_x_year'] = df['area_m2'] * df['year']
features = ['area_m2', 'year', 'floor', 'area_x_year']

X = df[features].values
y = df['log_price'].values

X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"Data Shape: {df.shape}")

for deg in [4, 5]:
    model = Pipeline([
        ('poly', PolynomialFeatures(degree=deg, include_bias=False)),
        ('scaler', StandardScaler()),
        ('model', Ridge(alpha=0.001)) # Low alpha for high degree from Level 9 experience
    ])
    model.fit(X_tr, y_tr)
    y_pred_log = model.predict(X_te)
    rmse = calculate_rmse(np.expm1(y_te), np.expm1(y_pred_log))
    print(f"Degree {deg} Ridge RMSE: {rmse:,.0f}")
