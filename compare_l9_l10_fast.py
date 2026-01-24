import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge, ElasticNet
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import warnings
warnings.filterwarnings('ignore')

def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))

# Load Data
df = pd.read_parquet('data/sample.parquet')
numeric_cols = df.select_dtypes(include=[np.number]).columns
for col in numeric_cols: df[col] = df[col].fillna(df[col].median())
if 'year' not in df.columns: df['year'] = 2000
if 'floor' not in df.columns: df['floor'] = 10

# --- Level 9 Logic (Using Ridge for speed, approx same result) ---
df_l9 = df.copy()
features_l9 = ['area_m2', 'year', 'floor']

# Outlier Removal (IQR 3.0 on Raw Price)
# Reduced dataset size for testing to avoid hanging
for col in ['price_10k_krw', 'area_m2']:
    Q1 = df_l9[col].quantile(0.25)
    Q3 = df_l9[col].quantile(0.75)
    IQR = Q3 - Q1
    df_l9 = df_l9[(df_l9[col] >= Q1 - 3.0*IQR) & (df_l9[col] <= Q3 + 3.0*IQR)]

df_l9 = df_l9.sample(frac=0.3, random_state=42) # Downsample for speed
X_l9 = df_l9[features_l9].values
y_l9 = df_l9['price_10k_krw'].values
X_tr9, X_te9, y_tr9, y_te9 = train_test_split(X_l9, y_l9, test_size=0.2, random_state=42)

model_l9 = Pipeline([
    ('poly', PolynomialFeatures(degree=5, include_bias=False)),
    ('scaler', StandardScaler()),
    ('model', Ridge(alpha=0.001))
])
model_l9.fit(X_tr9, y_tr9)
rmse_l9 = calculate_rmse(y_te9, model_l9.predict(X_te9))

# --- Level 10 Logic ---
df_l10 = df.copy()
df_l10['log_price'] = np.log1p(df_l10['price_10k_krw'])

for col in ['log_price', 'area_m2']:
    Q1 = df_l10[col].quantile(0.25)
    Q3 = df_l10[col].quantile(0.75)
    IQR = Q3 - Q1
    df_l10 = df_l10[(df_l10[col] >= Q1 - 1.5*IQR) & (df_l10[col] <= Q3 + 1.5*IQR)]

df_l10 = df_l10.sample(frac=0.3, random_state=42) # Downsample for speed
df_l10['area_x_year'] = df_l10['area_m2'] * df_l10['year']
features_l10 = ['area_m2', 'year', 'floor', 'area_x_year']

X_l10 = df_l10[features_l10].values
y_l10 = df_l10['log_price'].values
X_tr10, X_te10, y_tr10, y_te10 = train_test_split(X_l10, y_l10, test_size=0.2, random_state=42)

# Using Ridge instead of ElasticNet for speed check (conceptually similar for this test)
model_l10 = Pipeline([
    ('poly', PolynomialFeatures(degree=5, include_bias=False)),
    ('scaler', StandardScaler()),
    ('model', Ridge(alpha=0.0001)) # Ridge approx ElasticNet for speed
])
model_l10.fit(X_tr10, y_tr10)

y_pred_log = model_l10.predict(X_te10)
y_pred_real = np.expm1(y_pred_log)
y_te10_real = np.expm1(y_te10)

rmse_l10 = calculate_rmse(y_te10_real, y_pred_real)

print(f"Level 9 (Raw + Poly 5): RMSE approx {rmse_l9:,.0f}")
print(f"Level 10 (Log + Poly 5): RMSE approx {rmse_l10:,.0f}")
