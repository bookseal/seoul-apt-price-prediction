import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge, ElasticNet
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))

# Load Data
df = pd.read_parquet('data/sample.parquet')
if 'year' not in df.columns: df['year'] = 2000
if 'floor' not in df.columns: df['floor'] = 10
numeric_cols = df.select_dtypes(include=[np.number]).columns
for col in numeric_cols: df[col] = df[col].fillna(df[col].median())

# --- Level 9 Logic (Poly 5 + Ridge + IQR 3.0) ---
df_l9 = df.copy()
# IQR 3.0
for col in ['price_10k_krw', 'area_m2']:
    Q1 = df_l9[col].quantile(0.25)
    Q3 = df_l9[col].quantile(0.75)
    IQR = Q3 - Q1
    df_l9 = df_l9[(df_l9[col] >= Q1 - 3.0*IQR) & (df_l9[col] <= Q3 + 3.0*IQR)]

features = ['area_m2', 'year', 'floor']
X = df_l9[features].values
y = df_l9['price_10k_krw'].values

poly = PolynomialFeatures(degree=5, include_bias=False)
X_poly = poly.fit_transform(X)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_poly)
X_tr, X_te, y_tr, y_te = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

model_l9 = Ridge(alpha=0.001)
model_l9.fit(X_tr, y_tr)
rmse_l9 = calculate_rmse(y_te, model_l9.predict(X_te))

# --- Level 10 Logic (Poly 3 + ElasticNet + Log Target + IQR 1.5 + Interactions) ---
df_l10 = df.copy()
df_l10['log_price'] = np.log1p(df_l10['price_10k_krw'])

# IQR 1.5 on Log Price
for col in ['log_price', 'area_m2']:
    Q1 = df_l10[col].quantile(0.25)
    Q3 = df_l10[col].quantile(0.75)
    IQR = Q3 - Q1
    df_l10 = df_l10[(df_l10[col] >= Q1 - 1.5*IQR) & (df_l10[col] <= Q3 + 1.5*IQR)]

df_l10['area_x_year'] = df_l10['area_m2'] * df_l10['year']
features_l10 = ['area_m2', 'year', 'floor', 'area_x_year']

X = df_l10[features_l10].values
y = df_l10['log_price'].values

X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

model_l10 = Pipeline([
    ('poly', PolynomialFeatures(degree=3, include_bias=False)),
    ('scaler', StandardScaler()),
    ('model', ElasticNet(alpha=0.001, l1_ratio=0.5, random_state=42))
])
model_l10.fit(X_tr, y_tr)

# Predict and Inverse Log
y_pred_log = model_l10.predict(X_te)
y_te_real = np.expm1(y_te)
y_pred_real = np.expm1(y_pred_log)

rmse_l10 = calculate_rmse(y_te_real, y_pred_real)

print(f"Level 9 RMSE: {rmse_l9:,.0f}")
print(f"Level 10 RMSE: {rmse_l10:,.0f}")
