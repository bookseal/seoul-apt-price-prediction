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
numeric_cols = df.select_dtypes(include=[np.number]).columns
for col in numeric_cols: df[col] = df[col].fillna(df[col].median())
if 'year' not in df.columns: df['year'] = 2000

# Level 10 Setup
df_l10 = df.copy()
df_l10['log_price'] = np.log1p(df_l10['price_10k_krw'])

# IQR 1.5
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

# Try Degree 4 and 5
for deg in [3, 4, 5]:
    model = Pipeline([
        ('poly', PolynomialFeatures(degree=deg, include_bias=False)),
        ('scaler', StandardScaler()),
        ('model', ElasticNet(alpha=0.001, l1_ratio=0.5, random_state=42))
    ])
    model.fit(X_tr, y_tr)
    y_pred_log = model.predict(X_te)
    rmse = calculate_rmse(np.expm1(y_te), np.expm1(y_pred_log))
    print(f"Degree {deg} RMSE: {rmse:,.0f}")

