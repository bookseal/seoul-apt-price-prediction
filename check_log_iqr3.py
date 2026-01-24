import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
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

# Level 10: Log + IQR 3.0 (Relaxed)
df_l10 = df.copy()
df_l10['log_price'] = np.log1p(df_l10['price_10k_krw'])

for col in ['log_price', 'area_m2']:
    Q1 = df_l10[col].quantile(0.25)
    Q3 = df_l10[col].quantile(0.75)
    IQR = Q3 - Q1
    df_l10 = df_l10[(df_l10[col] >= Q1 - 3.0*IQR) & (df_l10[col] <= Q3 + 3.0*IQR)]

X_l10 = df_l10[['area_m2', 'year', 'floor']].values
y_l10 = df_l10['log_price'].values

X_tr, X_te, y_tr, y_te = train_test_split(X_l10, y_l10, test_size=0.2, random_state=42)

model = Pipeline([
    ('poly', PolynomialFeatures(degree=5, include_bias=False)),
    ('scaler', StandardScaler()),
    ('model', Ridge(alpha=0.001))
])
model.fit(X_tr, y_tr)
y_pred_log = model.predict(X_te)
y_pred_real = np.expm1(y_pred_log)
y_te_real = np.expm1(y_te)

rmse = calculate_rmse(y_te_real, y_pred_real)
print(f"Level 10 (Log + IQR 3.0 + Poly 5): RMSE = {rmse:,.0f}")
