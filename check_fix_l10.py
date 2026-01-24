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

# --- Level 10 Fixed? (Raw Price + IQR 3.0) ---
df_fix = df.copy()

# Outlier Removal: Relaxed to 3.0 (Matching Level 9 "Monster")
# Using RAW price for cleaning logic
for col in ['price_10k_krw', 'area_m2']:
    Q1 = df_fix[col].quantile(0.25)
    Q3 = df_fix[col].quantile(0.75)
    IQR = Q3 - Q1
    df_fix = df_fix[(df_fix[col] >= Q1 - 3.0*IQR) & (df_fix[col] <= Q3 + 3.0*IQR)]

X_fix = df_fix[['area_m2', 'year', 'floor']].values
y_fix = df_fix['price_10k_krw'].values
X_tr, X_te, y_tr, y_te = train_test_split(X_fix, y_fix, test_size=0.2, random_state=42)

model = Pipeline([
    ('poly', PolynomialFeatures(degree=5, include_bias=False)),
    ('scaler', StandardScaler()),
    ('model', Ridge(alpha=0.001)) # Ridge is standard for high poly
])
model.fit(X_tr, y_tr)
rmse = calculate_rmse(y_te, model.predict(X_te))
print(f"Level 10 (Raw + IQR 3.0 + Poly 5): RMSE = {rmse:,.0f}")
