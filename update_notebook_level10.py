import nbformat as nbf

def create_notebook():
    nb = nbf.v4.new_notebook()
    
    # 1. Header
    nb.cells.append(nbf.v4.new_markdown_cell("""
# Level 10: The Final Boss (The Ultimate Linear Model)

**Goal**: Combine EVERY technique we learned (Level 2~9) to build the **Perfect Linear Regression Model**.
We will prove the "Mathematical Limit" of Linear Models.

**The Pipeline**:
1.  **Level 7**: Advanced Data Cleaning (Log Transform, Outlier Removal).
2.  **Level 5 & 8**: Feature Engineering & Selection (Interactions, VIF).
3.  **Level 9**: Regularization (Poly + ElasticNet) via GridSearch.
    """))
    
    # 2. Setup
    nb.cells.append(nbf.v4.new_code_cell("""
# Install dependencies (Auto-healing)
!pip install pyarrow fastparquet matplotlib seaborn scikit-learn > /dev/null

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge, Lasso, ElasticNet
from sklearn.metrics import mean_squared_error, r2_score

# Configuration
np.random.seed(42)
plt.style.use('seaborn-v0_8')
    """))
    
    # 3. Load Data & Level 7 Cleaning
    nb.cells.append(nbf.v4.new_markdown_cell("""
### Step 1: Data Cleaning (From Level 7)
We apply **Log Transformation** to the target (Price) to normalize its distribution, and remove **Outliers** using IQR.
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
# 1. Load Data
try:
    df = pd.read_parquet('../data/sample.parquet')
except:
    df = pd.read_parquet('/data/ephemeral/home/workspace/seoul-apt-price-prediction/data/sample.parquet')

# Fill Missing (Level 2 Basic)
if 'year' not in df.columns: df['year'] = 2000
if 'floor' not in df.columns: df['floor'] = 10
df = df.fillna(df.median(numeric_only=True))

# 2. Log Transform Target (Level 7)
# Prices are often skewed. Log-transform makes them more "Normal" (Gaussian).
df['log_price'] = np.log1p(df['price_10k_krw'])

# 3. Outlier Removal (Level 7 - IQR Method)
def remove_outliers(data, column):
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    return data[(data[column] >= lower) & (data[column] <= upper)]

print(f"Original Shape: {df.shape}")
df_clean = remove_outliers(df, 'log_price')
df_clean = remove_outliers(df_clean, 'area_m2')
print(f"Cleaned Shape: {df_clean.shape}")

# Visualize the effect
fig, ax = plt.subplots(1, 2, figsize=(12, 5))
sns.histplot(df['price_10k_krw'], ax=ax[0], kde=True).set_title('Original Price')
sns.histplot(df_clean['log_price'], ax=ax[1], kde=True, color='green').set_title('Log-Transformed & Cleaned Price')
plt.show()
    """))

    # 4. Feature Engineering (Level 8) & Selection (Level 5)
    nb.cells.append(nbf.v4.new_markdown_cell("""
### Step 2: Feature Engineering & Selection (Level 5 & 8)
We create **Interaction Terms** (e.g., `Area * Year`) because a new apartment is worth more than an old one, but a *large* new apartment is worth exponentially more.
Then, we define our feature set.
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
# 1. Create Interaction Features (Level 8)
# Area * Year might capture "Modern Spaciousness" better than just Area + Year
df_clean['area_x_year'] = df_clean['area_m2'] * df_clean['year']

# 2. Select Features (Level 5)
# In a real scenario, we would calculate VIF here. For now, we manually pick the strongest ones known from Level 5.
features = ['area_m2', 'year', 'floor', 'area_x_year']
target = 'log_price'

X = df_clean[features].values
y = df_clean[target].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"Training Features: {features}")
    """))
    
    # 5. Model Optimization (Level 9)
    nb.cells.append(nbf.v4.new_markdown_cell("""
### Step 3: The "Final Boss" Tuning (Level 9)
Now we apply **GridSearchCV** to find the absolute best combination of:
1.  **Polynomial Degree**: 1, 2, 3 (Curvature)
2.  **Regularization**: Ridge (L2), Lasso (L1), ElasticNet (Mixed)
3.  **Alpha**: Strength of penalty

We use `neg_root_mean_squared_error` as the scoring metric.
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
# Pipeline: Poly -> Scaler -> Model
pipeline = Pipeline([
    ('poly', PolynomialFeatures(include_bias=False)),
    ('scaler', StandardScaler()),
    ('model', Ridge()) # Placeholder
])

# The Ultimate Grid
param_grid = [
    # Search Ridge
    {
        'poly__degree': [1, 2, 3, 4, 5], # Include Degree 5 to match Level 9!
        'model': [Ridge()],
        'model__alpha': [0.0001, 0.001, 0.01, 0.1, 1.0, 10.0]
    },
    # Search Lasso (Selects features)
    {
        'poly__degree': [1, 2, 3], # Lasso is expensive, keep degree lower
        'model': [Lasso(max_iter=5000)],
        'model__alpha': [0.001, 0.01, 0.1, 1.0]
    },
    # Search ElasticNet (Best of both worlds)
    {
        'poly__degree': [1, 2, 3],
        'model': [ElasticNet(max_iter=5000)],
        'model__alpha': [0.001, 0.01, 0.1],
        'model__l1_ratio': [0.2, 0.5, 0.8]
    }
]

print("Searching for the Ultimate Linear Model... (This may take a moment)")
grid = GridSearchCV(pipeline, param_grid, cv=5, scoring='neg_root_mean_squared_error', n_jobs=-1, verbose=1)
grid.fit(X_train, y_train)

print(f"Best parameters: {grid.best_params_}")
print(f"Best CV RMSE (Log Scale): {-grid.best_score_:.4f}")
    """))
    
    # 6. Evaluation
    nb.cells.append(nbf.v4.new_markdown_cell("""
### Step 4: Final Evaluation
We must convert the predicted **Log Price** back to **Real Price** (`np.expm1`) to calculate the true RMSE.
    """))
    
    nb.cells.append(nbf.v4.new_code_cell("""
# Predict
y_pred_log = grid.predict(X_test)

# Inverse Transform (Log -> Real Price)
y_test_real = np.expm1(y_test)
y_pred_real = np.expm1(y_pred_log)

# Calculate Metric
final_rmse = np.sqrt(mean_squared_error(y_test_real, y_pred_real))
final_r2 = r2_score(y_test_real, y_pred_real)

print(f"Final Test RMSE: {final_rmse:,.0f} KRW")
print(f"Final R2 Score: {final_r2:.4f}")

# Plot Residuals
plt.figure(figsize=(10, 6))
sns.scatterplot(x=y_test_real, y=y_pred_real, alpha=0.5)
plt.plot([y_test_real.min(), y_test_real.max()], [y_test_real.min(), y_test_real.max()], 'r--')
plt.xlabel('Actual Price')
plt.ylabel('Predicted Price')
plt.title('Actual vs Predicted (The Ultimate Linear Model)')
plt.show()
    """))

    # Save
    with open('/data/ephemeral/home/workspace/seoul-apt-price-prediction/notebooks/Level_10_The_Final_Boss.ipynb', 'w') as f:
        nbf.write(nb, f)
    print("Notebook created.")

if __name__ == "__main__":
    create_notebook()
