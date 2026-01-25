import nbformat as nbf

nb_path = 'notebooks/Level_10_The_Final_Boss.ipynb'
ntbk = nbf.read(nb_path, nbf.NO_CONVERT)

# Cell 1: Update Goal Description
ntbk.cells[0].source = """
# Level 10: The Final Boss (The Ultimate Linear Model)

**Goal**: Combine EVERY technique we learned (Level 2~9) to build the **Perfect Linear Regression Model**.
We will prove the "Mathematical Limit" of Linear Models.

**The Pipeline**:
1.  **Level 7**: Advanced Data Cleaning (Outlier Removal on Raw Price).
2.  **Level 5 & 8**: Feature Engineering & Selection (Interactions).
3.  **Level 9**: Regularization (Poly + Ridge) via GridSearch.
"""

# Cell 3: Update Data Cleaning (Raw Price, No Log)
ntbk.cells[2].source = """
### Step 1: Data Cleaning (From Level 7)
We will remove **Outliers** using IQR from the **Raw Price**.
Level 9 experiments showed that for this specific dataset and goal, predicting the raw price directly yields a lower RMSE than using log-transformation.
"""

# Cell 4: Code for Cleaning
ntbk.cells[3].source = """
# 1. Load Data
try:
    df = pd.read_parquet('../data/sample.parquet')
except:
    df = pd.read_parquet('/data/ephemeral/home/workspace/seoul-apt-price-prediction/data/sample.parquet')

# Fill Missing (Level 2 Basic)
if 'year' not in df.columns: df['year'] = 2000
if 'floor' not in df.columns: df['floor'] = 10
df = df.fillna(df.median(numeric_only=True))

# 2. Target Variable (Direct Price)
# Level 9 showed that predicting the raw price directly works better for minimizing RMSE on this specific dataset.
df['target'] = df['price_10k_krw']

# 3. Outlier Removal (Level 7 - IQR Method)
# We use IQR 1.5 on the raw price to match the strict cleaning used in our successful experiments.
def remove_outliers(data, column):
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    return data[(data[column] >= lower) & (data[column] <= upper)]

print(f"Original Shape: {df.shape}")
df_clean = remove_outliers(df, 'target')
df_clean = remove_outliers(df_clean, 'area_m2')
print(f"Cleaned Shape: {df_clean.shape}")

# Visualize
plt.figure(figsize=(8, 5))
sns.histplot(df_clean['target'], kde=True, color='purple').set_title('Cleaned Price Distribution')
plt.show()
"""

# Cell 6: Update Target Selection
ntbk.cells[5].source = """
# 1. Create Interaction Features (Level 8)
# Area * Year might capture "Modern Spaciousness" better than just Area + Year
df_clean['area_x_year'] = df_clean['area_m2'] * df_clean['year']

# 2. Select Features (Level 5)
features = ['area_m2', 'year', 'floor', 'area_x_year']
target = 'target' # Using the cleaned raw price

X = df_clean[features].values
y = df_clean[target].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"Training Features: {features}")
"""

# Cell 7: Markdown Update for GridSearch
ntbk.cells[6].source = """
### Step 3: The "Final Boss" Tuning (Level 9)
We learned from our experiments that **Ridge Regression** with **High Degree Polynomials (4 or 5)** is the winning combination.
We will focus our GridSearch on finetuning the `alpha` for Ridge.
"""

# Cell 8: Update GridSearch Code
ntbk.cells[7].source = """
# Pipeline: Poly -> Scaler -> Model
pipeline = Pipeline([
    ('poly', PolynomialFeatures(include_bias=False)),
    ('scaler', StandardScaler()),
    ('model', Ridge()) # Placeholder
])

# The Ultimate Grid (Focused on Ridge)
param_grid = [
    {
        'poly__degree': [4, 5],
        'model': [Ridge()],
        'model__alpha': [0.0001, 0.001, 0.01, 0.1, 1.0, 10.0]
    }
]

print("Searching for the Ultimate Linear Model... (This may take a moment)")
grid = GridSearchCV(pipeline, param_grid, cv=5, scoring='neg_root_mean_squared_error', n_jobs=-1, verbose=1)
grid.fit(X_train, y_train)

print(f"Best parameters: {grid.best_params_}")
print(f"Best CV RMSE: {-grid.best_score_:,.0f}")
"""

# Cell 9: Markdown Update for Final Eval
ntbk.cells[8].source = """
### Step 4: Final Evaluation
We calculate the final RMSE on the test set.
"""

# Cell 10: Update Evaluation Code (No expm1 needed)
ntbk.cells[9].source = """
# Final Prediction
best_model = grid.best_estimator_
y_pred = best_model.predict(X_test)

# Calculate Metrics (No inverse log transform needed)
final_rmse = np.sqrt(mean_squared_error(y_test, y_pred))
final_r2 = r2_score(y_test, y_pred)

print(f"Final Test RMSE: {final_rmse:,.0f} KRW")
print(f"Final R2 Score: {final_r2:.4f}")

# Visualization
plt.figure(figsize=(10, 6))
plt.scatter(y_test, y_pred, alpha=0.1, color='red')
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2)
plt.xlabel("Actual Price")
plt.ylabel("Predicted Price")
plt.title(f"Final Boss Performance (RMSE: {final_rmse:,.0f})")
plt.show()

# Success Check
if final_rmse < 25000:
    print("\\n🏆 MISSION ACCOMPLISHED: RMSE is under 25,000! 🏆")
else:
    print("\\n⚠️ MISSION FAILED: RMSE is still too high. Optimization needed.")
"""

nbf.write(ntbk, nb_path)
print("Notebook updated successfully.")
