# -*- coding: utf-8 -*-
"""
Level 8: Advanced EDA and Feature Engineering

Learn about feature scaling, transformations, and creating new features.
Transform raw data into better predictors!
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, MinMaxScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison


def display_header() -> None:
    """Display Level 8 introduction."""
    st.title("⚗️ Level 8: Feature Engineering")
    
    st.success("""
    **Goal**: Create BETTER features from raw data.
    
    "More data beats better algorithms, but **better features** beat more data!"
    """)
    
    with st.expander("💡 What is Feature Engineering?"):
        st.markdown("""
        **Feature Engineering** is the art of creating new input variables (features) from your existing data.
        
        *   **Raw Data**: `Area`, `Price`, `Year`
        *   **Engineered Features**: `Price per m²`, `Building Age`, `Log(Price)`
        
        Machine Learning models are only as good as the data you feed them.
        """)

def display_toc() -> None:
    """Display Table of Contents."""
    st.markdown("""
    ### 📑 Table of Contents
    1.  [**Step 1: Pipeline Overview**](#step-1-pipeline-overview)
    2.  [**Step 2: PCA vs Feature Engineering**](#step-2-pca-vs-feature-engineering)
    3.  [**Step 3: Feature Scaling (Standard vs MinMax)**](#step-3-feature-scaling)
    4.  [**Step 4: Log Transformation**](#step-4-log-transformation)
    5.  [**Step 5: Feature Creation**](#step-5-feature-creation)
    6.  [**Step 6: Polynomial Features**](#step-6-polynomial-features)
    7.  [**Step 7: Performance Comparison**](#step-7-performance-comparison)
    """)


def display_pipeline_overview() -> None:
    """Show the pipeline."""
    st.header("Step 1: Pipeline Overview")
    
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>1. Load & Scale</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>2. Transform</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>3. Create Features</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>4. Train & Compare</b>
        </div>
    </div>
    """, unsafe_allow_html=True)


def display_pca_vs_fe() -> None:
    """Explain differential between PCA and Feature Engineering."""
    st.header("Step 2: PCA vs Feature Engineering")
    
    st.markdown("""
    **Q: "How is this different from PCA in Level 6?"**
    
    Great question! Both deal with data, but their **goals** are opposite.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; border: 2px solid #2196F3;">
            <div style="text-align: center; font-size: 20px; margin-bottom: 10px;">📉 <b>PCA</b></div>
            <ul>
                <li><b>Goal</b>: Information Reduction</li>
                <li><b>Direction</b>: <b>Decrease</b> number of features</li>
                <li><b>Analogy</b>: "Grind fruits into a <b>Juice</b>" (Keep flavor, reduce volume)</li>
                <li><b>Use Case</b>: Visualization or too many features</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown("""
        <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; border: 2px solid #4CAF50;">
            <div style="text-align: center; font-size: 20px; margin-bottom: 10px;">🧪 <b>Feature Engineering</b></div>
            <ul>
                <li><b>Goal</b>: Information Exploration</li>
                <li><b>Direction</b>: <b>Increase/Transform</b> features</li>
                <li><b>Analogy</b>: "Bake flour dough into <b>Bread</b>" (Create new value)</li>
                <li><b>Use Case</b>: Maximizing model performance</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare data with additional columns."""
    df = df.copy()
    np.random.seed(RANDOM_STATE)
    n = len(df)
    
    if 'year' not in df.columns:
        df['year'] = np.random.randint(1985, 2024, n)
    if 'floor' not in df.columns:
        df['floor'] = np.random.randint(1, 30, n)
    
    return df


def display_scaling_comparison(df: pd.DataFrame) -> None:
    """Compare different scaling methods."""
    st.header("Step 3: Feature Scaling")
    
    st.markdown("""
    **Q: "Didn't we talk about scaling in Level 2?"**
    
    Correct! In **Level 2 Simulator (SGD)**, scaling was mandatory.
    
    However, `LinearRegression` itself (OLS) can work without it.
    Now, for **Advanced Feature Engineering**, let's visually confirm the difference.
    
    👇 **See how `Normal Data` changes when there is an `Outlier (5000)`!**
    """)
    
    # ... (code omitted for brevity in replacement, but I must match TargetContent exactly)
    # Actually I can replace the header and markdown only if I carefully target lines.
    # But wait, looking at the code structure provided in view_file,
    # Lines 141-153 are the markdown.
    # Lines 208-222 are content inside columns.
    
    # Let's target the first block (141-153)

    
    # Create simple data with one huge outlier
    np.random.seed(42)
    # 50 points roughly around 100 
    data = np.random.normal(100, 10, 50)  
    # 1 massive outlier
    data = np.append(data, [5000])        
    
    df_scale = pd.DataFrame({'Value': data})
    df_scale['Type'] = ['Normal'] * 50 + ['Outlier']
    
    scaler_std = StandardScaler()
    scaler_minmax = MinMaxScaler()
    
    df_scale['Standard'] = scaler_std.fit_transform(df_scale[['Value']])
    df_scale['MinMax'] = scaler_minmax.fit_transform(df_scale[['Value']])
    
    # Visualize with shared Y-axis logic or clear distinction
    fig, axes = plt.subplots(1, 3, figsize=(12, 5))
    
    # 1. Original
    axes[0].scatter(range(len(df_scale)), df_scale['Value'], c=['blue']*50 + ['red'], alpha=0.7)
    axes[0].set_title('1. Original Data')
    axes[0].set_ylabel('Raw Value')
    axes[0].text(50, 5000, 'Outlier(5000)', color='red', ha='right')
    axes[0].text(0, 100, 'Normal(~100)', color='blue')
    
    # 2. StandardScaler
    axes[1].scatter(range(len(df_scale)), df_scale['Standard'], c=['blue']*50 + ['red'], alpha=0.7)
    axes[1].set_title('2. StandardScaler')
    axes[1].set_ylabel('Sigmas (Standardized)')
    axes[1].axhline(0, color='black', linestyle='--', alpha=0.3)
    # The normal data is still distinguishable?
    axes[1].text(0, 0, 'Centered at 0', color='blue')
    axes[1].text(50, 7, 'Still at +7 Sigma', color='red', ha='right')
    
    # 3. MinMaxScaler
    axes[2].scatter(range(len(df_scale)), df_scale['MinMax'], c=['blue']*50 + ['red'], alpha=0.7)
    axes[2].set_title('3. MinMaxScaler')
    axes[2].set_ylabel('Range [0, 1]')
    # Normal data squashed?
    axes[2].text(50, 1.0, 'Outlier = 1.0', color='red', ha='right')
    axes[2].text(0, 0.02, 'Normal Data Squashed!\n(0.0 ~ 0.02)', color='blue')
    # Draw a box around the squashed area
    import matplotlib.patches as patches
    rect = patches.Rectangle((-2, 0), 55, 0.05, linewidth=1, edgecolor='r', facecolor='none')
    axes[2].add_patch(rect)
    
    plt.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    col1, col2 = st.columns(2)
    with col1:
        st.info("""
        **Standard Scaler**
        *   **Feature**: Mean 0, Variance 1
        *   **Outlier Effect**: Outliers are still far from mean (+7 Sigma)
        *   **Data Preservation**: Normal data is **appropriately spread** around 0.
        *   **Conclusion**: Less info loss even with outliers.
        """)
    with col2:
        st.error("""
        **MinMax Scaler**
        *   **Feature**: Min 0, Max 1
        *   **Outlier Effect**: Outlier takes `1.0` alone.
        *   **Data Destruction**: Normal data (~100) gets **squashed** into `0.0`~`0.02`.
        *   **Conclusion**: Hard to distinguish normal data differences if an outlier exists.
        """)
    
    st.code("""
# Code Example (StandardScaler vs MinMaxScaler)
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# 1. StandardScaler (Outlier에 강함)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# 2. MinMaxScaler (Outlier에 취약)
scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X)
    """, language="python")


def display_log_transformation(df: pd.DataFrame) -> None:
    """Show log transformation for skewed data."""
    st.header("Step 4: Log Transformation")
    
    st.markdown("""
    **"Skewed"** data like real estate prices is hard for models to learn.
    Applying Log makes the data closer to **Normal Distribution (Bell Curve)**.
    """)
    
    sample = df['price_10k_krw'].head(5000)
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    
    # Original
    axes[0].hist(sample, bins=50, alpha=0.7, color='steelblue', edgecolor='white')
    axes[0].set_title('Original Price (Skewed)')
    axes[0].set_xlabel('Price')
    
    # Log transformed
    log_sample = np.log1p(sample)  # log(1+x) to handle zeros
    axes[1].hist(log_sample, bins=50, alpha=0.7, color='green', edgecolor='white')
    axes[1].set_title('Log-Transformed (Normal-like)')
    axes[1].set_xlabel('log(Price)')
    
    plt.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    st.warning("""
    **⚠️ Downsides of Log Transform**
    
    1.  **Cannot handle 0 or negative**: $\log(x)$ is undefined for $x \le 0$.
        *   Solution: Use `np.log1p()` which does $\log(x + 1)$.
    2.  **Interpretation Difficulty**: Predicted values are also in log scale.
        *   "Log price increased by 1" → Hard to understand intuitively.
        *   Must reverse with `np.expm1()` to get **original price**.
    """)
    
    st.code("""
import numpy as np

# 1. Log transformation (Training)
# Use log1p to handle zeros (log(0) is -inf)
df['log_price'] = np.log1p(df['price'])

# 2. Train Model
model.fit(X_train, df['log_price'])

# 3. Predict & Reverse (Inference)
pred_log = model.predict(X_test)
pred_real = np.expm1(pred_log)  # exp(x) - 1 to reverse log1p
    """, language='python')


def display_feature_creation(df: pd.DataFrame) -> None:
    """Show creating new features."""
    st.header("Step 5: Feature Creation (Derived Features)")
    
    st.markdown("""
    Using **"Domain Knowledge"** to create new information.
    """)
    
    df = df.copy()
    
    # Create features
    df['building_age'] = 2024 - df['year']
    df['price_per_m2'] = df['price_10k_krw'] / df['area_m2']
    
    st.markdown("##### 💡 Example Features")
    features = [
        ("building_age", "2024 - year", "Building Age (Older = Cheaper?)"),
        ("price_per_m2", "price / area", "Price per m² (For comparison)"),
        ("is_new", "year > 2015", "Is New? (Premium)")
    ]
    st.table(pd.DataFrame(features, columns=['Feature', 'Formula', 'Meaning']))
    
    st.code("""
# Create meaningful features
df['building_age'] = 2024 - df['year']
df['is_new'] = (df['year'] > 2015).astype(int)
    """, language='python')
    
    return df


def display_polynomial_features(df: pd.DataFrame) -> None:
    """Show interactive polynomial features demo."""
    st.header("Step 6: Polynomial Features")
    
    st.markdown("""
    **Q: "Difference between Scaling and Polynomial?"**
    
    *   **Scaling**: Changes **Range** of data. (Shape stays same)
    *   **Polynomial**: Changes **Shape** of data. (Curve, 3D)
    
    Essential when data is **Curved** (Non-linear)!
    """)
    
    degree = st.slider("Polynomial Degree", 1, 10, 2)
    
    # Create non-linear data
    np.random.seed(42)
    X = np.sort(np.random.rand(40, 1) * 10, axis=0)
    y = np.cos(X).ravel() + np.random.randn(40) * 0.1 + (X.ravel()/5)
    
    # Transform & Fit
    poly = PolynomialFeatures(degree=degree, include_bias=False)
    X_poly = poly.fit_transform(X)
    model = LinearRegression()
    model.fit(X_poly, y)
    
    # Plot
    X_plot = np.linspace(0, 10, 100).reshape(-1, 1)
    y_plot = model.predict(poly.transform(X_plot))
    
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.scatter(X, y, color='blue', alpha=0.6, label='Data')
    ax.plot(X_plot, y_plot, color='red', linewidth=2, label=f'Degree {degree}')
    ax.set_title(f"Polynomial Regression (Degree {degree})")
    ax.legend()
    st.pyplot(fig, use_container_width=True)
    
    if degree > 5:
        st.warning("⚠️ High degree causes **Overfitting** (Wiggly line)!")
        
    st.code(f"""
from sklearn.preprocessing import PolynomialFeatures

# 1. Create Polynomial Features (Degree {degree})
poly = PolynomialFeatures(degree={degree}, include_bias=False)
X_poly = poly.fit_transform(X)

# 2. Train Linear Regression on transformed features
model = LinearRegression()
model.fit(X_poly, y)
    """, language="python")


@st.cache_resource
def train_models(df: pd.DataFrame):
    """Train and compare different approaches using Level 7 Cleaning logic."""
    # 0. Apply Level 7 Cleaning (IQR Outlier Removal) FIRST
    # Feature Engineering should be applied on CLEAN data, otherwise outliers ruin it.
    
    # Basic Cleaning Function (Same as Level 7)
    df_clean = df.copy()
    for col in ['price_10k_krw', 'area_m2']:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 3.0 * IQR
        upper = Q3 + 3.0 * IQR
        df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]
        
    # Start comparison on Clean Data
    df = df_clean.copy()
    
    # Create Features
    df['building_age'] = 2024 - df['year']
    
    X_base = df[['area_m2', 'year', 'floor']].values
    y = df['price_10k_krw'].values
    
    X_train, X_test, y_train, y_test = train_test_split(X_base, y, test_size=0.2, random_state=RANDOM_STATE)
    
    results = {}
    
    # 1. Level 7 Cleaned Baseline (Simple Linear Regression)
    model_base = LinearRegression()
    model_base.fit(X_train, y_train)
    results['Level 7 Cleaned'] = calculate_rmse(y_test, model_base.predict(X_test))
    
    # 2. + Log Target (on Cleaned data)
    model_log = LinearRegression()
    model_log.fit(X_train, np.log1p(y_train))
    y_pred_log = np.expm1(model_log.predict(X_test))
    results['+ Log Target'] = calculate_rmse(y_test, y_pred_log)
    
    # 3. + Polynomial Features (Degree 2)
    poly = PolynomialFeatures(degree=2, include_bias=False)
    X_train_poly = poly.fit_transform(X_train)
    X_test_poly = poly.transform(X_test)
    
    model_poly = LinearRegression()
    model_poly.fit(X_train_poly, y_train)
    results['+ Polynomial (d=2)'] = calculate_rmse(y_test, model_poly.predict(X_test_poly))
    
    return results


def display_compare_performance(results: dict) -> None:
    """Show final comparison."""
    st.header("Step 7: Performance Comparison")
    
    results_df = pd.DataFrame(list(results.items()), columns=['Model', 'RMSE']).sort_values('RMSE')
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.bar_chart(results_df.set_index('Model'))
        
    with col2:
        st.dataframe(results_df, hide_index=True)
        
        best_rmse = results_df.iloc[0]['RMSE']
        baseline_rmse = results['Level 7 Cleaned']
        imp = (baseline_rmse - best_rmse) / baseline_rmse * 100
        
        st.success(f"**Best RMSE**: {best_rmse:,.0f}")
        st.metric("Improvement vs Level 7", f"{imp:.1f}%")

    st.markdown("---")
    # Compare with other levels
    display_rmse_comparison(8, best_rmse)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        df = prepare_data(df)
        
        display_header()
        st.markdown("---")
        display_toc()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_pca_vs_fe()
        st.markdown("---")
        display_scaling_comparison(df)
        st.markdown("---")
        display_log_transformation(df)
        st.markdown("---")
        df = display_feature_creation(df)
        st.markdown("---")
        display_polynomial_features(df)
        st.markdown("---")
        
        with st.spinner("Training models..."):
            results = train_models(df)
            
        display_compare_performance(results)
        
        display_code_link("Level_8_Feature_Engineering.ipynb")
        display_next_level_teaser(8)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
