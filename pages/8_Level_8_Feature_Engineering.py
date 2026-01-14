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
    **Goal**: Transform raw features into better predictors.
    
    Sometimes the BEST features are ones you CREATE!
    """)
    
    with st.expander("💡 What is Feature Engineering?"):
        st.markdown("""
        **Feature Engineering** = Creating new features from existing ones.
        
        Why does it matter?
        - Raw data may not capture relationships directly
        - Combining features can reveal hidden patterns
        - Transformations can make patterns linear (easier for models)
        
        **Examples**:
        - `building_age` = Current Year - Building Year
        - `price_per_m2` = Price / Area
        - `log_price` = log(Price)  # Makes skewed data normal
        """)


def display_pipeline_overview() -> None:
    """Show the pipeline."""
    st.header("🔄 Level 8 Pipeline")
    
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>1. Load</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>2. Scale</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #00BCD4, #0097A7); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>3. Transform</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>4. Create</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>5. Compare</b>
        </div>
    </div>
    """, unsafe_allow_html=True)


def display_why_level8() -> None:
    """Explain motivation for Level 8."""
    st.header("🤔 Why Feature Engineering?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 10px 0;">
        <b>❌ The Problem: Raw Data is Not Optimal!</b><br>
        <span style="font-size: 13px;">
        • Features may have different scales (Area: 10-200, Price: 10,000-500,000)<br>
        • Relationships may not be linear<br>
        • Important information may be hidden in combinations<br><br>
        <b>Solution: Engineer better features!</b>
        </span>
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
    st.header("📏 Feature Scaling")
    
    st.markdown("""
    **Problem**: Features have different scales!
    - Area: 20 - 200 (m²)
    - Price: 10,000 - 500,000 (10K KRW)
    
    **Solution**: Scale features to similar ranges.
    """)
    
    # Show original distribution
    col1, col2 = st.columns(2)
    
    sample = df[['area_m2', 'price_10k_krw']].head(1000)
    
    with col1:
        st.markdown("### StandardScaler")
        st.markdown("""
        - Centers data (mean = 0)
        - Scales to unit variance (std = 1)
        - Formula: `(x - mean) / std`
        """)
        
        scaler1 = StandardScaler()
        scaled1 = scaler1.fit_transform(sample)
        
        st.code("""
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
# Mean=0, Std=1
""", language='python')
    
    with col2:
        st.markdown("### MinMaxScaler")
        st.markdown("""
        - Scales to [0, 1] range
        - Preserves zero values
        - Formula: `(x - min) / (max - min)`
        """)
        
        scaler2 = MinMaxScaler()
        scaled2 = scaler2.fit_transform(sample)
        
        st.code("""
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X)
# Values in [0, 1]
""", language='python')
    
    # Visualize
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    
    axes[0].hist(sample['area_m2'], bins=30, alpha=0.7, color='steelblue')
    axes[0].set_title('Original Area')
    axes[0].set_xlabel('Area (m²)')
    
    axes[1].hist(scaled1[:, 0], bins=30, alpha=0.7, color='green')
    axes[1].set_title('StandardScaler')
    axes[1].set_xlabel('Scaled (mean=0, std=1)')
    
    axes[2].hist(scaled2[:, 0], bins=30, alpha=0.7, color='orange')
    axes[2].set_title('MinMaxScaler')
    axes[2].set_xlabel('Scaled [0, 1]')
    
    plt.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    st.info("""
    **💡 When to use which?**
    - **StandardScaler**: When features have outliers, or for algorithms sensitive to scale
    - **MinMaxScaler**: When you need bounded values, or for neural networks
    """)


def display_log_transformation(df: pd.DataFrame) -> None:
    """Show log transformation for skewed data."""
    st.header("📐 Log Transformation")
    
    st.markdown("""
    **Problem**: Price distribution is heavily skewed (many cheap, few expensive).
    
    **Solution**: Log transformation makes it more normal!
    """)
    
    sample = df['price_10k_krw'].head(5000)
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    
    # Original
    axes[0].hist(sample, bins=50, alpha=0.7, color='steelblue', edgecolor='white')
    axes[0].set_title('Original Price Distribution')
    axes[0].set_xlabel('Price (10K KRW)')
    axes[0].axvline(sample.mean(), color='red', linestyle='--', label=f'Mean: {sample.mean():,.0f}')
    axes[0].axvline(sample.median(), color='green', linestyle='--', label=f'Median: {sample.median():,.0f}')
    axes[0].legend()
    
    # Log transformed
    log_sample = np.log1p(sample)  # log(1+x) to handle zeros
    axes[1].hist(log_sample, bins=50, alpha=0.7, color='green', edgecolor='white')
    axes[1].set_title('Log-Transformed Price')
    axes[1].set_xlabel('log(Price + 1)')
    axes[1].axvline(log_sample.mean(), color='red', linestyle='--', label=f'Mean: {log_sample.mean():.2f}')
    axes[1].axvline(log_sample.median(), color='green', linestyle='--', label=f'Median: {log_sample.median():.2f}')
    axes[1].legend()
    
    plt.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Skewness comparison
    col1, col2 = st.columns(2)
    
    with col1:
        skew_orig = sample.skew()
        st.metric("Original Skewness", f"{skew_orig:.2f}")
        st.caption("High skewness = heavily tailed")
    
    with col2:
        skew_log = log_sample.skew()
        st.metric("Log Skewness", f"{skew_log:.2f}")
        st.caption("Closer to 0 = more normal")
    
    st.code("""
import numpy as np

# Log transformation
price_log = np.log1p(df['price'])  # log(1 + x)

# To reverse: np.expm1(price_log)  # exp(x) - 1
""", language='python')


def display_feature_creation(df: pd.DataFrame) -> None:
    """Show creating new features."""
    st.header("🔨 Creating New Features")
    
    st.markdown("""
    **Idea**: Combine existing features to create more meaningful ones!
    """)
    
    df = df.copy()
    
    # Create features
    df['building_age'] = 2024 - df['year']
    df['price_per_m2'] = df['price_10k_krw'] / df['area_m2']
    df['floor_ratio'] = df['floor'] / 30  # Normalized floor
    
    st.markdown("### Example Feature Creations")
    
    features = [
        ("building_age", "2024 - year", "Years since construction"),
        ("price_per_m2", "price / area", "Price efficiency"),
        ("floor_ratio", "floor / max_floor", "Relative height"),
        ("area_squared", "area²", "Non-linear area effect"),
        ("is_new", "year > 2015", "Binary: new or not")
    ]
    
    feature_df = pd.DataFrame(features, columns=['Feature', 'Formula', 'Meaning'])
    st.dataframe(feature_df, use_container_width=True)
    
    st.code("""
# Create new features
df['building_age'] = 2024 - df['year']
df['price_per_m2'] = df['price'] / df['area']
df['is_new'] = (df['year'] > 2015).astype(int)
df['area_squared'] = df['area'] ** 2
""", language='python')
    
    # Show correlation of new features with price
    st.markdown("### Correlation with Price")
    
    corrs = {
        'area_m2': df['area_m2'].corr(df['price_10k_krw']),
        'year': df['year'].corr(df['price_10k_krw']),
        'building_age': df['building_age'].corr(df['price_10k_krw']),
        'price_per_m2': df['price_per_m2'].corr(df['price_10k_krw']),
    }
    
    corr_df = pd.DataFrame({
        'Feature': list(corrs.keys()),
        'Correlation': list(corrs.values())
    }).sort_values('Correlation', key=abs, ascending=False)
    
    fig, ax = plt.subplots(figsize=(8, 4))
    colors = ['green' if c > 0 else 'red' for c in corr_df['Correlation']]
    ax.barh(corr_df['Feature'], corr_df['Correlation'], color=colors)
    ax.set_xlabel('Correlation with Price')
    ax.set_title('Feature Correlation with Price')
    ax.axvline(x=0, color='black', linewidth=0.5)
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    return df


def display_polynomial_features(df: pd.DataFrame) -> None:
    """Show interactive polynomial features demo."""
    st.header("📈 Polynomial Features (Curve Fitting)")
    
    st.markdown("""
    **Linear Regression finds a straight line.**
    But what if the data is curved?
    
    **Polynomial Features** add powers of X ($x^2, x^3...$) to let the model bend!
    """)
    
    # User interaction
    degree = st.slider("Polynomial Degree (Complexity)", 1, 15, 2)
    
    # Create non-linear data
    np.random.seed(42)
    X = np.sort(np.random.rand(40, 1) * 10, axis=0) # 0 to 10
    y = np.cos(X).ravel() + np.random.randn(40) * 0.1 + (X.ravel()/5) # Wavy pattern
    
    # Train/Test Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    # Transform
    poly = PolynomialFeatures(degree=degree, include_bias=False)
    X_poly_train = poly.fit_transform(X_train)
    X_poly_test = poly.transform(X_test)
    
    # Fit
    model = LinearRegression()
    model.fit(X_poly_train, y_train)
    
    # Predict for smooth curve
    X_plot = np.linspace(0, 10, 100).reshape(-1, 1)
    X_plot_poly = poly.transform(X_plot)
    y_plot = model.predict(X_plot_poly)
    
    # Metrics
    rmse_train = np.sqrt(np.mean((y_train - model.predict(X_poly_train))**2))
    rmse_test = np.sqrt(np.mean((y_test - model.predict(X_poly_test))**2))
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(X_train, y_train, color='blue', alpha=0.6, label='Train Data')
    ax.scatter(X_test, y_test, color='red', alpha=0.6, label='Test Data')
    ax.plot(X_plot, y_plot, color='green', linewidth=2, label=f'Degree {degree} Fit')
    ax.set_ylim(-2, 4)
    ax.set_title(f"Degree {degree} | Train RMSE: {rmse_train:.2f} | Test RMSE: {rmse_test:.2f}")
    ax.legend()
    st.pyplot(fig, use_container_width=True)
    
    if degree == 1:
        st.info("Degree 1 = Straight Line (Underfitting)")
    elif degree < 5:
        st.success("Degree 2-4 = Good Fit (Captures the curve)")
    else:
        st.warning("High Degree = Wobbly Line (Overfitting to noise!)")
        
    st.code(f"""
# Create polynomial features of degree {degree}
poly = PolynomialFeatures(degree={degree}, include_bias=False)
X_poly = poly.fit_transform(X)
# Now X has {X_poly_train.shape[1]} columns!
""", language='python')


@st.cache_resource
def train_models(df: pd.DataFrame):
    """Train and compare different feature engineering approaches."""
    # Prepare base features
    df = df.copy()
    df['building_age'] = 2024 - df['year']
    
    X_base = df[['area_m2']].values
    y = df['price_10k_krw'].values
    
    X_train, X_test, y_train, y_test = train_test_split(
        X_base, y, test_size=0.2, random_state=RANDOM_STATE
    )
    
    results = {}
    
    # 1. No scaling
    model1 = LinearRegression()
    model1.fit(X_train, y_train)
    results['No Scaling'] = calculate_rmse(y_test, model1.predict(X_test))
    
    # 2. Standard scaling
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    model2 = LinearRegression()
    model2.fit(X_train_scaled, y_train)
    results['StandardScaler'] = calculate_rmse(y_test, model2.predict(X_test_scaled))
    
    # 3. Polynomial features
    poly = PolynomialFeatures(degree=2, include_bias=False)
    X_train_poly = poly.fit_transform(X_train)
    X_test_poly = poly.transform(X_test)
    model3 = LinearRegression()
    model3.fit(X_train_poly, y_train)
    results['Polynomial (d=2)'] = calculate_rmse(y_test, model3.predict(X_test_poly))
    
    # 4. Log transform target
    model4 = LinearRegression()
    model4.fit(X_train, np.log1p(y_train))
    y_pred_log = np.expm1(model4.predict(X_test))
    results['Log Target'] = calculate_rmse(y_test, y_pred_log)
    
    # 5. Multiple features + scaling
    X_multi = df[['area_m2', 'year', 'floor']].values
    X_train_m, X_test_m, y_train_m, y_test_m = train_test_split(
        X_multi, y, test_size=0.2, random_state=RANDOM_STATE
    )
    scaler_m = StandardScaler()
    X_train_ms = scaler_m.fit_transform(X_train_m)
    X_test_ms = scaler_m.transform(X_test_m)
    model5 = LinearRegression()
    model5.fit(X_train_ms, y_train_m)
    results['Multi + Scale'] = calculate_rmse(y_test_m, model5.predict(X_test_ms))
    
    return results


def display_comparison(results: dict) -> None:
    """Show comparison of different approaches."""
    st.header("📊 Performance Comparison")
    
    st.markdown("""
    Let's compare different feature engineering techniques:
    """)
    
    results_df = pd.DataFrame({
        'Technique': list(results.keys()),
        'RMSE': list(results.values())
    }).sort_values('RMSE')
    
    fig, ax = plt.subplots(figsize=(10, 5))
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(results_df)))
    bars = ax.barh(results_df['Technique'], results_df['RMSE'], color=colors)
    ax.set_xlabel('RMSE (lower is better)')
    ax.set_title('Feature Engineering Technique Comparison')
    
    # Add value labels
    for bar, val in zip(bars, results_df['RMSE']):
        ax.text(val + 500, bar.get_y() + bar.get_height()/2, 
                f'{val:,.0f}', va='center')
    
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Best result
    best = results_df.iloc[0]
    st.success(f"""
    **Best Technique**: {best['Technique']} with RMSE = {best['RMSE']:,.0f}
    """)
    
    st.dataframe(results_df, use_container_width=True)
    
    # Compare with other levels
    st.markdown("---")
    display_rmse_comparison(8, best['RMSE'])


def display_limitations() -> None:
    """Show limitations and next steps."""
    st.header("🤔 Questions You Might Have")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q1: Polynomial features made RMSE worse?</b><br>
        <span style="font-size: 13px;">
        Yes! More features can cause <b>overfitting</b>.<br>
        The model memorizes training data instead of learning patterns!<br>
        <i>→ Level 9 teaches Regularization to fix this!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q2: How do I know which features to create?</b><br>
        <span style="font-size: 13px;">
        Domain knowledge helps! For real estate:<br>
        • Price per m² (area efficiency)<br>
        • Distance to subway (convenience)<br>
        • School district ranking (families)<br>
        <i>→ This is why data scientists need domain expertise!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q3: Is there automatic feature engineering?</b><br>
        <span style="font-size: 13px;">
        Yes! AutoML tools can generate features automatically.<br>
        <i>→ Level 10 covers AutoML with PyCaret!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
    ### 🚀 What's Next in Level 9?
    
    | Level 8 (Now) | Level 9 (Next) |
    |---------------|----------------|
    | Add more features | Control feature impact |
    | Risk of overfitting | Regularization prevents it |
    | All coefficients free | Penalize large coefficients |
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        df = prepare_data(df)
        
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_why_level8()
        st.markdown("---")
        display_scaling_comparison(df)
        st.markdown("---")
        display_log_transformation(df)
        st.markdown("---")
        df = display_feature_creation(df)
        st.markdown("---")
        display_polynomial_features(df)
        st.markdown("---")
        
        # Train and compare
        with st.spinner("Comparing techniques..."):
            results = train_models(df)
        
        display_comparison(results)
        st.markdown("---")
        display_limitations()
        
        # Code Link
        
        display_code_link("Level_8_Feature_Engineering.ipynb")
        
        
        
        # Next level teaser
        display_next_level_teaser(8)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
