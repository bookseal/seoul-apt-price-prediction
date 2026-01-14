# -*- coding: utf-8 -*-
"""
Level 9: Regularization (Ridge, Lasso, ElasticNet)

Learn to prevent overfitting with regularization techniques.
Control model complexity by penalizing large coefficients.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split, cross_val_score
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison


def display_header() -> None:
    """Display Level 9 introduction."""
    st.title("🛡️ Level 9: Regularization")
    
    st.success("""
    **Goal**: Prevent overfitting with Ridge, Lasso, and ElasticNet.
    
    Don't let your model memorize - make it generalize!
    """)
    
    with st.expander("💡 What is Regularization?"):
        st.markdown("""
        **Regularization** = Adding a penalty for model complexity.
        
        **Problem**: Models with many features can "overfit":
        - Perfect on training data
        - Terrible on new data
        
        **Solution**: Penalize large coefficients!
        - Forces model to use simpler patterns
        - Improves performance on new data
        """)


def display_pipeline_overview() -> None:
    """Show the pipeline."""
    st.header("🔄 Level 9 Pipeline")
    
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>1. Overfit Demo</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>2. Ridge (L2)</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>3. Lasso (L1)</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>4. Compare</b>
        </div>
    </div>
    """, unsafe_allow_html=True)


def display_why_level9() -> None:
    """Explain motivation for Level 9."""
    st.header("🤔 The Overfitting Problem")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 10px 0;">
        <b>❌ Level 8's Problem: Polynomial features caused overfitting!</b><br>
        <span style="font-size: 13px;">
        More features = More parameters = Model can memorize training data!<br><br>
        <b>Symptoms of overfitting:</b><br>
        • Training error: Very low ✅<br>
        • Test error: Very high ❌<br>
        • Large coefficient values
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Visual demo of overfitting
    st.markdown("### 📈 Overfitting Visualization")
    
    np.random.seed(42)
    X = np.linspace(0, 1, 20).reshape(-1, 1)
    y = np.sin(2 * np.pi * X).ravel() + np.random.randn(20) * 0.3
    
    X_plot = np.linspace(0, 1, 100).reshape(-1, 1)
    
    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    
    # Underfitting (degree=1)
    poly1 = PolynomialFeatures(degree=1)
    X_poly1 = poly1.fit_transform(X)
    model1 = LinearRegression()
    model1.fit(X_poly1, y)
    X_plot_poly1 = poly1.transform(X_plot)
    
    axes[0].scatter(X, y, color='blue', s=50, alpha=0.7)
    axes[0].plot(X_plot, model1.predict(X_plot_poly1), 'r-', linewidth=2)
    axes[0].set_title('Underfitting (degree=1)\nToo simple!')
    axes[0].set_xlabel('X')
    axes[0].set_ylabel('Y')
    
    # Good fit (degree=3)
    poly3 = PolynomialFeatures(degree=3)
    X_poly3 = poly3.fit_transform(X)
    model3 = LinearRegression()
    model3.fit(X_poly3, y)
    X_plot_poly3 = poly3.transform(X_plot)
    
    axes[1].scatter(X, y, color='blue', s=50, alpha=0.7)
    axes[1].plot(X_plot, model3.predict(X_plot_poly3), 'g-', linewidth=2)
    axes[1].set_title('Good Fit (degree=3)\nJust right!')
    axes[1].set_xlabel('X')
    axes[1].set_ylabel('Y')
    
    # Overfitting (degree=15)
    poly15 = PolynomialFeatures(degree=15)
    X_poly15 = poly15.fit_transform(X)
    model15 = LinearRegression()
    model15.fit(X_poly15, y)
    X_plot_poly15 = poly15.transform(X_plot)
    
    axes[2].scatter(X, y, color='blue', s=50, alpha=0.7)
    y_pred_15 = model15.predict(X_plot_poly15)
    y_pred_15 = np.clip(y_pred_15, -3, 3)  # Clip for visualization
    axes[2].plot(X_plot, y_pred_15, 'orange', linewidth=2)
    axes[2].set_title('Overfitting (degree=15)\nToo complex!')
    axes[2].set_xlabel('X')
    axes[2].set_ylabel('Y')
    axes[2].set_ylim(-2, 2)
    
    plt.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close()


def display_regularization_concept() -> None:
    """Explain regularization concept."""
    st.header("📚 Regularization Explained")
    
    st.markdown("""
    ### The Basic Idea
    
    **Normal Linear Regression**: Minimize error only
    
    **Regularized**: Minimize error + Penalty for large coefficients
    """)
    
    st.latex(r"\text{Loss} = \text{Error} + \lambda \times \text{Penalty}")
    
    st.markdown("""
    - **λ (lambda/alpha)**: Controls penalty strength
    - Higher λ = Simpler model (smaller coefficients)
    - Lower λ = More complex model (larger coefficients)
    """)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### Ridge (L2)
        
        **Penalty**: Sum of squared coefficients
        
        **Effect**: Shrinks ALL coefficients toward zero
        
        **Good for**: Many correlated features
        """)
        st.latex(r"\lambda \sum w_i^2")
    
    with col2:
        st.markdown("""
        ### Lasso (L1)
        
        **Penalty**: Sum of absolute coefficients
        
        **Effect**: Can make some coefficients EXACTLY zero
        
        **Good for**: Feature selection
        """)
        st.latex(r"\lambda \sum |w_i|")
    
    with col3:
        st.markdown("""
        ### ElasticNet
        
        **Penalty**: Mix of L1 and L2
        
        **Effect**: Best of both worlds
        
        **Good for**: Many features, some correlated
        """)
        st.latex(r"\lambda_1 \sum |w_i| + \lambda_2 \sum w_i^2")
    
    st.code("""
from sklearn.linear_model import Ridge, Lasso, ElasticNet

# Ridge Regression (L2)
ridge = Ridge(alpha=1.0)
ridge.fit(X, y)

# Lasso Regression (L1)
lasso = Lasso(alpha=1.0)
lasso.fit(X, y)

# ElasticNet (L1 + L2)
elastic = ElasticNet(alpha=1.0, l1_ratio=0.5)
elastic.fit(X, y)
""", language='python')


def prepare_data(df: pd.DataFrame):
    """Prepare data for regularization demo."""
    df = df.copy()
    np.random.seed(RANDOM_STATE)
    n = len(df)
    
    if 'year' not in df.columns:
        df['year'] = np.random.randint(1985, 2024, n)
    if 'floor' not in df.columns:
        df['floor'] = np.random.randint(1, 30, n)
    
    # Create more features
    df['building_age'] = 2024 - df['year']
    df['area_sq'] = df['area_m2'] ** 2
    df['floor_sq'] = df['floor'] ** 2
    df['age_sq'] = df['building_age'] ** 2
    df['area_floor'] = df['area_m2'] * df['floor']
    df['area_age'] = df['area_m2'] * df['building_age']
    
    return df


def display_alpha_slider(df: pd.DataFrame) -> None:
    """Interactive alpha slider to see coefficient changes (Plotly)."""
    st.header("🎮 Interactive Alpha Slider")
    
    st.markdown("""
    **Adjust α (alpha) to see how regularization affects coefficients!**
    Hover over bars to see the exact penalty effect.
    """)
    
    df = prepare_data(df)
    feature_cols = ['area_m2', 'year', 'floor', 'building_age', 
                    'area_sq', 'floor_sq', 'age_sq', 'area_floor', 'area_age']
    
    X = df[feature_cols].values
    y = df['price_10k_krw'].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Alpha slider
    col1, col2 = st.columns([1, 2])
    with col1:
        alpha = st.slider("Alpha Strength", 0.01, 100.0, 1.0, 0.1)
        model_type = st.radio("Model", ["Ridge (L2)", "Lasso (L1)"], horizontal=True)
    
    # Fit model
    if model_type == "Ridge (L2)":
        model = Ridge(alpha=alpha)
    else:
        model = Lasso(alpha=alpha, max_iter=10000)
    
    model.fit(X_scaled, y)
    
    # Plot coefficients
    coef_df = pd.DataFrame({
        'Feature': feature_cols,
        'Coefficient': model.coef_
    }).sort_values('Coefficient', key=abs, ascending=False)
    
    with col2:
        fig = px.bar(coef_df, x='Coefficient', y='Feature', orientation='h',
                     title=f"{model_type} Coefficients (α={alpha})",
                     color='Coefficient', color_continuous_scale='RdBu')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Stats
    zero_count = np.sum(np.abs(model.coef_) < 1e-5)
    st.info(f"**Stats**: {zero_count} features have been penalized to ZERO (Removed).")


def display_coefficient_path(df: pd.DataFrame) -> None:
    """Show coefficient path as alpha changes (Plotly)."""
    st.header("📈 Coefficient Path")
    
    st.markdown("""
    **Watch features 'die' as Penalty increases.**
    This is how Lasso selects the best features automatically!
    """)
    
    df = prepare_data(df)
    feature_cols = ['area_m2', 'year', 'floor', 'building_age', 'area_sq']
    X = df[feature_cols].values
    y = df['price_10k_krw'].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    alphas = np.logspace(-2, 3, 50)
    lasso_coefs = []
    
    for a in alphas:
        m = Lasso(alpha=a, max_iter=10000)
        m.fit(X_scaled, y)
        lasso_coefs.append(m.coef_)
        
    path_df = pd.DataFrame(lasso_coefs, columns=feature_cols)
    path_df['Alpha'] = alphas
    
    # Melt for plotly
    melted = path_df.melt('Alpha', var_name='Feature', value_name='Coefficient')
    
    fig = px.line(melted, x='Alpha', y='Coefficient', color='Feature', log_x=True,
                  title='Lasso Path: Which features survive?',
                  labels={'Alpha': 'Penalty Strength (Log Scale)'})
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.info("""
    **How to read this:**
    1. **Left (Small Alpha)**: All features have non-zero values.
    2. **Middle**: As we move right, lines hit 0. These features are "dropped".
    3. **Right (Large Alpha)**: Only the strongest features line survives longest!
    """)


@st.cache_resource
def run_cross_validation(df: pd.DataFrame):
    """Run cross-validation for different models."""
    df = prepare_data(df)
    
    feature_cols = ['area_m2', 'year', 'floor', 'building_age', 'area_sq']
    
    X = df[feature_cols].values
    y = df['price_10k_krw'].values
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    results = {}
    
    # Linear Regression
    lr = LinearRegression()
    scores = cross_val_score(lr, X_scaled, y, cv=5, scoring='neg_root_mean_squared_error')
    results['Linear'] = -scores.mean()
    
    # Ridge with different alphas
    for alpha in [0.1, 1.0, 10.0, 100.0]:
        ridge = Ridge(alpha=alpha)
        scores = cross_val_score(ridge, X_scaled, y, cv=5, scoring='neg_root_mean_squared_error')
        results[f'Ridge(α={alpha})'] = -scores.mean()
    
    # Lasso with different alphas
    for alpha in [0.1, 1.0, 10.0, 100.0]:
        lasso = Lasso(alpha=alpha, max_iter=10000)
        scores = cross_val_score(lasso, X_scaled, y, cv=5, scoring='neg_root_mean_squared_error')
        results[f'Lasso(α={alpha})'] = -scores.mean()
    
    # ElasticNet
    elastic = ElasticNet(alpha=1.0, l1_ratio=0.5, max_iter=10000)
    scores = cross_val_score(elastic, X_scaled, y, cv=5, scoring='neg_root_mean_squared_error')
    results['ElasticNet'] = -scores.mean()
    
    return results


def display_comparison(results: dict) -> None:
    """Show comparison of different models."""
    st.header("📊 Model Comparison (Cross-Validation)")
    
    st.markdown("""
    **Which regularization works best for our data?**
    
    Using 5-fold cross-validation to get reliable estimates.
    """)
    
    results_df = pd.DataFrame({
        'Model': list(results.keys()),
        'RMSE': list(results.values())
    }).sort_values('RMSE')
    
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(results_df)))
    bars = ax.barh(results_df['Model'], results_df['RMSE'], color=colors)
    ax.set_xlabel('Cross-Validation RMSE (lower is better)')
    ax.set_title('Regularization Comparison')
    
    for bar, val in zip(bars, results_df['RMSE']):
        ax.text(val + 200, bar.get_y() + bar.get_height()/2, 
                f'{val:,.0f}', va='center')
    
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    best = results_df.iloc[0]
    st.success(f"""
    **Best Model**: {best['Model']} with RMSE = {best['RMSE']:,.0f}
    """)
    
    st.dataframe(results_df, use_container_width=True)
    
    # Compare with other levels
    st.markdown("---")
    display_rmse_comparison(9, best['RMSE'])


def display_limitations() -> None:
    """Show limitations and next steps."""
    st.header("🤔 Questions You Might Have")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q1: How do I choose the best alpha?</b><br>
        <span style="font-size: 13px;">
        Use <b>Cross-Validation</b>! Try different values and pick the one with lowest CV error.<br>
        Scikit-learn has `RidgeCV` and `LassoCV` that do this automatically!
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q2: We've only used Linear Regression. Are there better models?</b><br>
        <span style="font-size: 13px;">
        Yes! Decision Trees, Random Forest, XGBoost, Neural Networks...<br>
        <i>→ Level 10 uses AutoML to compare many models automatically!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q3: Is linear regression enough for real estate prediction?</b><br>
        <span style="font-size: 13px;">
        For learning, yes! For production, you'd want to try ensemble methods.<br>
        <i>→ Level 10 is the finale - AutoML comparison!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
    ### 🚀 What's Next in Level 10?
    
    | Level 9 (Now) | Level 10 (Finale) |
    |---------------|-------------------|
    | Linear models only | Many model types |
    | Manual tuning | AutoML |
    | Ridge/Lasso | RF, XGBoost, LightGBM |
    
    **Ready for the grand finale? Level 10 awaits!** 🎉
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_why_level9()
        st.markdown("---")
        display_regularization_concept()
        st.markdown("---")
        display_alpha_slider(df)
        st.markdown("---")
        display_coefficient_path(df)
        st.markdown("---")
        
        with st.spinner("Running cross-validation..."):
            results = run_cross_validation(df)
        
        display_comparison(results)
        st.markdown("---")
        display_limitations()
        
        # Code Link
        
        display_code_link("Level_9_Regularization.ipynb")
        
        
        
        # Next level teaser
        display_next_level_teaser(9)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
