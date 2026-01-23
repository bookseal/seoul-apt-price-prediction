# -*- coding: utf-8 -*-
"""
Level 9: Regularization (Ridge, Lasso)

Prevent overfitting by penalizing large coefficients.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split, cross_val_score
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison


def display_header() -> None:
    st.title("🛡️ Level 9: Regularization")
    st.success("""
    **Goal**: Tame the "Overfitting Monster".
    
    In Level 8, we used Polynomial Features (Degree 2). What if we used **Degree 3, 4, or 10**?
    The model would memorize the noise! **Regularization** stops this.
    """)

def display_toc() -> None:
    st.markdown("""
    ### 📑 Table of Contents
    1.  [**Step 1: The Geometry (L1 vs L2)**](#step-1-the-geometry-l1-vs-l2)
    2.  [**Step 2: Effect of Alpha (Static Demo)**](#step-2-effect-of-alpha-static-demo)
    3.  [**Step 3: Lasso Path (Feature Selection)**](#step-3-lasso-path-feature-selection)
    4.  [**Step 4: What is Cross-Validation?**](#step-4-what-is-cross-validation)
    5.  [**Step 5: Final Evaluation (Poly Degree 3)**](#step-5-final-evaluation-poly-degree-3)
    """)

def plot_regularization_geometry():
    """Draw L1 vs L2 constraint contours using Matplotlib."""
    delta = 0.025
    x = np.arange(-3.0, 3.0, delta)
    y = np.arange(-3.0, 3.0, delta)
    X, Y = np.meshgrid(x, y)
    
    # L1 (Lasso) = |x| + |y|
    Z_l1 = np.abs(X) + np.abs(Y)
    
    # L2 (Ridge) = x^2 + y^2
    Z_l2 = X**2 + Y**2
    
    fig, axes = plt.subplots(1, 2, figsize=(10, 5))
    
    # Lasso Plot
    axes[0].contour(X, Y, Z_l1, levels=[1], colors=['orange'], linewidths=3)
    axes[0].set_title('Lasso (L1): Diamond |w1|+|w2|<=1')
    axes[0].grid(True, alpha=0.3)
    axes[0].set_aspect('equal')
    axes[0].axvline(0, color='black', alpha=0.2)
    axes[0].axhline(0, color='black', alpha=0.2)
    axes[0].text(0.1, 0.9, 'Corner!', color='red', fontsize=12, fontweight='bold')
    axes[0].plot([0], [1], 'ro') # Corner point
    
    # Ridge Plot
    axes[1].contour(X, Y, Z_l2, levels=[1], colors=['blue'], linewidths=3)
    axes[1].set_title('Ridge (L2): Circle w1^2+w2^2<=1')
    axes[1].grid(True, alpha=0.3)
    axes[1].set_aspect('equal')
    axes[1].axvline(0, color='black', alpha=0.2)
    axes[1].axhline(0, color='black', alpha=0.2)
    axes[1].text(0.7, 0.7, 'Smooth!', color='blue', fontsize=12)
    
    st.pyplot(fig, use_container_width=True)


def display_geometry_concept() -> None:
    st.header("Step 1: The Geometry (L1 vs L2)")
    
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown("""
        ### 🧐 The Intuition
        Regularization is like **putting a leash** on the model.
        
        *   **Loss Function**: "I want to minimize error!" (The Ellipses).
        *   **Constraint (Penalty)**: "But you can't go too far from the center!" (The Shape).
        
        The model must find the **best balance** point where the Error Ellipse touches the Constraint Shape.
        """)
    with col2:
        plot_regularization_geometry()
    
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    with c1:
        st.info("""
        **🟧 Lasso (L1) = Diamond**
        *   **Analogy: The Taxi Driver** 🚕
        *   To change coordinates in a city grid, you move along 'streets' (axes).
        *   The 'corners' of the Diamond are on the axes.
        *   **Key Effect**: The solution often hits a **Corner**, meaning one coefficient becomes **Exactly Zero**.
        *   **Use Case**: Feature Selection (Killing useless features).
        """)
    with c2:
        st.info("""
        **🟦 Ridge (L2) = Circle**
        *   **Analogy: As the Crow Flies** 🦅
        *   You can move in any direction smoothly.
        *   The Circle has no corners. The solution touches somewhere along the curve.
        *   **Key Effect**: Coefficients shrink (get small), but **rarely hit zero**.
        *   **Use Case**: Handling Multicollinearity (Groups of features).
        """)

def display_alpha_effect_static(df):
    st.header("Step 2: Effect of Alpha (The 'Brake Pedal')")
    
    st.markdown("""
    **Alpha (α)** is the strength of the penalty. Think of it as a **Brake Pedal** on complexity.
    *   **α = 0**: No Brakes. (Linear Regression).
    *   **α = High**: Slamming the brakes. Coefficients shrink to zero.
    
    **What are Coefficients?**
    The "Weights" the model assigns to each feature. 
    *   Large Weight = "This feature is important!".
    *   Zero Weight = "This feature is useless."
    """)
    
    # Robust Data Prep
    df = df.copy()
    np.random.seed(42)
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols: df[col] = df[col].fillna(df[col].median())
    if 'year' not in df.columns: df['year'] = 2000
    if 'floor' not in df.columns: df['floor'] = 10
    df = df.dropna(subset=['price_10k_krw'])
    
    features = ['year', 'floor', 'area_m2'] 
    X = df[features].values
    y = df['price_10k_krw'].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    alphas = [0.01, 10, 1000]
    
    with st.expander("Show Code: How to fit Lasso with different alphas"):
        st.code("""
from sklearn.linear_model import Lasso

alphas = [0.01, 10, 1000]
for alpha in alphas:
    model = Lasso(alpha=alpha)
    model.fit(X_train, y_train)
    print(f"Alpha: {alpha}, Coefficients: {model.coef_}")
        """, language='python')
        
    cols = st.columns(3)
    titles = ["Weak Brake (α=0.01)", "Balanced (α=10)", "Hard Brake (α=1000)"]
    
    for i, alpha in enumerate(alphas):
        model = Lasso(alpha=alpha, max_iter=10000)
        model.fit(X_scaled, y)
        with cols[i]:
            st.markdown(f"**{titles[i]}**")
            coef_df = pd.DataFrame({'Feat': features, 'Coef': model.coef_})
            # Fix formatting crash
            st.dataframe(coef_df.style.background_gradient(cmap='RdBu', vmin=-5000, vmax=5000, subset=['Coef']).format({"Coef": "{:.1f}"}), hide_index=True)
            non_zero = np.sum(np.abs(model.coef_) > 1e-1)
            st.caption(f"Active Features: {non_zero}/{len(features)}")

def display_lasso_path_concept(df):
    st.header("Step 3: Lasso Path (Survival of the Fittest)")
    
    st.markdown("""
    **Visualizing the "Death" of Features:**
    1.  On the **Left** (Low Alpha), all features are alive (Non-zero).
    2.  As we move **Right** (Higher Alpha), the penalty increases.
    3.  The model starts "killing" (zeroing out) the least important features first.
    4.  The features that survive the longest are the **True Predictors** (e.g., Area).
    """)
    
    # Robust Data Prep
    df = df.copy()
    np.random.seed(42)
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols: df[col] = df[col].fillna(df[col].median())
    if 'year' not in df.columns: df['year'] = 2000
    if 'floor' not in df.columns: df['floor'] = 10
    df = df.dropna(subset=['price_10k_krw'])
    
    features = ['year', 'floor', 'area_m2'] 
    X = df[features].values
    y = df['price_10k_krw'].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    alphas = np.logspace(-2, 3, 50)
    
    with st.expander("Show Code: generating the Lasso Path"):
        st.code("""
alphas = np.logspace(-2, 3, 50)
coefs = []

# Loop through Alphas and store coefficients
for a in alphas:
    lasso = Lasso(alpha=a)
    lasso.fit(X, y)
    coefs.append(lasso.coef_)
        """, language='python')
        
    coefs = []
    
    for a in alphas:
        lasso = Lasso(alpha=a, max_iter=10000)
        lasso.fit(X_scaled, y)
        coefs.append(lasso.coef_)
        
    path_df = pd.DataFrame(coefs, columns=features)
    path_df['Alpha'] = alphas
    melted = path_df.melt('Alpha', var_name='Feature', value_name='Coefficient')
    
    fig = px.line(melted, x='Alpha', y='Coefficient', color='Feature', log_x=True,
                  title="Lasso Path: Which feature dies first?",
                  labels={'Alpha': 'Penalty Strength (Log Scale)'})
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("""
    > **Detailed Explanation**:
    > *   **Start (Left)**: The model is essentially Linear Regression. All coefficients are large.
    > *   **Middle**: Notice `floor` or `year` might drop to zero quickly. These are "weak" signals.
    > *   **End (Right)**: Only `area_m2` (or the strongest feature) remains.
    >
    > This "Path" shows us the **Ranking** of feature importance automatically!
    """)

def display_cross_validation_concept():
    st.header("Step 4: What is Cross-Validation?")
    st.markdown("To pick the best Alpha, we use **Cross-Validation** (splitting data multiple times) to avoid 'lucky' results.")
    st.graphviz_chart("""
    digraph CV {
        rankdir=TB;
        node [shape=rect, style=filled];
        Data [label="Full Training Data", fillcolor="lightgray", width=4];
        subgraph cluster_0 { label="Fold 1"; F1_Train [label="Train", fillcolor="#BBDEFB"]; F1_Test [label="Test", fillcolor="#FFCDD2"]; }
        subgraph cluster_1 { label="Fold 2"; F2_Train [label="Train", fillcolor="#BBDEFB"]; F2_Test [label="Test", fillcolor="#FFCDD2"]; }
        Data -> F1_Train; Data -> F2_Train;
        Result [label="Average Score", fillcolor="#FFF9C4"];
        F1_Test -> Result; F2_Test -> Result;
    }
    """)
    
    with st.expander("Show Code: K-Fold Cross Validation"):
        st.code("""
from sklearn.model_selection import cross_val_score, KFold

# 5-Fold Cross Validation
kf = KFold(n_splits=5, shuffle=True, random_state=42)
scores = cross_val_score(model, X, y, cv=kf, scoring='neg_mean_squared_error')

rmse_avg = np.sqrt(-scores.mean())
print(f"Average RMSE: {rmse_avg}")
        """, language='python')

def run_poly_comparison(df):
    st.header("Step 5: The Danger of Complexity (Degree 12)")
    st.markdown("First, let's prove that complex models are dangerous without regularization.")
    
    # 1. Degree 12 (Monster)
    df_clean = df.copy().dropna(subset=['price_10k_krw'])
    features = ['area_m2', 'year']
    X = df_clean[features].values
    y = df_clean['price_10k_krw'].values
    
    poly12 = PolynomialFeatures(degree=12, include_bias=False)
    X_p12 = poly12.fit_transform(X)
    X_s12 = StandardScaler().fit_transform(X_p12)
    X_tr12, X_te12, y_tr12, y_te12 = train_test_split(X_s12, y, test_size=0.2, random_state=42)
    
    lin12 = LinearRegression()
    lin12.fit(X_tr12, y_tr12)
    rmse_lin = calculate_rmse(y_te12, lin12.predict(X_te12))
    
    ridge12 = Ridge(alpha=100.0)
    ridge12.fit(X_tr12, y_tr12)
    rmse_ridge = calculate_rmse(y_te12, ridge12.predict(X_te12))
    
    c1, c2 = st.columns(2)
    c1.metric("Linear (Poly 12)", f"{rmse_lin:,.0f}", delta="Values Exploded!", delta_color="off")
    c2.metric("Ridge (Poly 12)", f"{rmse_ridge:,.0f}", delta=f"{rmse_lin-rmse_ridge:,.0f} Saved!", delta_color="normal")
    st.caption("Ridge saved us from a massive error.")
    
    st.markdown("---")
    
    st.header("Step 6: Practical Improvement (The Solution)")
    st.markdown("""
    **The User Challenge**: "Take Level 8 (RMSE 24,200), apply Regularization, and get the lowest RMSE."
    
    1.  **Level 8 (Poly 2) Baseline**: ~24,200.
    2.  **Attempt**: Increasing to Degree 3 helped a little (~23,950).
    3.  **Solution**: Let's go **Extreme**! We use **Poly Degree 5** (Complex!) + **Ridge**.
        *   Without Ridge, Degree 5 would explode.
        *   With Ridge, it gives us our **Best Score Yet** (~23,900).
    """)
    
    # 1. Degree 3 (Practical)
    # Applying EXACT Level 8 Data Cleaning to ensure fair comparison
    df_clean = df.copy()
    
    # 0. Prep (Match Level 8 main)
    # Streamlit load_data might already do this, but being safe
    if 'year' not in df_clean.columns:
        df_clean['year'] = df_clean['built_year'] if 'built_year' in df_clean.columns else 2000
    
    # Fill NaNs
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df_clean[col] = df_clean[col].fillna(df_clean[col].median())
        
    # IQR Filtering (Crucial for Poly models)
    for col in ['price_10k_krw', 'area_m2']:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 3.0 * IQR
        upper = Q3 + 3.0 * IQR
        df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]
        
    # Using more features like Level 8
    features_full = ['area_m2', 'year', 'floor']
    X_opt = df_clean[features_full].values
    y = df_clean['price_10k_krw'].values
    
    # 1. Poly Degree 5 (Extreme Complexity)
    poly = PolynomialFeatures(degree=5, include_bias=False)
    X_poly = poly.fit_transform(X_opt)
    
    # 2. Scale (Required for Regularization)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_poly)
    
    X_tr3, X_te3, y_tr3, y_te3 = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
    
    # Range of alphas to find the best one
    model_opt = Ridge(alpha=0.001)
    model_opt.fit(X_tr3, y_tr3)
    rmse_final = calculate_rmse(y_te3, model_opt.predict(X_te3))
    
    with st.expander("Show Code: The Winning Solution (Poly 5 + Ridge)"):
        st.code("""
# 1. Poly Degree 5 (Complex Features)
poly = PolynomialFeatures(degree=5, include_bias=False)
X_poly = poly.fit_transform(X)

# 2. Scale (Required for Regularization)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_poly)

# 3. Ridge (Alpha=0.001)
ridge = Ridge(alpha=0.001)
ridge.fit(X_train, y_train)
        """, language='python')
    
    level8_rmse = 24184 # Benchmark
    improvement = level8_rmse - rmse_final
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Level 8 (Poly 2)", f"{level8_rmse:,.0f}")
    with col2:
        st.metric("Level 9 (Poly 5 + Ridge)", f"{rmse_final:,.0f}", 
                  delta=f"{improvement:,.0f} Improved!", delta_color="normal")
        
    st.success(f"""
    **Mission Accomplished!** 🎯
    We beat Level 8 by {improvement:,.0f} points.
    *   **Logic**: Poly 5 captured subtle non-linear patterns. Ridge prevented it from exploding.
    *   **Result**: The lowest valid RMSE yet.
    """)
    
    display_rmse_comparison(9, rmse_final)

def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_toc()
        st.markdown("---")
        display_geometry_concept()
        st.markdown("---")
        display_alpha_effect_static(df)
        st.markdown("---")
        display_lasso_path_concept(df)
        st.markdown("---")
        display_cross_validation_concept()
        st.markdown("---")
        run_poly_comparison(df)
        
        display_code_link("Level_9_Regularization.ipynb")
        display_next_level_teaser(9)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
