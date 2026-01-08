# -*- coding: utf-8 -*-
"""
Level 5: High-Dimensional Regression (10+ Features)

Explore what happens when we add many features.
Learn about the curse of dimensionality and visualization limits.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE


def display_header() -> None:
    """Display Level 5 introduction."""
    st.title("🌌 Level 5: High-Dimensional Regression")
    
    st.success("""
    **Goal**: Use MANY features to improve predictions.
    
    But wait... How do we visualize 10+ dimensions?
    """)
    
    with st.expander("💡 What is High-Dimensional Data?"):
        st.markdown("""
        **Dimensions = Number of Features**
        
        - Level 2: 1D (Area only)
        - Level 3: 2D (Area + District)
        - Level 4: 3D (Area + District + Year)
        - Level 5: **10D+** (Many features!)
        
        **The challenge**: We can't draw a 10D scatter plot!
        """)


def display_pipeline_overview() -> None:
    """Show the pipeline."""
    st.header("🔄 Level 5 Pipeline")
    
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>1. Load</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>2. Select Features</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #00BCD4, #0097A7); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>3. Encode & Scale</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>4. Train</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>5. Analyze</b>
        </div>
    </div>
    """, unsafe_allow_html=True)


def display_why_level5() -> None:
    """Explain motivation for Level 5."""
    st.header("🤔 Why More Features?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 10px 0;">
        <b>Real estate prices depend on MANY factors!</b><br>
        <span style="font-size: 13px;">
        • Area (m²)<br>
        • District (location)<br>
        • Building Year<br>
        • Floor number<br>
        • Total units in building<br>
        • Parking ratio<br>
        • Distance to subway<br>
        • Nearby schools<br>
        • And more...
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    **Question**: If we use ALL these features, will our model be better?
    
    **Answer**: Maybe! But there are challenges...
    """)


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare multi-dimensional feature set."""
    df = df.copy()
    
    # Add synthetic features if not present
    np.random.seed(RANDOM_STATE)
    n = len(df)
    
    if 'year' not in df.columns:
        df['year'] = np.random.randint(1985, 2024, n)
    
    if 'floor' not in df.columns:
        df['floor'] = np.random.randint(1, 30, n)
    
    # Create additional features
    df['building_age'] = 2024 - df['year']
    df['price_per_m2'] = df['price_10k_krw'] / df['area_m2']
    
    # Synthetic features for demonstration
    df['total_units'] = np.random.randint(100, 2000, n)
    df['parking_ratio'] = np.random.uniform(0.5, 2.0, n)
    df['floor_ratio'] = df['floor'] / 30  # Normalized floor
    
    return df


def display_feature_selection(df: pd.DataFrame) -> list:
    """Let user select features."""
    st.header("🎯 Feature Selection")
    
    st.markdown("""
    **Choose which features to include in the model:**
    """)
    
    available_features = {
        'area_m2': 'Area (m²) - Size of apartment',
        'year': 'Building Year - When it was built',
        'floor': 'Floor Number - Which floor',
        'building_age': 'Building Age - Years since construction',
        'total_units': 'Total Units - Size of complex',
        'parking_ratio': 'Parking Ratio - Cars per household',
        'floor_ratio': 'Floor Ratio - Relative floor position'
    }
    
    # Default selection
    default_features = ['area_m2', 'year', 'floor', 'building_age']
    
    selected = st.multiselect(
        "Select numeric features",
        options=list(available_features.keys()),
        default=default_features,
        format_func=lambda x: f"{x}: {available_features[x]}"
    )
    
    include_district = st.checkbox("Include District (One-Hot Encoded)", value=True)
    
    st.info(f"""
    **Selected Features**: {len(selected)} numeric + {'25 district columns' if include_district else '0 categorical'}
    
    **Total Dimensions**: ~{len(selected) + (25 if include_district else 0)}
    """)
    
    return selected, include_district


def display_correlation_heatmap(df: pd.DataFrame, features: list) -> None:
    """Show correlation between features."""
    st.header("📊 Feature Correlation Heatmap")
    
    st.markdown("""
    **Are features related to each other?**
    
    High correlation between features = redundant information
    """)
    
    # Include price
    cols = features + ['price_10k_krw']
    corr_matrix = df[cols].corr()
    
    fig, ax = plt.subplots(figsize=(10, 8))
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
    sns.heatmap(
        corr_matrix, 
        mask=mask,
        annot=True, 
        fmt='.2f',
        cmap='RdYlBu_r',
        center=0,
        square=True,
        ax=ax
    )
    ax.set_title('Feature Correlation Matrix')
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Find highest correlations with price
    price_corr = corr_matrix['price_10k_krw'].drop('price_10k_krw').sort_values(key=abs, ascending=False)
    
    st.markdown("### 🎯 Correlation with Price")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Most Correlated:**")
        for feat, corr in price_corr.head(3).items():
            color = "green" if corr > 0 else "red"
            st.markdown(f"- {feat}: <span style='color:{color}'>{corr:.3f}</span>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("**Least Correlated:**")
        for feat, corr in price_corr.tail(3).items():
            st.markdown(f"- {feat}: {corr:.3f}")


@st.cache_resource
def train_model(df: pd.DataFrame, features: list, include_district: bool):
    """Train high-dimensional model."""
    # Prepare features
    X_numeric = df[features].values
    
    if include_district:
        encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
        X_district = encoder.fit_transform(df[['district']])
        X = np.hstack([X_numeric, X_district])
        feature_names = features + list(encoder.categories_[0])
    else:
        encoder = None
        X = X_numeric
        feature_names = features
    
    y = df['price_10k_krw'].values
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=RANDOM_STATE
    )
    
    # Train
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    rmse_train = calculate_rmse(y_train, y_pred_train)
    rmse_test = calculate_rmse(y_test, y_pred_test)
    
    return model, scaler, encoder, feature_names, rmse_train, rmse_test, (y_test, y_pred_test)


def display_feature_importance(model, feature_names: list, n_numeric: int) -> None:
    """Show feature importance from coefficients."""
    st.header("📊 Feature Importance")
    
    st.markdown("""
    **Which features matter most?**
    
    Larger absolute coefficient = More impact on price
    """)
    
    # Get coefficients
    coefs = model.coef_
    
    # Create importance dataframe
    importance_df = pd.DataFrame({
        'Feature': feature_names,
        'Coefficient': coefs,
        'Abs_Coef': np.abs(coefs)
    }).sort_values('Abs_Coef', ascending=False)
    
    # Show top features
    st.markdown("### Top 10 Most Important Features")
    
    top_10 = importance_df.head(10)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ['#4CAF50' if c > 0 else '#F44336' for c in top_10['Coefficient']]
    bars = ax.barh(range(len(top_10)), top_10['Coefficient'], color=colors)
    ax.set_yticks(range(len(top_10)))
    ax.set_yticklabels(top_10['Feature'])
    ax.axvline(x=0, color='black', linewidth=0.5)
    ax.set_xlabel('Coefficient (Standardized)')
    ax.set_title('Top 10 Feature Importance')
    ax.invert_yaxis()
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 15px 0;">
        <b>💡 Interpretation</b><br>
        <span style="font-size: 13px;">
        • <span style="color:#4CAF50">Green bars</span>: Increase price<br>
        • <span style="color:#F44336">Red bars</span>: Decrease price<br>
        • Longer bar = Stronger effect
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_evaluation(rmse_train: float, rmse_test: float, n_features: int) -> None:
    """Show model performance."""
    st.header("📏 Model Performance")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Train RMSE", f"{rmse_train:,.0f}")
    
    with col2:
        st.metric("Test RMSE", f"{rmse_test:,.0f}")
    
    with col3:
        overfit = rmse_test - rmse_train
        st.metric("Overfit Gap", f"{overfit:,.0f}", 
                  delta="Good" if overfit < 1000 else "Warning!")
    
    # Overfitting warning
    if rmse_test > rmse_train * 1.1:
        st.warning("""
        ⚠️ **Potential Overfitting Detected!**
        
        Test error is significantly higher than train error.
        This can happen when we have too many features!
        """)
    
    st.markdown(f"""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 15px 0;">
        <b>📊 Summary</b><br>
        <span style="font-size: 13px;">
        • Number of features: <b>{n_features}</b><br>
        • More features can improve predictions...<br>
        • But too many can cause <b>overfitting</b>!
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_visualization_problem() -> None:
    """Explain the visualization challenge."""
    st.header("🎨 The Visualization Problem")
    
    st.markdown("""
    <div style="padding: 20px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 15px 0;">
        <b>❌ We Can't Visualize 10D Data!</b><br><br>
        <span style="font-size: 14px;">
        • 1D: Line<br>
        • 2D: Scatter plot<br>
        • 3D: 3D scatter plot<br>
        • 4D: Color? Size?<br>
        • 5D+: <b>????</b><br><br>
        <i>Human brains can only perceive 3 spatial dimensions!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### What We Can Do:
        - View 2 features at a time
        - Use color for 3rd feature
        - Use size for 4th feature
        - Create multiple plots
        """)
    
    with col2:
        st.markdown("""
        ### Better Solutions:
        - **PCA**: Reduce dimensions
        - **t-SNE**: Embed in 2D/3D
        - **Feature Selection**: Use fewer features
        """)
    
    st.info("""
    **💡 Preview of Level 6**: We'll learn about **PCA (Principal Component Analysis)** 
    which can compress 10+ dimensions into 2-3 dimensions while keeping most information!
    """)


def display_curse_of_dimensionality() -> None:
    """Explain the curse of dimensionality."""
    st.header("👻 The Curse of Dimensionality")
    
    st.markdown("""
    **More dimensions = More problems!**
    """)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(156,39,176,0.1); border-radius: 10px; 
                border-left: 4px solid #9C27B0; margin: 10px 0;">
        <b>Problem 1: Data Becomes Sparse</b><br>
        <span style="font-size: 13px;">
        In high dimensions, data points are far apart.<br>
        Imagine 100 points in a 1D line vs 100 points in a 10D space!
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(156,39,176,0.1); border-radius: 10px; 
                border-left: 4px solid #9C27B0; margin: 10px 0;">
        <b>Problem 2: Overfitting Risk</b><br>
        <span style="font-size: 13px;">
        More features = More parameters to learn<br>
        Model can memorize training data instead of learning patterns!
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(156,39,176,0.1); border-radius: 10px; 
                border-left: 4px solid #9C27B0; margin: 10px 0;">
        <b>Problem 3: Noise Accumulation</b><br>
        <span style="font-size: 13px;">
        Some features might be irrelevant or noisy.<br>
        Adding them can hurt performance!
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Visual demonstration
    st.markdown("### 📈 Demo: How Dimensions Affect Distance")
    
    n_points = 100
    dims = [1, 2, 5, 10, 50, 100]
    avg_distances = []
    
    for d in dims:
        np.random.seed(42)
        points = np.random.rand(n_points, d)
        # Average distance between all pairs
        from scipy.spatial.distance import pdist
        distances = pdist(points)
        avg_distances.append(np.mean(distances))
    
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(dims, avg_distances, 'o-', linewidth=2, markersize=8)
    ax.set_xlabel('Number of Dimensions')
    ax.set_ylabel('Average Distance Between Points')
    ax.set_title('As Dimensions Increase, Points Get Further Apart')
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)
    plt.close()


def display_limitations() -> None:
    """Show limitations and next steps."""
    st.header("🤔 Questions You Might Have")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q1: How do we reduce dimensions?</b><br>
        <span style="font-size: 13px;">
        PCA (Principal Component Analysis) can compress many features into fewer!<br>
        <i>→ Level 6 teaches PCA!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q2: What about bad data (nulls, outliers)?</b><br>
        <span style="font-size: 13px;">
        We've been assuming clean data. Real data is messy!<br>
        <i>→ Level 7 covers data cleaning!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q3: Should we add more features or improve existing ones?</b><br>
        <span style="font-size: 13px;">
        Feature engineering (creating better features) can be more valuable!<br>
        <i>→ Level 8 explores feature engineering!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
    ### 🚀 What's Next in Level 6?
    
    | Level 5 (Now) | Level 6 (Next) |
    |---------------|----------------|
    | 10+ dimensions | Reduce to 2-3 with PCA |
    | Can't visualize | Can visualize again! |
    | Risk of overfitting | Compressed, robust features |
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        df = prepare_features(df)
        
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_why_level5()
        st.markdown("---")
        
        selected_features, include_district = display_feature_selection(df)
        
        if len(selected_features) < 2:
            st.warning("Please select at least 2 features!")
            return
        
        st.markdown("---")
        display_correlation_heatmap(df, selected_features)
        st.markdown("---")
        
        # Train model
        with st.spinner("Training high-dimensional model..."):
            model, scaler, encoder, feature_names, rmse_train, rmse_test, (y_test, y_pred) = train_model(
                df, selected_features, include_district
            )
        
        display_feature_importance(model, feature_names, len(selected_features))
        st.markdown("---")
        display_evaluation(rmse_train, rmse_test, len(feature_names))
        st.markdown("---")
        display_visualization_problem()
        st.markdown("---")
        display_curse_of_dimensionality()
        st.markdown("---")
        display_limitations()
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
