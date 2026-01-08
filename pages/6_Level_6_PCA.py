# -*- coding: utf-8 -*-
"""
Level 6: PCA (Principal Component Analysis)

Learn about dimensionality reduction.
Compress many features into fewer while keeping most information.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser


def display_header() -> None:
    """Display Level 6 introduction."""
    st.title("🔬 Level 6: PCA (Principal Component Analysis)")
    
    st.success("""
    **Goal**: Compress many features into fewer dimensions.
    
    Keep the important information, discard the noise!
    """)
    
    with st.expander("💡 What is PCA?"):
        st.markdown("""
        **PCA** = Find new axes that capture the most variance in data.
        
        Imagine you have 100 features. PCA can:
        - Find that 95% of the information is in just 10 directions
        - Reduce 100D to 10D
        - Make visualization possible again!
        
        **Key insight**: Not all features are equally important!
        """)


def display_pipeline_overview() -> None:
    """Show the pipeline."""
    st.header("🔄 Level 6 Pipeline")
    
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>1. Load</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>2. Scale</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #E91E63, #C2185B); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>3. PCA</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>4. Train</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>5. Compare</b>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(233,30,99,0.1); border-radius: 10px; 
                border-left: 4px solid #E91E63; margin: 10px 0;">
        <b>🆕 New Step: PCA Transformation!</b><br>
        <span style="font-size: 13px;">
        Before training, we compress our features:<br>
        • Many features → Few principal components<br>
        • Keep most of the variance<br>
        • Reduce noise and overfitting risk
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_why_level6() -> None:
    """Explain motivation for PCA."""
    st.header("🤔 Why Do We Need PCA?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 10px 0;">
        <b>❌ Problems from Level 5:</b><br>
        <span style="font-size: 13px;">
        • Too many features (30+ dimensions)<br>
        • Can't visualize<br>
        • Risk of overfitting<br>
        • Some features might be redundant
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 10px 0;">
        <b>✅ PCA Solution:</b><br>
        <span style="font-size: 13px;">
        • Reduce 30D to 5D or less<br>
        • Can visualize in 2D/3D again!<br>
        • Remove noise<br>
        • Keep the essential information
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_pca_concept() -> None:
    """Explain PCA concept visually."""
    st.header("📐 How PCA Works")
    
    st.markdown("""
    **Simple explanation**: PCA finds new axes that capture the most spread in your data.
    """)
    
    # Create demo data
    np.random.seed(42)
    n_points = 200
    
    # Create correlated data
    x1 = np.random.randn(n_points) * 2
    x2 = x1 * 0.8 + np.random.randn(n_points) * 0.5
    data_2d = np.column_stack([x1, x2])
    
    # Apply PCA
    pca = PCA(n_components=2)
    pca.fit(data_2d)
    
    # Transform
    data_pca = pca.transform(data_2d)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Before PCA")
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.scatter(data_2d[:, 0], data_2d[:, 1], alpha=0.5, s=20)
        
        # Draw original axes
        ax.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        ax.axvline(x=0, color='gray', linestyle='--', alpha=0.5)
        
        # Draw PC directions
        mean = data_2d.mean(axis=0)
        for i, (comp, var) in enumerate(zip(pca.components_, pca.explained_variance_)):
            ax.arrow(mean[0], mean[1], comp[0]*var*2, comp[1]*var*2, 
                    head_width=0.2, head_length=0.1, fc=f'C{i}', ec=f'C{i}', linewidth=2)
        
        ax.set_xlabel('Feature 1')
        ax.set_ylabel('Feature 2')
        ax.set_title('Original Data with PC Directions')
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)
        plt.close()
    
    with col2:
        st.markdown("### After PCA")
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.scatter(data_pca[:, 0], data_pca[:, 1], alpha=0.5, s=20, c='green')
        
        ax.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        ax.axvline(x=0, color='gray', linestyle='--', alpha=0.5)
        
        ax.set_xlabel('PC1 (Most Variance)')
        ax.set_ylabel('PC2 (Second Most)')
        ax.set_title('Data in PC Space')
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)
        plt.close()
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 15px 0;">
        <b>💡 What happened?</b><br>
        <span style="font-size: 13px;">
        • PCA found the direction of maximum spread (PC1)<br>
        • PC2 is perpendicular to PC1<br>
        • Most variance is along PC1 → We could drop PC2 with little loss!
        </span>
    </div>
    """, unsafe_allow_html=True)


def prepare_data(df: pd.DataFrame):
    """Prepare high-dimensional data for PCA demo."""
    df = df.copy()
    np.random.seed(RANDOM_STATE)
    n = len(df)
    
    if 'year' not in df.columns:
        df['year'] = np.random.randint(1985, 2024, n)
    if 'floor' not in df.columns:
        df['floor'] = np.random.randint(1, 30, n)
    
    df['building_age'] = 2024 - df['year']
    df['total_units'] = np.random.randint(100, 2000, n)
    df['parking_ratio'] = np.random.uniform(0.5, 2.0, n)
    
    return df


@st.cache_resource
def train_models(df: pd.DataFrame, n_components: int):
    """Train models with and without PCA."""
    # Prepare features
    numeric_features = ['area_m2', 'year', 'floor', 'building_age', 'total_units', 'parking_ratio']
    
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    X_district = encoder.fit_transform(df[['district']])
    X_numeric = df[numeric_features].values
    X = np.hstack([X_numeric, X_district])
    y = df['price_10k_krw'].values
    
    # Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=RANDOM_STATE
    )
    
    # Model WITHOUT PCA
    model_no_pca = LinearRegression()
    model_no_pca.fit(X_train, y_train)
    rmse_no_pca_train = calculate_rmse(y_train, model_no_pca.predict(X_train))
    rmse_no_pca_test = calculate_rmse(y_test, model_no_pca.predict(X_test))
    
    # Model WITH PCA
    pca = PCA(n_components=n_components)
    X_train_pca = pca.fit_transform(X_train)
    X_test_pca = pca.transform(X_test)
    
    model_pca = LinearRegression()
    model_pca.fit(X_train_pca, y_train)
    rmse_pca_train = calculate_rmse(y_train, model_pca.predict(X_train_pca))
    rmse_pca_test = calculate_rmse(y_test, model_pca.predict(X_test_pca))
    
    return {
        'n_original': X.shape[1],
        'n_components': n_components,
        'pca': pca,
        'no_pca': {'train': rmse_no_pca_train, 'test': rmse_no_pca_test},
        'with_pca': {'train': rmse_pca_train, 'test': rmse_pca_test},
        'explained_variance': pca.explained_variance_ratio_,
        'X_test_pca': X_test_pca,
        'y_test': y_test
    }


def display_explained_variance(results: dict) -> None:
    """Show explained variance plot."""
    st.header("📊 Explained Variance")
    
    st.markdown("""
    **How much information does each component capture?**
    """)
    
    variance = results['explained_variance']
    cumulative = np.cumsum(variance)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(range(1, len(variance)+1), variance, alpha=0.7, label='Individual')
        ax.plot(range(1, len(variance)+1), cumulative, 'ro-', label='Cumulative')
        ax.axhline(y=0.95, color='green', linestyle='--', alpha=0.7, label='95% threshold')
        ax.set_xlabel('Principal Component')
        ax.set_ylabel('Explained Variance Ratio')
        ax.set_title('Scree Plot')
        ax.legend()
        ax.grid(True, alpha=0.3)
        st.pyplot(fig, use_container_width=True)
        plt.close()
    
    with col2:
        # Find components needed for 95%
        n_95 = np.argmax(cumulative >= 0.95) + 1
        
        st.markdown(f"""
        <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                    border-left: 4px solid #4CAF50; margin: 10px 0;">
            <b>📈 Results</b><br><br>
            <span style="font-size: 14px;">
            • Original dimensions: <b>{results['n_original']}</b><br>
            • PC1 captures: <b>{variance[0]*100:.1f}%</b> variance<br>
            • First {n_95} PCs capture: <b>{cumulative[n_95-1]*100:.1f}%</b><br><br>
            <b>We can reduce {results['n_original']}D to {n_95}D!</b>
            </span>
        </div>
        """, unsafe_allow_html=True)
    
    st.info("""
    **💡 How to read the Scree Plot:**
    - Blue bars = variance captured by each PC
    - Red line = cumulative variance
    - Green dashed = 95% threshold
    - **Elbow rule**: Choose the point where adding more PCs gives diminishing returns
    """)


def display_component_slider(df: pd.DataFrame) -> int:
    """Let user select number of components."""
    st.header("🎚️ Choose Number of Components")
    
    st.markdown("""
    **Experiment**: How many components do we need?
    """)
    
    n_components = st.slider(
        "Number of Principal Components",
        min_value=2,
        max_value=20,
        value=5,
        help="More components = more information but higher dimensionality"
    )
    
    return n_components


def display_comparison(results: dict) -> None:
    """Compare models with and without PCA."""
    st.header("⚖️ Before vs After PCA")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Without PCA")
        st.metric("Dimensions", results['n_original'])
        st.metric("Train RMSE", f"{results['no_pca']['train']:,.0f}")
        st.metric("Test RMSE", f"{results['no_pca']['test']:,.0f}")
    
    with col2:
        st.markdown("### With PCA")
        reduction = (1 - results['n_components']/results['n_original']) * 100
        st.metric("Dimensions", results['n_components'], delta=f"-{reduction:.0f}%")
        
        train_diff = results['with_pca']['train'] - results['no_pca']['train']
        test_diff = results['with_pca']['test'] - results['no_pca']['test']
        
        st.metric("Train RMSE", f"{results['with_pca']['train']:,.0f}", 
                  delta=f"{train_diff:+,.0f}")
        st.metric("Test RMSE", f"{results['with_pca']['test']:,.0f}", 
                  delta=f"{test_diff:+,.0f}")
    
    # Summary
    if abs(test_diff) < results['no_pca']['test'] * 0.05:
        st.success(f"""
        ✅ **Great!** We reduced {results['n_original']}D to {results['n_components']}D 
        with minimal loss in accuracy!
        """)
    elif test_diff > 0:
        st.warning(f"""
        ⚠️ Performance decreased slightly. Try more components or check your data.
        """)
    else:
        st.success(f"""
        🎉 **Excellent!** PCA actually improved performance! This suggests the original
        data had redundant or noisy features.
        """)


def display_pca_visualization(results: dict) -> None:
    """Visualize data in PC space."""
    st.header("🎨 Data in PC Space")
    
    st.markdown("""
    **Now we can visualize high-dimensional data in 2D!**
    """)
    
    if results['n_components'] >= 2:
        X_pca = results['X_test_pca']
        y = results['y_test']
        
        fig, ax = plt.subplots(figsize=(10, 6))
        scatter = ax.scatter(X_pca[:, 0], X_pca[:, 1], c=y, cmap='viridis', 
                            alpha=0.5, s=20)
        plt.colorbar(scatter, label='Price')
        ax.set_xlabel('PC1')
        ax.set_ylabel('PC2')
        ax.set_title('Data Points in First 2 Principal Components')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig, use_container_width=True)
        plt.close()
        
        st.info("""
        **💡 What are we seeing?**
        - Each point is an apartment
        - X = PC1 (most important direction)
        - Y = PC2 (second most important)
        - Color = Price
        
        Even though we had 30+ features, we can see patterns in just 2D!
        """)


def display_pca_code() -> None:
    """Show PCA implementation code."""
    st.header("📝 PCA Code")
    
    st.code("""
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

# Step 1: Scale features (IMPORTANT for PCA!)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Step 2: Apply PCA
# Option A: Specify number of components
pca = PCA(n_components=5)

# Option B: Keep 95% of variance
pca = PCA(n_components=0.95)

# Step 3: Transform data
X_pca = pca.fit_transform(X_scaled)

# Check results
print(f"Original shape: {X.shape}")
print(f"PCA shape: {X_pca.shape}")
print(f"Variance explained: {sum(pca.explained_variance_ratio_):.2%}")
""", language='python')


def display_limitations() -> None:
    """Show limitations of PCA."""
    st.header("🤔 Limitations of PCA")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>⚠️ PCA assumes linear relationships!</b><br>
        <span style="font-size: 13px;">
        PCA finds linear combinations of features.<br>
        If relationships are non-linear, PCA might miss important patterns.
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>⚠️ PCA doesn't fix bad data!</b><br>
        <span style="font-size: 13px;">
        Garbage in, garbage out.<br>
        If your data has nulls, outliers, or errors, PCA won't help.
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>⚠️ PCA components are hard to interpret!</b><br>
        <span style="font-size: 13px;">
        "PC1" is a mix of all features.<br>
        Unlike "area" or "price", PC1 has no intuitive meaning.
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
    ### 🚀 What's Next?
    
    | Level 6 (Now) | Level 7 (Next) |
    |---------------|----------------|
    | Dimensionality reduction | Data cleaning |
    | PCA transformation | Handle nulls & outliers |
    | Assumes clean data | Fix data quality issues |
    
    **Next**: Let's go back to basics and clean our data properly!
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
        display_why_level6()
        st.markdown("---")
        display_pca_concept()
        st.markdown("---")
        
        n_components = display_component_slider(df)
        
        st.markdown("---")
        
        with st.spinner("Training models..."):
            results = train_models(df, n_components)
        
        display_explained_variance(results)
        st.markdown("---")
        display_comparison(results)
        st.markdown("---")
        display_pca_visualization(results)
        st.markdown("---")
        display_pca_code()
        st.markdown("---")
        display_limitations()
        
        # Next level teaser
        display_next_level_teaser(6)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
