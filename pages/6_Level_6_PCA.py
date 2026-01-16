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
import plotly.express as px
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison


def display_header() -> None:
    """Display Level 6 introduction."""
    st.title("🔬 Level 6: PCA (Dimensionality Reduction)")
    
    st.markdown("""
    ### 🤷‍♀️ Wait, what is PCA?
    
    **PCA (Principal Component Analysis)** is a fancy name for a simple concept:
    **"Simplifying data without losing the important details."**
    
    > **The "Backpack" Analogy** 🎒
    > Imagine you are packing for a trip. You have 30 items (shirts, socks, toothbrush...).
    > You can't carry 30 loose items. So you pack them into **3 bags**.
    > * You still have your stuff (mostly).
    > * But now you only have to carry 3 objects instead of 30!
    
    In Data Science:
    * **Items** = Original Features (Area, Year, Mapo-gu, Gangnam-gu...)
    * **Bags** = Principal Components (Super-Features)
    """)

def display_pipeline_overview() -> None:
    """Show the pipeline with visual flow."""
    st.header("🔄 The Process")
    
    st.markdown("### From Chaos to Simplicity")
    
    st.graphviz_chart("""
    digraph PCA {
        rankdir=LR;
        node [shape=box, style=filled, fillcolor="#E3F2FD", fontname="Arial"];
        
        A [label="Input:\n30 Features", fillcolor="#FFEBEE"];
        B [label="PCA Machine\n(Squeeze & Rotate)", fillcolor="#E1F5FE", shape=ellipse];
        C [label="Output:\n3 Principal Components", fillcolor="#E8F5E9"];
        D [label="Training Model\n(Linear Regression)", fillcolor="#FFF3E0"];
        
        A -> B [label="Too Complex"];
        B -> C [label="Simplified"];
        C -> D [label="Train"];
        
        {rank=same; A B C D}
    }
    """)
    st.caption("Flow: We take many columns, squeeze them into a few 'Principal Components', and use those to train.")

def display_why_level6() -> None:
    """Explain motivation for PCA connecting to Level 5."""
    st.header("🤔 Why do we need this?")
    
    st.info("""
    **Recall Level 5:** We had **30+ features** because we added "District" (One-Hot Encoding).
    - `dist_Gangnam`, `dist_Mapo`, `dist_Yongsan`...
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 15px; background: #ffebee; border-radius: 10px;">
            <b>❌ The Problem (High Dimensions)</b><br>
            • Hard to visualize (Can you draw a 30D plot?)<br>
            • Computationally slow<br>
            • "Curse of Dimensionality" (Data gets sparse)
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown("""
        <div style="padding: 15px; background: #e8f5e9; border-radius: 10px;">
            <b>✅ The Solution (PCA)</b><br>
            • Crush 30 columns into ~5 "Super Columns"<br>
            • Keep 95% of the information<br>
            • Remove noise and redundancy
        </div>
        """, unsafe_allow_html=True)

def display_pca_concept() -> None:
    """Explain PCA concept visually with accessible terms."""
    st.header("📐 How does it work? (The Camera Analogy)")
    
    st.markdown("""
    Imagine a **3D Teapot**. Using PCA is like finding the **best angle to take a photo** of it.
    
    1.  **Top View**: You just see a circle (Loop lid). **Bad photo** (Low Variance). ❌
    2.  **Side View**: You see the handle, spout, and body. **Great photo** (High Variance). ✅
    
    PCA mathematically rotates your data to find that "Side View" where the data looks most spread out.
    """)
    
    st.markdown("### 🧪 Interactive Demo: Finding the 'Best Axis'")
    
    st.markdown("Here is some 2D data (blue dots). We want to compress it to 1D (a line).")
    
    # Create demo data
    np.random.seed(42)
    n_points = 200
    x1 = np.random.randn(n_points) * 2
    x2 = x1 * 0.8 + np.random.randn(n_points) * 0.5
    data_2d = np.column_stack([x1, x2])
    
    # Apply PCA
    pca = PCA(n_components=2)
    pca.fit(data_2d)
    data_pca = pca.transform(data_2d)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. Original Data")
        fig, ax = plt.subplots(figsize=(5, 5))
        ax.scatter(data_2d[:, 0], data_2d[:, 1], alpha=0.4, s=15, label='Data')
        
        # Draw vectors
        mean = data_2d.mean(axis=0)
        
        # PC1
        v1 = pca.components_[0] * pca.explained_variance_[0]
        ax.arrow(mean[0], mean[1], v1[0], v1[1], width=0.1, color='red', label='PC1 (Best Axis)')
        
        # PC2
        v2 = pca.components_[1] * pca.explained_variance_[1]
        ax.arrow(mean[0], mean[1], v2[0], v2[1], width=0.1, color='green', label='PC2 (2nd Best)')
        
        ax.legend()
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)
        
        st.markdown("""
        **PC1 (Red Arrow)**: The direction where data is most spread out.
        **PC2 (Green Arrow)**: The next best direction (always 90° to PC1).
        """)

    with col2:
        st.subheader("2. After Rotating (PC Space)")
        fig, ax = plt.subplots(figsize=(5, 5))
        
        # Visualize projection
        ax.scatter(data_pca[:, 0], data_pca[:, 1], alpha=0.4, s=15, c='purple')
        ax.axhline(0, color='red', linestyle='--', alpha=0.5, label='PC1 Axis')
        ax.axvline(0, color='green', linestyle='--', alpha=0.5, label='PC2 Axis')
        
        ax.set_xlabel("PC1 (Value of Red Arrow)")
        ax.set_ylabel("PC2 (Value of Green Arrow)")
        ax.legend()
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)
        
        st.markdown("""
        We rotated the graph! Now **PC1** is the flat horizontal axis.
        Notice how "wide" the data is left-to-right? That's **Variance**!
        """)


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
    """Show explained variance with simplified Packing List analogy."""
    st.header("📊 How Efficient is Packing?")
    
    st.markdown("""
    **Let's check what's inside our bags.**
    
    Remember, we compressed 30+ features into these components.
    Here is a peek at how much "Information" (Variance) each bag holds:
    """)
    
    variance = results['explained_variance']
    cumulative = np.cumsum(variance)
    n_95 = np.argmax(cumulative >= 0.95) + 1
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("🎒 The Packing List")
        
        # Create a DataFrame for visual display
        packing_data = []
        for i, var in enumerate(variance):
            if i >= 5: break # Only show top 5
            
            # Simple heuristic descriptions (static for demo)
            desc = "Mix of everything"
            if i == 0: desc = "Big Stuff (Area, Price-like info)"
            elif i == 1: desc = "Medium Stuff (Year, Floor...)"
            elif i == 2: desc = "Small Details"
            else: desc = "Minor details"
            
            packing_data.append({
                "Bag": f"Bag #{i+1}",
                "Information": var, # 0.0 to 1.0
                "Description": desc
            })
            
        packing_df = pd.DataFrame(packing_data)
        
        st.dataframe(
            packing_df,
            hide_index=True,
            column_config={
                "Bag": st.column_config.TextColumn("Bag"),
                "Information": st.column_config.ProgressColumn(
                    "Information Content",
                    format="%.1f%%",
                    min_value=0,
                    max_value=max(variance)*1.2, # Scale nicely
                ),
                "Description": st.column_config.TextColumn("Content Hint"),
            },
            use_container_width=True
        )
        
        st.caption("And many small bags with <1% info (Noise)...")
    
    with col2:
        st.subheader("📈 Total Info Kept")
        st.markdown("If we keep **N bags**, how much total info do we have?")
        
        # Simple Line Chart for Cumulative Variance
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(range(1, len(variance)+1), cumulative * 100, 'o-', color='#E91E63', linewidth=2)
        ax.axhline(y=95, color='green', linestyle='--', alpha=0.7, label='95% Goal')
        
        # Highlight the user's current selection
        current_n = results['n_components']
        current_var = cumulative[current_n-1] * 100
        ax.plot(current_n, current_var, 'o', color='blue', markersize=12, label='Current Choice')
        
        ax.set_xlabel('Number of Bags (Components)')
        ax.set_ylabel('Total Information (%)')
        ax.set_ylim(0, 105)
        ax.grid(True, alpha=0.3)
        ax.legend()
        st.pyplot(fig, use_container_width=True)
        plt.close()
        
        st.info(f"""
        **Current Status:**
        With **{current_n} bags**, you keep **{current_var:.1f}%** of the original information!
        """)
    
    st.info("""
    **💡 The "Diminishing Returns" Rule:**
    - Notice how the blue dots get closer together?
    - **Bag #1** adds a huge amount of info.
    - **Bag #20** helps very little.
    - **Strategy**: Stop adding bags when they stop giving you "enough" new information!
    """)


def select_pca_mode(df: pd.DataFrame) -> int:
    """Let user select PCA mode using presets."""
    st.header("🎚️ How many 'Bags' (Components)?")
    
    st.markdown("""
    Instead of guessing, choose a strategy:
    """)
    
    mode = st.radio(
        "Select Compression Strategy:",
        ["🚀 Extreme Compression (2 Bags)", "🧠 Smart Compression (Keep 95% Info)"],
        index=1,
        help="Extreme = Good for visual, Smart = Good for model"
    )
    
    if "Extreme" in mode:
        return 2
    else:
        # Calculate components needed for 95% variance
        # We need a quick PCA fit to find this number
        # Note: This is a bit inefficient to run twice (here + training), 
        # but for this dataset size it's negligible and provides better UX.
        numeric_features = ['area_m2', 'year', 'floor', 'building_age', 'total_units', 'parking_ratio']
        encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
        X_district = encoder.fit_transform(df[['district']])
        X_numeric = df[numeric_features].fillna(0).values
        X = np.hstack([X_numeric, X_district])
        
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        pca = PCA()
        pca.fit(X_scaled)
        cumulative = np.cumsum(pca.explained_variance_ratio_)
        n_95 = np.argmax(cumulative >= 0.95) + 1
        
        st.caption(f"🤖 Smart Mode: We calculated that **{n_95} bags** are needed to keep 95% of your data.")
        return int(n_95)


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
    
    # Compare with other levels
    st.markdown("---")
    display_rmse_comparison(6, results['with_pca']['test'])


def display_pca_visualization(results: dict, df: pd.DataFrame) -> None:
    """Visualize data in PC space using Plotly."""
    st.header("🎨 Data in PC Space")
    
    st.markdown("""
    **Now we can visualize high-dimensional data in 2D!**
    Hover over the points to see the *original* data hidden inside the Principal Components.
    """)
    
    if results['n_components'] >= 2:
        X_pca = results['X_test_pca']
        y = results['y_test']
        
        # We need to map back to original data indices to get hover info
        # But train_test_split shuffles data.
        # For simplicity in visualization, let's just re-run PCA on a sample subset 
        # where we keep the index alignment
        
        sample = df.sample(min(1000, len(df)), random_state=RANDOM_STATE)
        
        # Preprocessing (same as before)
        numeric_features = ['area_m2', 'year', 'floor', 'building_age', 'total_units', 'parking_ratio']
        encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
        X_district = encoder.fit_transform(sample[['district']])
        X_numeric = sample[numeric_features].values
        X = np.hstack([X_numeric, X_district])
        
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        pca = PCA(n_components=2)
        X_pca_2d = pca.fit_transform(X_scaled)
        
        # Create plotting dataframe
        plot_df = sample.copy()
        plot_df['PC1'] = X_pca_2d[:, 0]
        plot_df['PC2'] = X_pca_2d[:, 1]
        plot_df['Price'] = plot_df['price_10k_krw']
        
        fig = px.scatter(
            plot_df, 
            x='PC1', 
            y='PC2', 
            color='Price',
            hover_data=['area_m2', 'year', 'district'],
            title='Interactive PCA Projection (PC1 vs PC2)',
            labels={'area_m2': 'Area', 'year': 'Year', 'district': 'District'},
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        st.info("""
        **💡 Explore the Pattern:**
        - **Right side (High PC1)**: usually larger Area (check hover!) - PC1 often captures Size.
        - **Top/Bottom (PC2)**: might capture Age or Location.
        - **Color (Price)**: Notice how expensive apartments (Yellow) cluster together?
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
    st.header("🤔 Limitations: It's not magic")
    
    st.markdown("""
    PCA is powerful, but it's not perfect. Here is why:
    """)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; height: 200px;">
            <b>1. It's Linear 📏</b><br><br>
            <span style="font-size: 13px;">
            PCA only looks for straight lines (or flat planes). 
            If your data looks like a spiral or a banana, PCA fails to capture it efficiently.
            </span>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown("""
        <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; height: 200px;">
            <b>2. "Black Box" 📦</b><br><br>
            <span style="font-size: 13px;">
            What is "PC1"?<br>
            It's 0.5*Area + 0.3*Year - 0.2*District...<br>
            It loses the clear meaning of "Square Meters".
            </span>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; height: 200px;">
            <b>3. Sensitive to Trash 🗑️</b><br><br>
            <span style="font-size: 13px;">
            If your data has "outliers" (crazy values), PCA will try to account for them, messing up the rotation.
            Garbage In = Garbage Out.
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
        
        n_components = select_pca_mode(df)
        
        st.markdown("---")
        
        with st.spinner("Training models..."):
            results = train_models(df, n_components)
        
        display_explained_variance(results)
        st.markdown("---")
        display_comparison(results)
        st.markdown("---")
        display_pca_visualization(results, df)
        st.markdown("---")
        display_pca_code()
        st.markdown("---")
        display_limitations()
        
        # Code Link
        
        display_code_link("Level_6_PCA.ipynb")
        
        
        
        # Next level teaser
        display_next_level_teaser(6)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
