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
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison

# Korean to English district name mapping
DISTRICT_NAME_MAP = {
    '강남구': 'Gangnam', '서초구': 'Seocho', '송파구': 'Songpa', '용산구': 'Yongsan',
    '성동구': 'Seongdong', '광진구': 'Gwangjin', '마포구': 'Mapo', '양천구': 'Yangcheon',
    '영등포구': 'Yeongdeungpo', '동작구': 'Dongjak', '종로구': 'Jongno', '중구': 'Jung',
    '서대문구': 'Seodaemun', '동대문구': 'Dongdaemun', '성북구': 'Seongbuk', '강동구': 'Gangdong',
    '강서구': 'Gangseo', '구로구': 'Guro', '금천구': 'Geumcheon', '관악구': 'Gwanak',
    '은평구': 'Eunpyeong', '노원구': 'Nowon', '도봉구': 'Dobong', '강북구': 'Gangbuk', 
    '중랑구': 'Jungnang'
}


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
    
    # Note: 'year' and 'floor' are now in the real dataset!
    # We no longer need to generate them randomly.
    
    # Create additional features
    # Create additional features
    if 'built_year' in df.columns:
        df['building_age'] = 2024 - df['built_year']
    else:
        # Fallback if built_year missing (shouldn't happen with sample.parquet)
        df['building_age'] = 2024 - df['year']
        
    # Generate synthetic features if missing (for Level 5 demo purposes)
    np.random.seed(42)
    if 'total_units' not in df.columns:
        # Random correlation with area (larger complexes tend to have various sizes, but let's just make it random-ish)
        df['total_units'] = np.random.randint(100, 3000, size=len(df))
        
    if 'parking_spaces' not in df.columns:
        # Parking usually correlates with units (approx 1.2 per unit)
        df['parking_spaces'] = (df['total_units'] * np.random.uniform(0.8, 1.5, size=len(df))).astype(int)
    
    # Notebook uses: Area, Floor, Building Age, Total Units, Parking Spaces
    return df


def display_feature_selection(df: pd.DataFrame) -> list:
    """Let user select features."""
    st.header("🎯 Feature Selection")
    
    st.markdown("""
    **Look at all these features we can use!**
    
    In Level 2, we only used Area. In Level 3, we added District. In Level 4, Building Year.
    Now we have MANY more options! 🎉
    """)
    
    # Show all available features prominently
    available_features = {
        'area_m2': ('📐', 'Area (m²)', 'Apartment size - the most basic feature'),
        'floor': ('🏢', 'Floor Number', 'Which floor? Penthouses cost more!'),
        'building_age': ('⏰', 'Building Age', 'Years since construction (2024 - year)'),
        'total_units': ('🏘️', 'Total Units', 'How many apartments in the complex?'),
        'parking_spaces': ('🚗', 'Parking Spaces', 'Total parking spots available')
    }
    
    # Display feature cards
    st.markdown("### 📋 Available Features (3 numeric + 25 districts)")
    
    cols = st.columns(4)
    for i, (key, (emoji, name, desc)) in enumerate(available_features.items()):
        with cols[i % 4]:
            st.markdown(f"""
            <div style="padding: 10px; background: rgba(33,150,243,0.1); border-radius: 8px; 
                        margin: 5px 0; text-align: center; min-height: 80px;">
                <div style="font-size: 20px;">{emoji}</div>
                <div style="font-weight: bold; font-size: 11px;">{name}</div>
                <div style="font-size: 9px; color: gray;">{desc[:30]}...</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Default: select ALL features to show the "high dimensional" concept
    default_features = list(available_features.keys())  # All features!
    
    selected = st.multiselect(
        "🔧 Select features to include",
        options=list(available_features.keys()),
        default=['area_m2', 'floor', 'building_age', 'total_units', 'parking_spaces'],
        format_func=lambda x: f"{available_features[x][0]} {x}"
    )
    
    include_district = st.checkbox("✅ Include District (adds 25 One-Hot columns)", value=True)
    
    # Visual dimension count
    total_dims = len(selected) + (25 if include_district else 0)
    
    st.markdown(f"""
    <div style="padding: 20px; background: linear-gradient(135deg, rgba(156,39,176,0.1), rgba(233,30,99,0.1)); 
                border-radius: 10px; border: 2px solid #9C27B0; margin: 15px 0; text-align: center;">
        <div style="font-size: 40px; font-weight: bold; color: #9C27B0;">{total_dims}</div>
        <div style="font-size: 14px;">Total Dimensions!</div>
        <div style="font-size: 12px; color: gray; margin-top: 5px;">
            {len(selected)} numeric + {25 if include_district else 0} district columns
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    if total_dims >= 10:
        st.warning("🌌 **This is HIGH-DIMENSIONAL data!** Can't visualize this in a single plot!")
    
    return selected, include_district


def display_code_logic_explanation(selected_features: list) -> None:
    """Explain how we handle multiple features in code."""
    st.header("💻 Code Reality Check: Handling Multiple Features")
    
    st.markdown("""
    Before we visualize the relationships, let's look at how we actually write this in Python.
    It's surprisingly simple!
    """)
    
    st.markdown("### 1. Selecting Multiple Features")
    st.markdown("Instead of just selecting `'area_m2'`, we pass a **list** of column names.")
    
    # Format simple list for display
    display_list = str(selected_features[:3]).replace("]", ", ...]") if len(selected_features) > 3 else str(selected_features)
    
    code_snippet = f"""
# 1. Define the list of features we want
features = {display_list}

# 2. Select these columns from our DataFrame
# This creates a new table with ONLY these columns
subset_df = df[features]
"""
    st.code(code_snippet, language='python')
    
    st.markdown("### 2. Calculating Correlation")
    st.markdown("To get the heatmap numbers, we simply ask pandas to calculate the correlation matrix.")
    
    st.code("""
# Calculate correlation between all these columns
# corr() calculates the relationship (-1 to 1) for EVERY pair
corr_matrix = subset_df.corr()

# This gives us a square table of numbers:
#          Area    Year   Price
# Area     1.00    0.35    0.65
# Year     0.35    1.00    0.20
# Price    0.65    0.20    1.00
""", language='python')

    st.info("The **Heatmap** below is simply a colorful way to visualize this table of numbers!")


def display_correlation_heatmap(df: pd.DataFrame, features: list) -> None:
    """Show correlation between features."""
    st.header("📊 Feature Correlation Heatmap")
    
    # How to read explanation
    with st.expander("📖 How to Read This Heatmap", expanded=True):
        st.markdown("""
        **Correlation** measures how two variables move together (-1 to +1):
        
        | Value | Meaning | Color |
        |-------|---------|-------|
        | **+1.0** | Perfect positive (↑ together) | 🔴 Dark Red |
        | **+0.5** | Moderate positive | 🟠 Orange |
        | **0.0** | No relationship | ⚪ White |
        | **-0.5** | Moderate negative (↑↓ opposite) | 🔵 Light Blue |
        | **-1.0** | Perfect negative | 🔵 Dark Blue |
        
        **What to look for:**
        - **Last row/column (price)**: Which features correlate with price?
        - **Between features**: High correlation = redundant information!
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
        ax=ax,
        vmin=-1, vmax=1
    )
    ax.set_title('Feature Correlation Matrix')
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # My analysis section
    st.markdown("### 🔍 My Analysis of This Heatmap")
    
    price_corr = corr_matrix['price_10k_krw'].drop('price_10k_krw').sort_values(key=abs, ascending=False)
    
    # Analyze correlations
    st.markdown("""
    <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 10px 0;">
        <b>🎯 Key Findings:</b>
    </div>
    """, unsafe_allow_html=True)
    
    # Finding 1: Best predictor
    best_feat = price_corr.index[0]
    best_corr = price_corr.iloc[0]
    st.markdown(f"""
    **1. Best Price Predictor: `{best_feat}` (correlation: {best_corr:.3f})**
    - {'Strong positive!' if best_corr > 0.5 else 'Moderate' if best_corr > 0.3 else 'Weak'} correlation with price
    - This feature should be in our model!
    """)
    
    # Finding 2: Check for multicollinearity
    feature_corrs = corr_matrix.drop('price_10k_krw', axis=0).drop('price_10k_krw', axis=1)
    high_corr_pairs = []
    for i, feat1 in enumerate(feature_corrs.columns):
        for feat2 in feature_corrs.columns[i+1:]:
            corr_val = feature_corrs.loc[feat1, feat2]
            if abs(corr_val) > 0.7:
                high_corr_pairs.append((feat1, feat2, corr_val))
    
    if high_corr_pairs:
        st.markdown(f"""
        **2. ⚠️ Highly Correlated Features Found!**
        """)
        for f1, f2, corr in high_corr_pairs[:3]:
            st.markdown(f"- `{f1}` ↔ `{f2}`: {corr:.3f} (might be redundant!)")
    else:
        st.markdown("**2. ✅ No highly correlated features** - all features provide unique info!")
    
    # Finding 3: Weak predictors
    weak_feats = price_corr[abs(price_corr) < 0.1]
    if len(weak_feats) > 0:
        st.markdown(f"""
        **3. 🤔 Weak Predictors (correlation < 0.1):**
        - {', '.join([f'`{f}`' for f in weak_feats.index[:3]])}
        - These might not help much - consider removing them!
        """)
    
    st.markdown("---")
    
    # Summary table
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🏆 Top 3 Price Predictors:**")
        for i, (feat, corr) in enumerate(price_corr.head(3).items(), 1):
            color = "#4CAF50" if corr > 0 else "#F44336"
            st.markdown(f"{i}. `{feat}`: <span style='color:{color};font-weight:bold'>{corr:.3f}</span>", 
                       unsafe_allow_html=True)
    
    with col2:
        st.markdown("**📉 Weakest Predictors:**")
        for feat, corr in price_corr.tail(3).items():
            st.markdown(f"- `{feat}`: {corr:.3f}")


def display_training_code_explanation(include_district: bool) -> None:
    """Explain the training code involving One-Hot Encoding."""
    st.header("💻 Code Reality Check: Preprocessing & Training")
    
    st.markdown("""
    Now that we've analyzed the features, how do we feed them into the model?
    Particularly **District**, which is text data!
    """)
    
    if include_district:
        st.markdown("### 1. Handling Text Data (One-Hot Encoding)")
        st.markdown("""
        The notebook uses `pd.get_dummies()` to convert **'Gangnam'**, **'Mapo'** into numbers.
        
        **Why?**
        - Computers only understand numbers.
        - We can't just say `Gangnam = 1, Mapo = 2` because that implies Mapo is "twice" Gangnam (math error!).
        - Instead, we create a **Switch (0 or 1)** for every district.
        """)
        
        st.code("""
# One-Hot Encoding in the Notebook
# This creates 25 new columns like 'dist_Gangnam', 'dist_Mapo'
ct = pd.get_dummies(df['district'], prefix='dist')

# Combine numeric features with these new district columns
# hstack = Horizontal Stack (putting columns side-by-side)
X = np.hstack([df[numeric_features].values, ct.values])
""", language='python')
    else:
        st.markdown("### 1. Selecting Numeric Features")
        st.code("""
# Simply select numeric columns
X = df[numeric_features].values
""", language='python')

    st.markdown("### 2. Training the Model")
    st.markdown("The training process is standard sklearn workflow:")
    
    st.code("""
# 1. Split into Train/Test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# 2. Create and Train Model
model = LinearRegression()
model.fit(X_train, y_train)  # The math happens here!

# 3. Predict & Evaluate
y_pred = model.predict(X_test)
rmse = mean_squared_error(y_test, y_pred, squared=False)
""", language='python')


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
    
    # Split (Match Notebook's random_state=42)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Train
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    rmse_train = calculate_rmse(y_train, y_pred_train)
    rmse_test = calculate_rmse(y_test, y_pred_test)
    
    return model, None, encoder, feature_names, rmse_train, rmse_test, (y_test, y_pred_test)


def display_feature_importance(model, feature_names: list, n_numeric: int) -> None:
    """Show feature importance from coefficients."""
    st.header("📊 Feature Importance")
    
    st.markdown("""
    **Which features matter most?**
    
    Larger absolute coefficient = More impact on price
    """)
    
    # Get coefficients
    coefs = model.coef_
    
    # Convert Korean district names to English
    english_names = []
    for name in feature_names:
        if name in DISTRICT_NAME_MAP:
            english_names.append(DISTRICT_NAME_MAP[name])
        else:
            english_names.append(name)
    
    # Create importance dataframe
    importance_df = pd.DataFrame({
        'Feature': english_names,
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
        • <span style="color:#4CAF50">Green bars</span>: Increase price (positive coefficient)<br>
        • <span style="color:#F44336">Red bars</span>: Decrease price (negative coefficient)<br>
        • Longer bar = Stronger effect on price<br><br>
        <b>Note:</b> District names are shown in English (e.g., Gangnam, Seocho)
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Show top 3 insights
    st.markdown("### 🔍 Key Insights")
    
    top_positive = importance_df[importance_df['Coefficient'] > 0].head(3)
    top_negative = importance_df[importance_df['Coefficient'] < 0].head(3)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🟢 Price Boosters:**")
        for _, row in top_positive.iterrows():
            st.markdown(f"- **{row['Feature']}**: +{row['Coefficient']:,.0f}")
    
    with col2:
        st.markdown("**🔴 Price Reducers:**")
        for _, row in top_negative.iterrows():
            st.markdown(f"- **{row['Feature']}**: {row['Coefficient']:,.0f}")


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
    
    if n_features > 20: 
        st.markdown("### 🏆 Comparison with Notebook")
        
        # Determine performance relative to notebook
        notebook_rmse = 32666
        diff = notebook_rmse - rmse_test
        
        if diff > 1000:
            status = "🎉 **Better than Notebook!**"
        elif diff < -1000:
            status = "⚠️ **Worse than Notebook**"
        else:
            status = "✅ **Matches Notebook**"
            
        msg = f"""
        We aim to match or beat the result from the Jupyter Notebook (~32,666).
        
        | Model | RMSE (Test Error) | Status |
        |-------|-------------------|--------|
        | **Notebook Level 5** | ~32,666 | Benchmark Target |
        | **Your Model** | **{rmse_test:,.0f}** | {status} |
        """
        st.markdown(msg)
        
        if abs(diff) <= 1000:
            st.info("Perfect! You have successfully reproduced the notebook's results.")
        elif diff > 1000:
            st.success("Great job! You found a combination of features that beats the baseline.")
    
    # Compare with previous levels
    st.markdown("---")
    display_rmse_comparison(5, rmse_test)


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


def display_curse_of_dimensionality(df: pd.DataFrame) -> None:
    """Demonstrate the curse of dimensionality with interactive noise injection."""
    st.header("👻 The Curse of Dimensionality Simulator")
    
    st.markdown("""
    **What happens if we add garbage data (Noise)?**
    
    A smarter model should ignore it, right? 
    In High Dimensions, simple Linear Regression gets **confused** and tries to find patterns in the noise!
    """)
    
    st.markdown("### 🧪 Experiment: Injecting Random Noise")
    
    # User interaction
    n_noise = st.slider("Number of Random Noise Features to Add", 0, 100, 0, step=10)
    
    # Prepare base data (using just Area to keep it simple at first)
    X_base = df[['area_m2']].values
    y = df['price_10k_krw'].values
    
    # Add noise
    np.random.seed(42)
    if n_noise > 0:
        noise = np.random.rand(len(df), n_noise)
        X = np.hstack([X_base, noise])
    else:
        X = X_base

    # Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Train
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Evaluate
    train_rmse = calculate_rmse(y_train, model.predict(X_train))
    test_rmse = calculate_rmse(y_test, model.predict(X_test))
    
    # Display metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Noise Features", n_noise)
    c2.metric("Train RMSE (Lower is better)", f"{train_rmse:,.0f}", delta=f"{-train_rmse/1000:.1f}k", delta_color="inverse")
    c3.metric("Test RMSE (REAL accuracy)", f"{test_rmse:,.0f}", delta=f"{test_rmse/1000:.1f}k", delta_color="inverse")
    
    st.info("""
    **Observe:** As you add more noise...
    1. **Train RMSE** goes DOWN (Model memorizes the noise! "I explained everything!")
    2. **Test RMSE** goes UP (Model fails on new data! "I learned false patterns!")
    """)
    
    # Visualization of the Gap
    # We pre-calculate range for the chart
    results = []
    noise_range = [0, 10, 30, 50, 80, 100, 150, 200]
    
    for n in noise_range:
        noise_f = np.random.rand(len(df), n) if n > 0 else np.zeros((len(df), 0))
        X_curr = np.hstack([X_base, noise_f]) if n > 0 else X_base
        Xt, Xv, yt, yv = train_test_split(X_curr, y, test_size=0.2, random_state=42)
        m = LinearRegression()
        m.fit(Xt, yt)
        results.append({
            'Noise': n,
            'Train RMSE': calculate_rmse(yt, m.predict(Xt)),
            'Test RMSE': calculate_rmse(yv, m.predict(Xv))
        })
        
    res_df = pd.DataFrame(results)
    
    # Highlight current selection
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(res_df['Noise'], res_df['Train RMSE'], 'o-', label='Train Error (Memorization)', color='#2196F3')
    ax.plot(res_df['Noise'], res_df['Test RMSE'], 'o-', label='Test Error (Real Performance)', color='#F44336')
    
    # Add current point
    ax.axvline(x=n_noise, color='black', linestyle='--', alpha=0.5)
    
    ax.set_xlabel('Number of Noise Features')
    ax.set_ylabel('RMSE (Error)')
    ax.set_title('Overfitting: The Gap Widens!')
    ax.legend()
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)


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
        
        display_code_logic_explanation(selected_features)
        
        st.markdown("---")
        display_correlation_heatmap(df, selected_features)
        st.markdown("---")
        
        display_training_code_explanation(include_district)
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
        display_curse_of_dimensionality(df)
        st.markdown("---")
        display_limitations()
        
        # Code Link
        
        display_code_link("Level_5_High_Dimensional.ipynb")
        
        
        
        # Next level teaser
        display_next_level_teaser(5)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
