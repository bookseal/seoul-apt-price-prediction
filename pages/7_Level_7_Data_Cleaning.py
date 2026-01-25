# -*- coding: utf-8 -*-
"""
Level 7: Data Cleaning (Null Values and Outliers)

Learn about data quality and preprocessing techniques.
"Garbage in, garbage out" - clean data is essential!
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison


def display_header() -> None:
    """Display Level 7 introduction."""
    st.title("🧹 Level 7: Data Cleaning")
    
    st.success("""
    **Goal**: Learn to handle missing values and outliers.
    
    "Garbage in, garbage out" - even the best model fails with bad data!
    """)
    
    with st.expander("💡 Why is Data Cleaning Important?"):
        st.markdown("""
        **Real-world data is messy!**
        
        Common problems:
        - **Missing values (NaN/Null)**: Sensors fail, users skip fields
        - **Outliers**: Typos, measurement errors, exceptional cases
        - **Inconsistent formats**: "Seoul" vs "서울" vs "SEOUL"
        - **Duplicates**: Same record entered twice
        
        **Impact**: These issues can completely mislead your model!
        """)

def display_toc() -> None:
    """Display Table of Contents using Streamlit markdown."""
    st.markdown("""
    ### 📑 Table of Contents
    1.  [**Step 1: Pipeline Overview**](#step-1-pipeline-overview)
    2.  [**Step 2-1: Detecting Missing Values**](#step-2-1-detecting-missing-values)
    3.  [**Step 2-2: Detecting Outliers**](#step-2-2-detecting-outliers)
    4.  [**Step 3-1: How to Handle Missing Values?**](#step-3-1-how-to-handle-missing-values)
    5.  [**Step 3-2: How to Handle Outliers?**](#step-3-2-how-to-handle-outliers)
    6.  [**Step 4: Interactive Simulation**](#step-4-interactive-simulation)
    7.  [**Step 5: Before vs After**](#step-5-before-vs-after)
    8.  [**Step 6: Model Performance Check**](#step-6-model-performance-check)
    """)


def display_pipeline_overview() -> None:
    """Show the pipeline."""
    st.header("Step 1: Pipeline Overview (전체 흐름)")
    
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>1. Load</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #F44336, #D32F2F); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>2. Detect Issues</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>3. Clean</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 18px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>4. Train & Compare</b>
        </div>
    </div>
    """, unsafe_allow_html=True)





def display_missing_values(df: pd.DataFrame) -> None:
    """Show missing values analysis."""
    st.header("🔍 Step 2-1: Detecting Missing Values")
    
    st.markdown("""
    **First, let's see what's missing in our data:**
    """)
    
    # Calculate missing
    missing = df.isnull().sum()
    missing_pct = (missing / len(df) * 100).round(2)
    
    missing_df = pd.DataFrame({
        'Column': missing.index,
        'Missing Count': missing.values,
        'Missing %': missing_pct.values
    }).sort_values('Missing Count', ascending=False)
    
    missing_df = missing_df[missing_df['Missing Count'] > 0]
    
    if len(missing_df) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            st.dataframe(missing_df, use_container_width=True)
        
        with col2:
            fig, ax = plt.subplots(figsize=(8, 4))
            colors = ['#F44336' if x > 5 else '#FF9800' if x > 1 else '#4CAF50' 
                     for x in missing_df['Missing %']]
            ax.barh(missing_df['Column'], missing_df['Missing %'], color=colors)
            ax.set_xlabel('Missing %')
            ax.set_title('Missing Values by Column')
            ax.grid(True, alpha=0.3, axis='x')
            st.pyplot(fig, use_container_width=True)
            plt.close()
        
        st.code("""
# Check missing values
missing = df.isnull().sum()
print(missing[missing > 0])
        """, language="python")
        
        st.markdown("""
        <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                    border-left: 4px solid #F44336; margin: 15px 0;">
            <b>⚠️ Missing Values Found!</b><br>
            <span style="font-size: 13px;">
            If we train a model with NaN values, it will crash or give wrong results!
            </span>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.success("✅ No missing values found!")
    
    return missing_df


def display_null_handling_options() -> str:
    """Show options for handling nulls."""
    st.header("Step 3-1: How to Handle Missing Values? (결측치 처리)")
    
    st.markdown("""
    **Common strategies:**
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; margin: 5px 0;">
            <b>1. Drop Rows</b><br>
            Remove rows with any null<br>
            ✅ Simple<br>
            ❌ Loses data
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; margin: 5px 0;">
            <b>2. Fill with Mean</b><br>
            Replace null with column average<br>
            ✅ Keeps all rows<br>
            ❌ Ignores distribution
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="padding: 15px; background: rgba(156,39,176,0.1); border-radius: 10px; margin: 5px 0;">
            <b>3. Fill with Median</b><br>
            Replace null with middle value<br>
            ✅ Robust to outliers<br>
            ❌ Same value for all
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; margin: 5px 0;">
            <b>4. Fill with Mode</b><br>
            Replace with most common value<br>
            ✅ Good for categories<br>
            ❌ Not for numeric
        </div>
        """, unsafe_allow_html=True)
    
    st.info("""
    **💡 초보자를 위한 추천 전략: 중앙값(Median) 채우기**
    
    복잡한 고민 없이 **'중앙값(Median)'** 으로 채우는 것이 가장 안전하고 효과적인 출발점입니다.
    평균(Mean)은 이상치(Outlier)에 민감하지만, 중앙값은 흔들리지 않기 때문입니다.
    
    이번 데모에서는 **중앙값** 방식을 적용하겠습니다.
    """)
    
    st.code("""
# Fill missing values with Median
df['column'] = df['column'].fillna(df['column'].median())
    """, language="python")
    
    return "median"


def display_outlier_detection(df: pd.DataFrame) -> None:
    """Show outlier detection detections using interactive Plotly box plots."""
    st.header("🔍 Step 2-2: Detecting Outliers")
    
    st.markdown("""
    **Outliers are extreme values that don't fit the pattern.**
    Hover over the box plot to see Quartiles and Fences!
    """)
    
    # Select column for analysis
    numeric_cols = ['price_10k_krw', 'area_m2']
    
    col1, col2 = st.columns(2)
    
    for i, col in enumerate(numeric_cols):
        with [col1, col2][i]:
            st.markdown(f"### {col}")
            
            # Interactive Box Plot
            fig = px.box(df, y=col, title=f"Box Plot: {col}", points="outliers")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # IQR method stats for explanation
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - 3.0 * IQR  # Relaxed threshold to match Notebook
            upper = Q3 + 3.0 * IQR
            
            outliers = df[(df[col] < lower) | (df[col] > upper)]
            
            st.info(f"""
            **Stats:**
            - **IQR Range**: {Q1:,.0f} ~ {Q3:,.0f}
            - **Outlier Thresholds (3.0 IQR)**: < {lower:,.0f} or > {upper:,.0f}
            - **Outliers Found**: {len(outliers)}
            """)
            
            st.code(f"""
# IQR Method for {col} (Extreme Outliers)
Q1 = df['{col}'].quantile(0.25)
Q3 = df['{col}'].quantile(0.75)
IQR = Q3 - Q1
lower = Q1 - 3.0 * IQR
upper = Q3 + 3.0 * IQR

outliers = df[(df['{col}'] < lower) | (df['{col}'] > upper)]
            """, language="python")
    
    st.info("""
    ### 📊 박스 플롯(Box Plot) 해석 가이드 (초보자용)
    
    박스 플롯은 데이터의 **분포(Distribution)** 와 **이상치(Outlier)** 를 한눈에 보여주는 최고의 도구입니다.
    
    1.  **IQR (Interquartile Range, 사분위수 범위)**:
        *   **설명**: 데이터의 중간 50%가 모여 있는 구간입니다. (Q3 - Q1)
        *   **의미**: "대부분의 평범한 데이터는 이 박스 안에 있습니다."
    
    2.  **Whiskers (수염)**:
        *   **설명**: 박스 위아래로 뻗어 있는 선입니다.
        *   **길이**: 보통 $1.5 \\times IQR$ 까지 뻗습니다.
        *   **의미**: "여기까지는 그래도 정상 범위로 봐줄 수 있습니다."
        
    3.  **Thresholds (임계값/울타리)**:
        *   **설명**: 수염의 끝부분입니다. (Lower/Upper Fence)
        *   **설명**: 수염의 끝부분입니다. (Lower/Upper Fence)
        *   **계산**: $Q1 - 3.0 \\times IQR$ (하한), $Q3 + 3.0 \\times IQR$ (상한)
        
    4.  **Outliers (이상치)**:
        *   **설명**: 수염(울타리) 밖으로 나간 점들입니다.
        *   **의미**: "이건 너무 튀는 값입니다. 에러이거나 예외적인 케이스입니다."
    """)


def display_outlier_handling_options() -> str:
    """Show options for handling outliers."""
    st.header("Step 3-2: How to Handle Outliers? (이상치 처리)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; margin: 5px 0;">
            <b>1. Remove</b><br>
            Delete outlier rows<br>
            ✅ Cleans data completely<br>
            ❌ Loses potentially valid data
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; margin: 5px 0;">
            <b>2. Cap (Winsorize)</b><br>
            Replace extremes with bounds<br>
            ✅ Keeps all rows<br>
            ❌ Changes actual values
        </div>
        """, unsafe_allow_html=True)
    
    st.info("""
    **💡 초보자를 위한 추천 전략: 제거(Remove)**
    
    이상치는 데이터의 '암'과 같습니다. 초보 단계에서는 **과감하게 제거(Remove)** 하는 것이 모델의 혼란을 막는 가장 확실한 방법입니다.
    
    이번 데모에서는 이상치를 **제거**하는 방식을 적용하겠습니다.
    """)
    
    st.code("""
# Remove Outliers
df_clean = df[(df['col'] >= lower) & (df['col'] <= upper)]
    """, language="python")
    
    return "remove"


def display_outlier_game(df: pd.DataFrame) -> None:
    """Interactive demo of outlier impact."""
    st.header("Step 4: Interactive Simulation (이상치 영향력)")
    
    st.markdown("""
    **Outliers are like magnets**: They pull the regression line depending on how strong they are.
    
    👇 **Check the box** to remove outliers and see the line snap back to normal!
    """)
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.write("") 
        st.write("")
        remove = st.checkbox("🚫 Remove Outliers", value=False)
        
    # Prepare data (Simulate a simple case with 1 outlier)
    np.random.seed(42)
    n = 50
    X = np.linspace(10, 100, n)
    y = 5 * X + 100 + np.random.normal(0, 50, n)
    
    # Add one massive outlier
    X_out = np.append(X, [90])
    y_out = np.append(y, [1500]) # Huge price
    types = ['Normal'] * n + ['Outlier']
    
    demo_df = pd.DataFrame({'Area': X_out, 'Price': y_out, 'Type': types})
    
    if remove:
        plot_df = demo_df[demo_df['Type'] == 'Normal']
        line_color = 'green'
        title = "✅ Outliers Removed (Line fits well)"
    else:
        plot_df = demo_df
        line_color = 'red'
        title = "⚠️ With Outliers (Line is pulled up!)"
        
    # Plot using Plotly
    fig = px.scatter(plot_df, x='Area', y='Price', color='Type', 
                     color_discrete_map={'Normal': 'steelblue', 'Outlier': 'red'},
                     trendline="ols", trendline_color_override=line_color,
                     title=title)
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")


def clean_data(df: pd.DataFrame, null_strategy: str, outlier_strategy: str) -> pd.DataFrame:
    """Clean the data based on selected strategies."""
    df_clean = df.copy()
    
    # Handle nulls
    if null_strategy == "drop":
        df_clean = df_clean.dropna()
    elif null_strategy == "mean":
        for col in df_clean.select_dtypes(include=[np.number]).columns:
            df_clean[col] = df_clean[col].fillna(df_clean[col].mean())
    # Handle nulls
    if null_strategy == "drop":
        df_clean = df_clean.dropna()
    elif null_strategy == "median":
        # Fill ALL numeric columns with median
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            df_clean[col] = df_clean[col].fillna(df_clean[col].median())
    
    # Handle outliers for numeric columns
    for col in ['price_10k_krw', 'area_m2']:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 3.0 * IQR
        upper = Q3 + 3.0 * IQR
        
        if outlier_strategy == "remove":
            df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]
        elif outlier_strategy == "cap":
            df_clean[col] = df_clean[col].clip(lower, upper)
    
    return df_clean


def display_before_after(df_baseline: pd.DataFrame, df_clean: pd.DataFrame) -> None:
    """Show before/after comparison."""
    st.header("Step 5: Before vs After (전후 비교)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Before (Baseline)")
        st.metric("Rows", f"{len(df_baseline):,}")
        st.metric("Missing Values", f"{df_baseline.isnull().sum().sum():,}")
        
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.hist(df_baseline['price_10k_krw'].dropna(), bins=50, alpha=0.7, color='grey')
        ax.set_xlabel('Price')
        ax.set_title('Price Distribution (Baseline)')
        ax.set_xlim(0, df_baseline['price_10k_krw'].quantile(0.99) * 2)
        st.pyplot(fig)
        plt.close()
    
    with col2:
        st.markdown("### After (Cleaned Data)")
        st.metric("Rows", f"{len(df_clean):,}", delta=f"{len(df_clean)-len(df_baseline):,}")
        st.metric("Missing Values", f"{df_clean.isnull().sum().sum():,}")
        
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.hist(df_clean['price_10k_krw'].dropna(), bins=50, alpha=0.7, color='green')
        ax.set_xlabel('Price')
        ax.set_title('Price Distribution (Cleaned)')
        ax.set_xlim(0, df_clean['price_10k_krw'].quantile(0.99) * 2)
        st.pyplot(fig)
        plt.close()


@st.cache_resource
@st.cache_resource
def train_and_compare(df_baseline: pd.DataFrame, df_clean: pd.DataFrame):
    """Train models on baseline vs clean data."""
    results = {}
    
    # We compare Level 5 Baseline (Raw Data) vs Level 7 Cleaned
    
    # Common features for fair comparison
    # We use a simple set to isolate the effect of outliers/cleaning
    features = ['area_m2', 'year', 'floor'] 
    
    for name, df in [('baseline', df_baseline), ('clean', df_clean)]:
        # Prepare data
        # For baseline, we just drop NaNs to make it runnable, but keep outliers
        df_train = df.dropna(subset=['price_10k_krw', 'area_m2', 'year', 'floor'])
        
        if len(df_train) < 100:
            continue
            
        # Ensure features exist
        for f in features:
            if f not in df_train.columns:
                 # Should not happen with sample data, but safety check
                df_train[f] = 0
                
        X = df_train[features].values
        y = df_train['price_10k_krw'].values
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=RANDOM_STATE
        )
        
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        rmse_train = calculate_rmse(y_train, model.predict(X_train))
        rmse_test = calculate_rmse(y_test, model.predict(X_test))
        
        results[name] = {
            'n_samples': len(df_train),
            'train_rmse': rmse_train,
            'test_rmse': rmse_test
        }
    
    return results


def display_model_comparison(results: dict) -> None:
    """Compare model performance on baseline vs clean data."""
    st.header("Step 6: Model Performance Check (성능 검증)")
    
    st.info("""
    **ℹ️ Baseline vs Cleaned**
    *   **Level 5 Baseline**: 원본 데이터(이상치 포함)로 학습한 모델입니다.
    *   **Level 7 Cleaned**: 이상치(Outliers)를 제거하여 데이터 품질을 높인 모델입니다.
    """)
    
    st.markdown("### 📏 Model Performance Metrics")
    
    if 'baseline' not in results or 'clean' not in results:
        st.warning("Not enough data for comparison.")
        return
    
    train_diff = results['clean']['train_rmse'] - results['baseline']['train_rmse']
    test_diff = results['clean']['test_rmse'] - results['baseline']['test_rmse']
    improvement = -test_diff / results['baseline']['test_rmse'] * 100
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Baseline (Raw Data)", f"{results['baseline']['test_rmse']:,.0f}", 
                  help=f"Samples: {results['baseline']['n_samples']:,}")
    
    with col2:
        st.metric("Cleaned Data (Outliers Removed)", f"{results['clean']['test_rmse']:,.0f}", 
                  delta=f"{test_diff:+,.0f}", delta_color="inverse",
                  help=f"Samples: {results['clean']['n_samples']:,}")
        
    with col3:
        st.metric("Improvement", f"{improvement:.1f}%",
                   delta="Better Accuracy", delta_color="normal")
    
    # Analysis
    # Analysis
    notebook_target = 25084
    diff_from_nb = abs(results['clean']['test_rmse'] - notebook_target)
    
    if test_diff < 0:
        st.success(f"""
        ✅ **Success!** Cleaning improved RMSE by **{improvement:.1f}%**.
        By removing outliers (extreme prices), the model learned the "general rule" better!
        """)
        
        if diff_from_nb < 1000:
            st.info(f"🎉 **Matches Notebook**: Result ({results['clean']['test_rmse']:,.0f}) is close to notebook benchmark ({notebook_target:,.0f}).")
            
    else:
        st.info("""
        **Note**: RMSE didn't improve much. This suggests the "outliers" might have been valid data points 
        or the model needs more features to explain them (not just cleaning).
        """)
    
    # Compare with other levels
    st.markdown("---")
    
    # Historical Comparison (Standard Format)
    st.subheader("📜 RMSE History (Level 2~7)")
    display_rmse_comparison(7, results['clean']['test_rmse'])


def display_cleaning_code() -> None:
    """Show data cleaning code."""
    st.header("📝 Data Cleaning Code")
    
    st.code("""
import pandas as pd
import numpy as np

# 1. Check for missing values
print(df.isnull().sum())

# 2. Handle missing values
# Option A: Drop rows
df_clean = df.dropna()

# Option B: Fill with median
df['column'] = df['column'].fillna(df['column'].median())

# 3. Detect outliers with IQR
Q1 = df['price'].quantile(0.25)
Q3 = df['price'].quantile(0.75)
IQR = Q3 - Q1
lower = Q1 - 3.0 * IQR
upper = Q3 + 3.0 * IQR

# 4. Handle outliers
# Option A: Remove
df_clean = df[(df['price'] >= lower) & (df['price'] <= upper)]

# Option B: Cap
df['price'] = df['price'].clip(lower, upper)
""", language='python')


def display_limitations() -> None:
    """Show limitations and next steps."""
    st.header("🤔 What's Next?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q: Is cleaning enough?</b><br>
        <span style="font-size: 13px;">
        Cleaning fixes problems, but doesn't create new information.<br>
        <i>→ Level 8 teaches Feature Engineering!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q: What about transforming features?</b><br>
        <span style="font-size: 13px;">
        Some features work better after transformation (log, scaling).<br>
        <i>→ Level 8 covers this too!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
    ### 🚀 Level 8 Preview: Feature Engineering
    
    - **Scaling**: StandardScaler, MinMaxScaler
    - **Transformation**: Log transform for skewed data
    - **Creation**: Build new features from existing ones
    - **Polynomial**: Capture non-linear relationships
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        # Ensure year/floor exist for consistency with Level 5 comparison
        if 'year' not in df.columns:
            # Fallback if missing in sample (should be there)
             df['year'] = df['built_year'] if 'built_year' in df.columns else 2000
        if 'floor' not in df.columns:
             np.random.seed(42)
             df['floor'] = np.random.randint(1, 30, len(df))

        display_header()
        st.markdown("---")
        display_toc()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        
        display_missing_values(df)
        st.markdown("---")
        null_strategy = display_null_handling_options()
        st.markdown("---")
        display_outlier_detection(df)
        st.markdown("---")
        outlier_strategy = display_outlier_handling_options()
        st.markdown("---")
        display_outlier_game(df) # Add interactive game
        st.markdown("---")
        
        # Clean data
        df_clean = clean_data(df, null_strategy, outlier_strategy)
        
        display_before_after(df, df_clean)
        st.markdown("---")
        
        with st.spinner("Training models..."):
            results = train_and_compare(df, df_clean)
        
        display_model_comparison(results)
        st.markdown("---")
        st.code("""
# Final Training Code (Step 6)
model = LinearRegression()
model.fit(X_train, y_train)
rmse = np.sqrt(mean_squared_error(y_test, model.predict(X_test)))
        """, language="python")
        display_cleaning_code()
        st.markdown("---")
        display_limitations()
        
        # Code Link
        
        display_code_link("Level_7_Data_Cleaning.ipynb")
        
        
        
        # Next level teaser
        display_next_level_teaser(7)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
