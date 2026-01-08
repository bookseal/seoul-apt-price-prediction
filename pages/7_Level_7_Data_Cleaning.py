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
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser


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


def display_pipeline_overview() -> None:
    """Show the pipeline."""
    st.header("🔄 Level 7 Pipeline")
    
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


def create_messy_data(df: pd.DataFrame) -> pd.DataFrame:
    """Create synthetic messy data for demonstration."""
    df = df.copy()
    np.random.seed(RANDOM_STATE)
    n = len(df)
    
    # Add year and floor if not present
    if 'year' not in df.columns:
        df['year'] = np.random.randint(1985, 2024, n)
    if 'floor' not in df.columns:
        df['floor'] = np.random.randint(1, 30, n)
    
    # Create missing values (5% random)
    null_mask = np.random.random(n) < 0.05
    df.loc[null_mask, 'floor'] = np.nan
    
    null_mask2 = np.random.random(n) < 0.03
    df.loc[null_mask2, 'year'] = np.nan
    
    # Create outliers (1% extreme values)
    outlier_mask = np.random.random(n) < 0.01
    df.loc[outlier_mask, 'price_10k_krw'] = df['price_10k_krw'] * np.random.uniform(5, 10, sum(outlier_mask))
    
    outlier_mask2 = np.random.random(n) < 0.01
    df.loc[outlier_mask2, 'area_m2'] = df['area_m2'] * np.random.uniform(5, 10, sum(outlier_mask2))
    
    return df


def display_missing_values(df: pd.DataFrame) -> None:
    """Show missing values analysis."""
    st.header("🔍 Step 2a: Detecting Missing Values")
    
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
    st.header("🛠️ How to Handle Missing Values?")
    
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
    
    strategy = st.selectbox(
        "Choose a strategy for this demo:",
        ["drop", "mean", "median"],
        format_func=lambda x: {"drop": "Drop rows with null", 
                               "mean": "Fill with mean", 
                               "median": "Fill with median"}[x]
    )
    
    return strategy


def display_outlier_detection(df: pd.DataFrame) -> None:
    """Show outlier detection methods."""
    st.header("🔍 Step 2b: Detecting Outliers")
    
    st.markdown("""
    **Outliers are extreme values that don't fit the pattern.**
    """)
    
    # Select column for analysis
    numeric_cols = ['price_10k_krw', 'area_m2']
    
    col1, col2 = st.columns(2)
    
    for i, col in enumerate(numeric_cols):
        with [col1, col2][i]:
            st.markdown(f"### {col}")
            
            # Box plot
            fig, ax = plt.subplots(figsize=(6, 4))
            bp = ax.boxplot(df[col].dropna(), vert=True)
            ax.set_ylabel(col)
            ax.set_title(f'Box Plot: {col}')
            st.pyplot(fig)
            plt.close()
            
            # IQR method
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR
            
            outliers = df[(df[col] < lower) | (df[col] > upper)]
            
            st.markdown(f"""
            **IQR Method:**
            - Q1: {Q1:,.0f}
            - Q3: {Q3:,.0f}
            - Lower bound: {lower:,.0f}
            - Upper bound: {upper:,.0f}
            - **Outliers: {len(outliers)} ({len(outliers)/len(df)*100:.1f}%)**
            """)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 15px 0;">
        <b>📊 IQR Method Explained</b><br>
        <span style="font-size: 13px;">
        • Q1 = 25th percentile (lower quartile)<br>
        • Q3 = 75th percentile (upper quartile)<br>
        • IQR = Q3 - Q1 (interquartile range)<br>
        • Outlier if: value < Q1 - 1.5×IQR or value > Q3 + 1.5×IQR
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_outlier_handling_options() -> str:
    """Show options for handling outliers."""
    st.header("🛠️ How to Handle Outliers?")
    
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
    
    strategy = st.selectbox(
        "Choose outlier handling strategy:",
        ["remove", "cap"],
        format_func=lambda x: {"remove": "Remove outliers", 
                               "cap": "Cap at IQR bounds"}[x]
    )
    
    return strategy


def clean_data(df: pd.DataFrame, null_strategy: str, outlier_strategy: str) -> pd.DataFrame:
    """Clean the data based on selected strategies."""
    df_clean = df.copy()
    
    # Handle nulls
    if null_strategy == "drop":
        df_clean = df_clean.dropna()
    elif null_strategy == "mean":
        for col in df_clean.select_dtypes(include=[np.number]).columns:
            df_clean[col] = df_clean[col].fillna(df_clean[col].mean())
    elif null_strategy == "median":
        for col in df_clean.select_dtypes(include=[np.number]).columns:
            df_clean[col] = df_clean[col].fillna(df_clean[col].median())
    
    # Handle outliers for numeric columns
    for col in ['price_10k_krw', 'area_m2']:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        
        if outlier_strategy == "remove":
            df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]
        elif outlier_strategy == "cap":
            df_clean[col] = df_clean[col].clip(lower, upper)
    
    return df_clean


def display_before_after(df_messy: pd.DataFrame, df_clean: pd.DataFrame) -> None:
    """Show before/after comparison."""
    st.header("📊 Before vs After Cleaning")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Before (Messy Data)")
        st.metric("Rows", f"{len(df_messy):,}")
        st.metric("Missing Values", f"{df_messy.isnull().sum().sum():,}")
        
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.hist(df_messy['price_10k_krw'].dropna(), bins=50, alpha=0.7, color='red')
        ax.set_xlabel('Price')
        ax.set_title('Price Distribution (Messy)')
        ax.set_xlim(0, df_messy['price_10k_krw'].quantile(0.99) * 2)
        st.pyplot(fig)
        plt.close()
    
    with col2:
        st.markdown("### After (Cleaned Data)")
        st.metric("Rows", f"{len(df_clean):,}", delta=f"{len(df_clean)-len(df_messy):,}")
        st.metric("Missing Values", f"{df_clean.isnull().sum().sum():,}")
        
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.hist(df_clean['price_10k_krw'].dropna(), bins=50, alpha=0.7, color='green')
        ax.set_xlabel('Price')
        ax.set_title('Price Distribution (Cleaned)')
        ax.set_xlim(0, df_clean['price_10k_krw'].quantile(0.99) * 2)
        st.pyplot(fig)
        plt.close()


@st.cache_resource
def train_and_compare(df_messy: pd.DataFrame, df_clean: pd.DataFrame):
    """Train models on messy vs clean data."""
    results = {}
    
    for name, df in [('messy', df_messy), ('clean', df_clean)]:
        # Prepare data
        df_train = df.dropna(subset=['price_10k_krw', 'area_m2'])
        
        if len(df_train) < 100:
            continue
        
        X = df_train[['area_m2']].values
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
    """Compare model performance on messy vs clean data."""
    st.header("📏 Model Performance Comparison")
    
    if 'messy' not in results or 'clean' not in results:
        st.warning("Not enough data for comparison.")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Messy Data Model")
        st.metric("Training Samples", f"{results['messy']['n_samples']:,}")
        st.metric("Train RMSE", f"{results['messy']['train_rmse']:,.0f}")
        st.metric("Test RMSE", f"{results['messy']['test_rmse']:,.0f}")
    
    with col2:
        st.markdown("### Clean Data Model")
        st.metric("Training Samples", f"{results['clean']['n_samples']:,}")
        
        train_diff = results['clean']['train_rmse'] - results['messy']['train_rmse']
        test_diff = results['clean']['test_rmse'] - results['messy']['test_rmse']
        
        st.metric("Train RMSE", f"{results['clean']['train_rmse']:,.0f}", 
                  delta=f"{train_diff:+,.0f}")
        st.metric("Test RMSE", f"{results['clean']['test_rmse']:,.0f}", 
                  delta=f"{test_diff:+,.0f}")
    
    if test_diff < 0:
        improvement = -test_diff / results['messy']['test_rmse'] * 100
        st.success(f"""
        ✅ **Cleaning improved RMSE by {improvement:.1f}%!**
        
        Clean data leads to better predictions!
        """)
    else:
        st.info("""
        The improvement depends on how messy the original data was 
        and the cleaning strategies chosen.
        """)


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
lower = Q1 - 1.5 * IQR
upper = Q3 + 1.5 * IQR

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
        df_messy = create_messy_data(df)
        
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        
        display_missing_values(df_messy)
        st.markdown("---")
        null_strategy = display_null_handling_options()
        st.markdown("---")
        display_outlier_detection(df_messy)
        st.markdown("---")
        outlier_strategy = display_outlier_handling_options()
        st.markdown("---")
        
        # Clean data
        df_clean = clean_data(df_messy, null_strategy, outlier_strategy)
        
        display_before_after(df_messy, df_clean)
        st.markdown("---")
        
        with st.spinner("Training models..."):
            results = train_and_compare(df_messy, df_clean)
        
        display_model_comparison(results)
        st.markdown("---")
        display_cleaning_code()
        st.markdown("---")
        display_limitations()
        
        # Next level teaser
        display_next_level_teaser(7)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
