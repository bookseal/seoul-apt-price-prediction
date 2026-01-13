# -*- coding: utf-8 -*-
"""
Level 1: Heuristic Prediction (No ML)

The simplest end-to-end prediction using district median price per m².
Formula: Predicted Price = Median Price per m² (by district) × Area
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from src.io import load_sample_dataset
from src.navigation import display_next_level_teaser

# Korean to English district name mapping
DISTRICT_NAME_MAP = {
    '강남구': 'Gangnam', '서초구': 'Seocho', '용산구': 'Yongsan',
    '송파구': 'Songpa', '강동구': 'Gangdong', '마포구': 'Mapo',
    '성동구': 'Seongdong', '광진구': 'Gwangjin', '영등포구': 'Yeongdeungpo',
    '양천구': 'Yangcheon', '강서구': 'Gangseo', '구로구': 'Guro',
    '동작구': 'Dongjak', '관악구': 'Gwanak', '서대문구': 'Seodaemun',
    '종로구': 'Jongno', '중구': 'Jung', '동대문구': 'Dongdaemun',
    '성북구': 'Seongbuk', '강북구': 'Gangbuk', '도봉구': 'Dobong',
    '노원구': 'Nowon', '중랑구': 'Jungnang', '금천구': 'Geumcheon',
    '은평구': 'Eunpyeong',
}

def convert_district_name(name: str) -> str:
    """Convert Korean district name to English."""
    return DISTRICT_NAME_MAP.get(name, name)


def display_header() -> None:
    # Colab Badge
    st.markdown("""
    [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bookseal/seoul-apt-price-prediction/blob/main/notebooks/Level_1_Heuristic.ipynb)
    """)

    # Display Level 1 introduction
    st.title("🎯 Level 1: Heuristic Prediction")
    
    # Table of Contents
    # Table of Contents
    st.markdown("""
    **📋 Table of Contents**
    
    1. [📥 Load Data](#step-1-load-data)
    2. [👀 Explore Data](#step-2-explore-data)
    3. [📊 EDA (Exploratory Data Analysis)](#step-3-eda-exploratory-data-analysis)
    4. [📍 Group by District](#step-4-group-by-district)
    5. [📈 Calculate Median](#step-5-calculate-median-m)
    6. [🔮 Predict Price](#step-6-predict)
    """)
    
    # Explain what "Heuristic" means
    with st.expander("💡 What does 'Heuristic' mean?", expanded=True):
        st.markdown("""
        **Heuristic** = A simple rule-of-thumb approach
        
        - Not perfect, but **quick and easy**
        - Based on **common sense**, not complex math
        - Example: *"Apartments in expensive areas cost more per m²"*
        
        Think of it like this:
        > 🍕 "A large pizza costs about 2x a small pizza" - That's a heuristic!
        
        We use simple logic, **not machine learning**. ML comes in Level 2!
        """)
    
    st.success("""
    **Goal**: Predict apartment price using the simplest possible method.
    
    No machine learning needed - just basic math!
    """)


def display_pipeline() -> None:
    """Display the end-to-end pipeline visualization."""
    st.header("🗺️ Pipeline Overview")
    
    st.markdown("""
    This is the complete journey from raw data to prediction.
    Follow each step to understand how we build a simple price estimator!
    """)
    
    # Main Pipeline Diagram - Using Streamlit columns for responsiveness
    st.markdown("""
    <div style="background: linear-gradient(135deg, rgba(76,175,80,0.1) 0%, rgba(33,150,243,0.1) 100%);
                padding: 20px; border-radius: 15px; margin: 20px 0; border: 2px solid #4CAF50;">
        <div style="text-align: center; font-weight: bold; font-size: 16px; margin-bottom: 15px; color: #4CAF50;">
            📋 End-to-End Pipeline
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Use Streamlit columns for better responsiveness
    cols = st.columns(6)
    
    steps = [
        ("📥", "1. Load", "Raw Data", "76,175,80", "#4CAF50"),      # Green
        ("👀", "2. Explore", "Overview", "33,150,243", "#2196F3"),   # Blue
        ("📊", "3. EDA", "Visualize", "156,39,176", "#9C27B0"),      # Purple
        ("📍", "4. Group", "by District", "255,152,0", "#FF9800"),   # Orange
        ("📈", "5. Median", "$/m² calc", "244,67,54", "#F44336"),    # Red
        ("🔮", "6. Predict", "Output!", "76,175,80", "#4CAF50"),     # Green
    ]
    
    for i, (emoji, title, subtitle, rgb, color) in enumerate(steps):
        with cols[i]:
            st.markdown(f"""
            <div style="text-align: center; padding: 10px 5px; background: rgba({rgb}, 0.25); 
                        border-radius: 10px; border: 2px solid {color}; min-height: 80px;">
                <div style="font-size: 20px;">{emoji}</div>
                <div style="font-weight: bold; font-size: 11px;">{title}</div>
                <div style="font-size: 9px; color: gray;">{subtitle}</div>
            </div>
            """, unsafe_allow_html=True)
    
    # Arrow visualization
    st.markdown("""
    <div style="text-align: center; color: #4CAF50; font-size: 14px; margin: 10px 0;">
        ──────────────────→ Flow Direction ──────────────────→
    </div>
    """, unsafe_allow_html=True)
    
    # What happens at each step - Single column layout with motivating questions
    st.markdown("### 🔍 What happens at each step?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 10px 0;">
        <b>📥 Step 1: Load Data</b><br>
        <span style="font-size: 13px; color: #4CAF50; font-style: italic;">
        "Can you cook without ingredients? No data = No analysis!"
        </span><br>
        <span style="font-size: 13px;">
        → Read CSV/Parquet: District, Area, Price, Year, Floor...
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 10px 0;">
        <b>👀 Step 2: Explore Data</b><br>
        <span style="font-size: 13px; color: #2196F3; font-style: italic;">
        "Would you cook without checking your ingredients? Know your data first!"
        </span><br>
        <span style="font-size: 13px;">
        → How many rows? What columns? Missing values? Data types?
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(156,39,176,0.1); border-radius: 10px; 
                border-left: 4px solid #9C27B0; margin: 10px 0;">
        <b>📊 Step 3: EDA (Exploratory Data Analysis)</b><br>
        <span style="font-size: 13px; color: #9C27B0; font-style: italic;">
        "Jump into modeling blindly? See the patterns with your eyes first!"
        </span><br>
        <span style="font-size: 13px;">
        → Visualize: Price distribution, Area vs Price, District differences
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>📍 Step 4: Group by District</b><br>
        <span style="font-size: 13px; color: #FF9800; font-style: italic;">
        "Is Gangnam the same price as other areas? Location matters!"
        </span><br>
        <span style="font-size: 13px;">
        → Group transactions by district. Different area = Different price.
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 10px 0;">
        <b>📈 Step 5: Calculate Median $/m²</b><br>
        <span style="font-size: 13px; color: #F44336; font-style: italic;">
        "Use average? One $100M apartment ruins everything! Use median!"
        </span><br>
        <span style="font-size: 13px;">
        → price_per_m2 = price / area → Take MEDIAN (safe from outliers!)
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(76,175,80,0.2); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 10px 0;">
        <b>🔮 Step 6: Predict!</b><br>
        <span style="font-size: 13px; color: #4CAF50; font-style: italic;">
        "All ready! One simple formula gives you the price!"
        </span><br>
        <span style="font-size: 13px;">
        → <code style="background: #2d2d2d; padding: 4px 8px; border-radius: 4px; color: #4CAF50;">Price = Median($/m²) × Area</code> Done!
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_method() -> None:
    """Explain the heuristic method with formula."""
    st.header("📐 The Method")
    
    st.markdown("""
    ### Simple Logic
    
    We use **one assumption**: apartments in the same district have similar price per m².
    """)
    
    # Formula visualization
    st.markdown("""
    <div style="text-align: center; padding: 25px; background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                border-radius: 15px; margin: 20px 0; border: 2px solid #4CAF50;">
        <div style="font-size: 14px; color: #888; margin-bottom: 10px;">THE FORMULA</div>
        <div style="font-size: 22px; color: #4CAF50; font-family: monospace;">
            Predicted Price = Median(Price/m²)<sub>district</sub> × Area
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    with st.expander("🤔 Why does this work?"):
        st.markdown("""
        - **Location matters most** in real estate
        - Apartments in the same district have similar price per m²
        - Median is robust to outliers (unlike mean)
        
        This is our **baseline** - any ML model should beat this!
        """)


def display_step1_load(df: pd.DataFrame) -> None:
    """Step 1: Data Loading."""
    st.header("📥 Step 1: Load Data")
    
    st.markdown("""
    <div style="padding: 10px 15px; background: rgba(76,175,80,0.15); border-radius: 8px; 
                border-left: 4px solid #4CAF50; margin-bottom: 15px;">
        <b>What we're doing:</b> Reading the raw apartment transaction data
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    col1.metric("📄 Total Rows", f"{len(df):,}")
    col2.metric("📍 Districts", f"{df['district'].nunique()}")
    col3.metric("📅 Year Range", f"{df['year'].min()}-{df['year'].max()}")
    
    with st.expander("👀 View raw data sample"):
        st.dataframe(df.head(10), use_container_width=True)
    
    st.code("""
# Python code to load data
import pandas as pd
df = pd.read_parquet("data/sample.parquet")
print(f"Loaded {len(df):,} rows")
    """, language="python")


def display_step2_explore(df: pd.DataFrame) -> None:
    """Step 2: Data Exploration."""
    st.header("👀 Step 2: Explore Data")
    
    st.markdown("""
    <div style="padding: 10px 15px; background: rgba(33,150,243,0.15); border-radius: 8px; 
                border-left: 4px solid #2196F3; margin-bottom: 15px;">
        <b>What we're doing:</b> Understanding our data structure and quality
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**📋 Column Info**")
        info_df = pd.DataFrame({
            'Column': df.columns,
            'Type': df.dtypes.astype(str),
            'Non-Null': df.count().values
        })
        st.dataframe(info_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("**📊 Basic Stats**")
        st.dataframe(df[['area_m2', 'price_10k_krw']].describe().round(1), 
                     use_container_width=True)
    
    st.code("""
# Check data structure
df.info()
df.describe()
df.isnull().sum()  # Check missing values
    """, language="python")


def display_step3_eda(df: pd.DataFrame) -> None:
    """Step 3: Exploratory Data Analysis."""
    st.header("📊 Step 3: EDA (Exploratory Data Analysis)")
    
    st.markdown("""
    <div style="padding: 10px 15px; background: rgba(156,39,176,0.15); border-radius: 8px; 
                border-left: 4px solid #9C27B0; margin-bottom: 15px;">
        <b>What we're doing:</b> Visualizing data to find patterns and insights
    </div>
    """, unsafe_allow_html=True)
    
    # ========== 1. Price Distribution ==========
    st.markdown("### 1️⃣ Price Distribution")
    
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.hist(df['price_10k_krw'], bins=50, color='#9C27B0', alpha=0.7, edgecolor='white')
    ax.set_xlabel('Price (10K KRW)')
    ax.set_ylabel('Count')
    ax.axvline(df['price_10k_krw'].median(), color='red', linestyle='--', linewidth=2, label=f"Median: {df['price_10k_krw'].median():,.0f}")
    ax.axvline(df['price_10k_krw'].mean(), color='orange', linestyle='--', linewidth=2, label=f"Mean: {df['price_10k_krw'].mean():,.0f}")
    ax.legend()
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Insight for Price Distribution
    col1, col2, col3 = st.columns(3)
    col1.metric("Median Price", f"{df['price_10k_krw'].median():,.0f} 만원")
    col2.metric("Mean Price", f"{df['price_10k_krw'].mean():,.0f} 만원")
    col3.metric("Difference", f"{df['price_10k_krw'].mean() - df['price_10k_krw'].median():,.0f} 만원")
    
    st.markdown("""
    <div style="padding: 12px; background: rgba(156,39,176,0.1); border-radius: 8px; margin: 10px 0;">
        <b>🔍 What we learned:</b><br>
        • <b>Right-skewed distribution</b> - Most apartments are cheap, few are very expensive<br>
        • <b>Mean > Median</b> - Expensive apartments pull the average UP<br>
        • <b>Outliers exist</b> - Some apartments cost 10x more than typical ones!<br>
        • <b>Why it matters:</b> We should use <b>MEDIAN</b>, not mean (outlier-proof!)
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ========== 2. Area vs Price ==========
    st.markdown("### 2️⃣ Area vs Price Relationship")
    
    sample = df.sample(n=min(3000, len(df)), random_state=42)
    
    # Calculate correlation
    corr = df['area_m2'].corr(df['price_10k_krw'])
    
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.scatter(sample['area_m2'], sample['price_10k_krw'], 
               alpha=0.3, s=15, c='#2196F3')
    ax.set_xlabel('Area (m²)')
    ax.set_ylabel('Price (10K KRW)')
    ax.grid(True, alpha=0.3)
    
    # Add trend line
    z = np.polyfit(sample['area_m2'], sample['price_10k_krw'], 1)
    p = np.poly1d(z)
    x_line = np.linspace(sample['area_m2'].min(), sample['area_m2'].max(), 100)
    ax.plot(x_line, p(x_line), 'r--', linewidth=2, label=f'Trend (r={corr:.2f})')
    ax.legend()
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Insight for Area vs Price
    col1, col2 = st.columns(2)
    col1.metric("Correlation (r)", f"{corr:.3f}", "Strong positive!")
    col2.metric("Trend", f"+{z[0]:,.0f} 만원/m²", "Price increase per m²")
    
    st.markdown("""
    <div style="padding: 12px; background: rgba(33,150,243,0.1); border-radius: 8px; margin: 10px 0;">
        <b>🔍 What we learned:</b><br>
        • <b>Strong positive correlation</b> - Bigger area = Higher price (obvious!)<br>
        • <b>BUT look at the spread!</b> - Same 85m² can be 5억 or 20억. Why?<br>
        • <b>Hidden variable</b> - Something else affects price. What is it?<br>
        • <b>Answer:</b> <span style="color: #FF9800; font-weight: bold;">LOCATION (District)!</span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ========== 3. Price by District ==========
    st.markdown("### 3️⃣ Price by District (The Hidden Factor!)")
    
    # Calculate price per m2 and get top/bottom districts
    df_calc = df.copy()
    df_calc['price_per_m2'] = df_calc['price_10k_krw'] / df_calc['area_m2']
    district_median = df_calc.groupby('district')['price_per_m2'].median().sort_values(ascending=False)
    
    # Convert Korean district names to English for chart
    district_median_en = district_median.copy()
    district_median_en.index = [convert_district_name(d) for d in district_median_en.index]
    
    fig, ax = plt.subplots(figsize=(10, 4))
    colors = ['#4CAF50' if i < 5 else '#F44336' if i >= len(district_median_en) - 5 else '#9E9E9E' 
              for i in range(len(district_median_en))]
    district_median_en.plot(kind='bar', ax=ax, color=colors, edgecolor='white')
    ax.set_xlabel('District')
    ax.set_ylabel('Median Price per m² (10K KRW)')
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right', fontsize=8)
    ax.grid(True, alpha=0.3, axis='y')
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Show top and bottom (with both Korean and English names)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**🏆 Top 5 (Most Expensive)**")
        for i, (district, price) in enumerate(district_median.head(5).items()):
            en_name = convert_district_name(district)
            st.markdown(f"{i+1}. **{en_name}** ({district}): {price:,.0f}")
    with col2:
        st.markdown("**📉 Bottom 5 (Cheapest)**")
        for i, (district, price) in enumerate(district_median.tail(5).items()):
            en_name = convert_district_name(district)
            st.markdown(f"{i+1}. **{en_name}** ({district}): {price:,.0f}")
    
    # Price gap
    price_gap = district_median.iloc[0] / district_median.iloc[-1]
    
    st.markdown(f"""
    <div style="padding: 12px; background: rgba(255,152,0,0.1); border-radius: 8px; margin: 10px 0;">
        <b>🔍 What we learned:</b><br>
        • <b>Huge price gap!</b> - Top district is <b>{price_gap:.1f}x</b> more expensive than bottom<br>
        • <b>Gangnam effect</b> - 강남, 서초, 용산 are premium areas<br>
        • <b>Location = Everything</b> - Same apartment, different district = VERY different price<br>
        • <b>Conclusion:</b> We MUST consider district when predicting price!
    </div>
    """, unsafe_allow_html=True)
    
    st.success("💡 **Final EDA Insight**: Price depends on AREA and DISTRICT. Our heuristic model uses both!")


def display_step4_group(df: pd.DataFrame) -> None:
    """Step 4: Group by District."""
    st.header("📍 Step 4: Group by District")
    
    st.markdown("""
    <div style="padding: 10px 15px; background: rgba(255,152,0,0.15); border-radius: 8px; 
                border-left: 4px solid #FF9800; margin-bottom: 15px;">
        <b>What we're doing:</b> Grouping data by location (district) to capture price differences
    </div>
    """, unsafe_allow_html=True)
    
    # Show district counts
    district_counts = df.groupby('district').size().sort_values(ascending=False)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("**Transactions per District**")
        st.bar_chart(district_counts.head(10))
    
    with col2:
        st.markdown("**Top 5 Districts**")
        for district, count in district_counts.head(5).items():
            st.markdown(f"• **{district}**: {count:,} 건")
    
    st.code("""
# Group by district
grouped = df.groupby('district')
for district, group_df in grouped:
    print(f"{district}: {len(group_df)} transactions")
    """, language="python")


def display_step5_median(df: pd.DataFrame) -> None:
    """Step 5: Calculate Median Price per m²."""
    st.header("📈 Step 5: Calculate Median $/m²")
    
    st.markdown("""
    <div style="padding: 10px 15px; background: rgba(244,67,54,0.15); border-radius: 8px; 
                border-left: 4px solid #F44336; margin-bottom: 15px;">
        <b>What we're doing:</b> Computing the "typical" price per m² for each district
    </div>
    """, unsafe_allow_html=True)
    
    # Calculate price per m²
    df_calc = df.copy()
    df_calc['price_per_m2'] = df_calc['price_10k_krw'] / df_calc['area_m2']
    
    # Get median by district
    district_median = df_calc.groupby('district')['price_per_m2'].median().sort_values(ascending=False)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("**Median Price per m² by District**")
        st.bar_chart(district_median)
    
    with col2:
        st.markdown("**Top 5 Most Expensive**")
        for district, median in district_median.head(5).items():
            st.markdown(f"• **{district}**: {median:,.0f}")
        
        st.markdown("---")
        st.markdown("**Bottom 3**")
        for district, median in district_median.tail(3).items():
            st.markdown(f"• **{district}**: {median:,.0f}")
    
    # Why median?
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; margin-top: 15px;">
        <b>🤔 Why MEDIAN instead of MEAN?</b><br><br>
        <table style="width: 100%;">
            <tr>
                <td style="padding: 8px;"><b>Mean (평균)</b></td>
                <td style="padding: 8px;">Sensitive to outliers. One 100억 apartment skews everything!</td>
            </tr>
            <tr>
                <td style="padding: 8px;"><b>Median (중앙값)</b></td>
                <td style="padding: 8px;">Robust. The "middle" value - outliers don't affect it much.</td>
            </tr>
        </table>
    </div>
    """, unsafe_allow_html=True)
    
    st.code("""
# Calculate price per m² and get median
df['price_per_m2'] = df['price_10k_krw'] / df['area_m2']
median_by_district = df.groupby('district')['price_per_m2'].median()
    """, language="python")


def predict_heuristic(df: pd.DataFrame, district: str, area: float) -> float:
    """
    Calculate heuristic price prediction.
    
    Args:
        df: Sample dataset
        district: Selected district
        area: Exclusive area in m²
    
    Returns:
        Predicted price in 10K KRW
    """
    df_calc = df.copy()
    df_calc['price_per_m2'] = df_calc['price_10k_krw'] / df_calc['area_m2']
    median_price_per_m2 = df_calc[df_calc['district'] == district]['price_per_m2'].median()
    return median_price_per_m2 * area


def display_step6_predict(df: pd.DataFrame) -> None:
    """Step 6: Interactive prediction demo."""
    st.header("🔮 Step 6: Predict!")
    
    st.markdown("""
    <div style="padding: 10px 15px; background: rgba(76,175,80,0.15); border-radius: 8px; 
                border-left: 4px solid #4CAF50; margin-bottom: 15px;">
        <b>What we're doing:</b> Using our heuristic formula to predict apartment prices!
    </div>
    """, unsafe_allow_html=True)
    
    # Input
    col1, col2 = st.columns(2)
    
    with col1:
        districts = sorted(df['district'].unique())
        selected_district = st.selectbox("🏘️ Select District", districts)
    
    with col2:
        selected_area = st.slider("📐 Exclusive Area (m²)", 
                                   min_value=10, max_value=200, value=84)
    
    # Predict
    predicted_price = predict_heuristic(df, selected_district, selected_area)
    
    # Get median for display
    df_calc = df.copy()
    df_calc['price_per_m2'] = df_calc['price_10k_krw'] / df_calc['area_m2']
    median_ppm2 = df_calc[df_calc['district'] == selected_district]['price_per_m2'].median()
    
    # Result display
    st.markdown(f"""
    <div style="text-align: center; padding: 25px; background: linear-gradient(135deg, rgba(76,175,80,0.2) 0%, rgba(76,175,80,0.1) 100%);
                border-radius: 15px; margin: 20px 0; border: 3px solid #4CAF50;">
        <div style="font-size: 14px; color: #888; margin-bottom: 5px;">PREDICTED PRICE</div>
        <div style="font-size: 36px; font-weight: bold; color: #4CAF50;">
            {predicted_price:,.0f} <span style="font-size: 18px;">만원</span>
        </div>
        <div style="font-size: 16px; color: #888; margin-top: 5px;">
            ≈ {predicted_price/10000:.1f} 억원
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Show calculation
    with st.expander("📝 See calculation details"):
        st.markdown(f"""
        **Formula**: `Price = Median($/m²) × Area`
        
        **Calculation**:
        1. District: **{selected_district}**
        2. Median price/m² in {selected_district}: **{median_ppm2:,.0f}** 만원/m²
        3. Your area: **{selected_area}** m²
        4. Result: {median_ppm2:,.0f} × {selected_area} = **{predicted_price:,.0f}** 만원
        """)
        
        st.code(f"""
# Your prediction in Python
median_price_per_m2 = {median_ppm2:,.0f}  # for {selected_district}
area = {selected_area}
predicted_price = median_price_per_m2 * area
print(f"Predicted: {{predicted_price:,}} 만원")  # {predicted_price:,.0f} 만원
        """, language="python")


def display_questions() -> None:
    """Show common questions users might have."""
    st.header("🤔 Questions You Might Have")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,193,7,0.1); border-radius: 10px; 
                border-left: 4px solid #FFC107; margin: 10px 0;">
        <b>Q1: "Is this really Machine Learning?"</b><br>
        <span style="color: #FFC107;">→ NO!</span> This is just statistics (median calculation). 
        Real ML starts in <b>Level 2</b> where the computer <b>learns</b> from data!
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,193,7,0.1); border-radius: 10px; 
                border-left: 4px solid #FFC107; margin: 10px 0;">
        <b>Q2: "Why median, not average (mean)?"</b><br>
        <span style="color: #FFC107;">→ Outliers!</span> One $100M penthouse would skew the average. 
        Median is <b>robust</b> - it ignores extreme values.
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,193,7,0.1); border-radius: 10px; 
                border-left: 4px solid #FFC107; margin: 10px 0;">
        <b>Q3: "Same district = Same price per m²? Really?"</b><br>
        <span style="color: #FFC107;">→ That's the weakness!</span> A 20-year-old apartment and a brand new one 
        in Gangnam get the same price/m². That's obviously wrong!
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,193,7,0.1); border-radius: 10px; 
                border-left: 4px solid #FFC107; margin: 10px 0;">
        <b>Q4: "What about floor, building age, complex name?"</b><br>
        <span style="color: #FFC107;">→ Great question!</span> We ignore them here. 
        <b>Level 3</b> will use multiple features!
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.15); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 10px 0;">
        <b>Q5: "How is Level 2 different?"</b><br>
        <span style="color: #2196F3;">→ Level 2 uses Linear Regression!</span><br>
        • Level 1: We <b>calculate</b> median (no learning)<br>
        • Level 2: Computer <b>learns</b> optimal w, b from data<br>
        • Formula: <code>Price = w × Area + b</code><br>
        • The machine finds the best w and b automatically!
    </div>
    """, unsafe_allow_html=True)


def display_limitations() -> None:
    """Show method limitations and next steps."""
    st.header("⚠️ Limitations & Next Steps")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 20px; background: rgba(244,67,54,0.1); border-radius: 10px; border: 2px solid #F44336;">
            <div style="font-size: 18px; font-weight: bold; margin-bottom: 10px;">❌ What we ignore:</div>
            <ul style="margin: 0; padding-left: 20px;">
                <li>Floor number (higher = more $)</li>
                <li>Building age (newer = more $)</li>
                <li>Specific apartment complex</li>
                <li>Market trends over time</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="padding: 20px; background: rgba(76,175,80,0.1); border-radius: 10px; border: 2px solid #4CAF50;">
            <div style="font-size: 18px; font-weight: bold; margin-bottom: 10px;">✅ Next Level (ML!):</div>
            <ul style="margin: 0; padding-left: 20px;">
                <li>Use Linear Regression</li>
                <li>LEARN from data</li>
                <li>Multiple features</li>
                <li>Evaluate with metrics</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("""
    ---
    
    ### 🎓 Summary
    
    You've completed Level 1! You learned:
    
    1. **Load** raw data
    2. **Explore** data structure
    3. **EDA** to find patterns
    4. **Group** by location
    5. **Calculate** median price/m²
    6. **Predict** using simple formula
    
    **Ready for Level 2?** → Use machine learning to improve predictions!
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_pipeline()
        st.markdown("---")
        display_method()
        st.markdown("---")
        display_step1_load(df)
        st.markdown("---")
        display_step2_explore(df)
        st.markdown("---")
        display_step3_eda(df)
        st.markdown("---")
        display_step4_group(df)
        st.markdown("---")
        display_step5_median(df)
        st.markdown("---")
        display_step6_predict(df)
        st.markdown("---")
        display_questions()
        st.markdown("---")
        display_limitations()
        
        # Next level teaser
        display_next_level_teaser(1)
        
    except Exception as e:
        st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
