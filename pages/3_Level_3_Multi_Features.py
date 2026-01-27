# -*- coding: utf-8 -*-
"""
Level 3: Multi-Feature Linear Regression (Area + District)

Learn how to handle categorical features using One-Hot Encoding.
Formula: Price = w1 * Area + w2 * District_A + w3 * District_B + ... + b
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison


def display_header() -> None:
    """Display Level 3 introduction."""
    st.title("🏘️ Level 3: Multi-Feature Linear Regression")
    
    # Table of Contents
    # Table of Contents
    st.markdown("""
    **📋 Table of Contents**
    
    1. [🔄 One-Hot Concept](#understanding-one-hot-encoding)
    2. [🚂 Training Process](#step-3-training)
    3. [🧠 Analyze Weights](#what-did-the-model-learn)
    4. [📏 Model Performance](#model-performance)
    5. [🔮 Prediction Demo](#try-it-yourself)
    """)
    
    st.success("""
    **Goal**: Add District to our model using **One-Hot Encoding**.
    
    Now the model can learn: Gangnam apartments cost more!
    """)
    
    with st.expander("💡 Why One-Hot Encoding? (Important!)"):
        st.markdown("""
        **Why not just use numbers like 1, 2, 3?**
        
        If we say `Gangnam=1`, `Seocho=2`, `Nowon=3`:
        *   The model thinks **Nowon (3)** is "3 times greater" than **Gangnam (1)**.
        *   It creates a **false ranking** (Order) that doesn't exist.
        
        **Solution: One-Hot Encoding**
        *   Treats every district as a separate, independent "switch".
        *   Gangnam? Yes/No (1/0)
        *   Seocho? Yes/No (1/0)
        *   **No false math, just pure categorical data!**
        """)


def display_pipeline_overview() -> None:
    """Show the end-to-end ML pipeline for Level 3."""
    st.header("🔄 Level 3 Pipeline Overview")
    
    st.markdown("""
    **What we'll do step by step:**
    """)
    
    # Pipeline flow diagram
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>1. Load</b><br><span style="font-size: 11px;">Get data</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>2. Encode</b><br><span style="font-size: 11px;">One-Hot</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>3. Train</b><br><span style="font-size: 11px;">Learn weights</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #E91E63, #C2185B); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>4. Evaluate</b><br><span style="font-size: 11px;">Check RMSE</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>5. Predict</b><br><span style="font-size: 11px;">Use model</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Step explanations
    # What happens at each step - Single column layout with motivating questions
    st.markdown("### 🔍 What happens at each step?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 10px 0;">
        <b>📥 Step 1: Load Data</b><br>
        <span style="font-size: 13px;">
        Same as before - get Area, District, and Price data.
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(156,39,176,0.1); border-radius: 10px; 
                border-left: 4px solid #9C27B0; margin: 10px 0;">
        <b>🔄 Step 2: One-Hot Encode</b> ⭐ <i>NEW in Level 3!</i><br>
        <span style="font-size: 13px;">
        <b>Transform Text to Independent Switches</b><br>
        Create a separate column for each district.<br>
        Prevents the model from learning false patterns (e.g. A > B).
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>🎓 Step 3: Train Model</b><br>
        <span style="font-size: 13px;">
        Now we train with Area + District columns.<br>
        Formula: Price = w_area × Area + w_gangnam × Is_Gangnam + w_seocho × Is_Seocho + ...
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(233,30,99,0.1); border-radius: 10px; 
                border-left: 4px solid #E91E63; margin: 10px 0;">
        <b>📏 Step 4: Evaluate</b><br>
        <span style="font-size: 13px;">
        Compare RMSE with Level 2. Did adding District help?
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 10px 0;">
        <b>🔮 Step 5: Predict</b><br>
        <span style="font-size: 13px;">
        Now we can predict different prices for Gangnam vs Nowon!
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_why_level3() -> None:
    """Explain problems with Level 2 and motivation for Level 3."""
    st.header("🤔 Wait... What's Wrong with Level 2?")
    
    st.markdown("""
    Level 2 worked! But think about this problem...
    """)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 10px 0;">
        <b>❌ The Big Problem: Location Doesn't Matter!</b><br>
        <span style="font-size: 13px;">
        In Level 2, a 100m² apartment in <b>Gangnam</b> has the SAME predicted price 
        as a 100m² apartment in <b>Nowon</b>!<br><br>
        But we all know: <b>Location is EVERYTHING in real estate!</b><br>
        <i>→ We need to add District to our model!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Visual comparison
    st.markdown("### 📊 Same Area, Different Districts")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 20px; background: rgba(244,67,54,0.1); border-radius: 10px; text-align: center;">
            <b>Level 2 Prediction</b><br><br>
            100m² in Gangnam: <b>90,000</b><br>
            100m² in Nowon: <b>90,000</b><br><br>
            <span style="color: #F44336;">❌ Same price? That's wrong!</span>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="padding: 20px; background: rgba(76,175,80,0.1); border-radius: 10px; text-align: center;">
            <b>Level 3 Prediction</b><br><br>
            100m² in Gangnam: <b>150,000</b><br>
            100m² in Nowon: <b>50,000</b><br><br>
            <span style="color: #4CAF50;">✅ Different prices! Realistic!</span>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
    <div style="padding: 20px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 15px 0;">
        <b>✅ Level 3 Solution: Add District Feature!</b><br><br>
        <span style="font-size: 14px;">
        But wait... District is a <b>word</b> ("Gangnam", "Seocho"), not a number!<br>
        How can we put it into our equation?<br><br>
        <b>Answer: One-Hot Encoding!</b><br>
        Convert each district into a binary (0/1) column.
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_onehot_explanation(df: pd.DataFrame) -> None:
    """Explain One-Hot Encoding with examples."""
    st.header("🔄 Understanding One-Hot Encoding")
    
    st.markdown("""
    **The Problem with Simple Numbers (Label Encoding)**
    
    If we assign numbers arbitrarily:
    *   **Gangnam = 1**
    *   **Seocho = 2**
    *   **Nowon = 3**
    
    The Linear Regression formula `y = wx + b` would do this math:
    > `Price = w * (District Number) + b`
    
    It would calculate `Price` for **Nowon (3)** as **3 times** the effect of **Gangnam (1)**.
    **This is logically wrong!** Districts are categories, not magnitudes.
    
    ---
    
    **The Solution: One-Hot Encoding**
    
    We create a **separate binary column** for each district. Each column asks a Yes/No question:
    *   "Is this Gangnam?" (1 or 0)
    *   "Is this Seocho?" (1 or 0)
    
    This treats all districts fairly and independently.
    """)
    
    # Show example with real data
    st.markdown("### Before: Original Data")
    
    sample = df[['district', 'area_m2', 'price_10k_krw']].head(5)
    st.dataframe(sample)
    
    st.markdown("### After: One-Hot Encoded")
    
    # Create example one-hot
    districts = df['district'].unique()[:5]  # First 5 districts
    example_df = pd.DataFrame({
        'area_m2': [85, 60, 120, 45, 75]
    })
    
    for i, district in enumerate(districts):
        example_df[f'is_{district[:6]}'] = [1 if j == i else 0 for j in range(5)]
    
    st.dataframe(example_df)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 15px 0;">
        <b>💡 Key Insight</b><br>
        <span style="font-size: 13px;">
        Each district becomes its own column!<br>
        • Only ONE column has value 1 (the apartment's district)<br>
        • All other columns have value 0<br><br>
        This is called "One-Hot" because only ONE value is "hot" (=1)!
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Show the code
    st.markdown("### 📝 The Code")
    
    st.code("""
from sklearn.preprocessing import OneHotEncoder

# Create encoder
encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')

# Fit and transform district column
district_encoded = encoder.fit_transform(df[['district']])

# Now district_encoded is a 2D array with 0s and 1s
# Each column represents one district
""", language='python')
    
    with st.expander("🤔 Why not just use numbers like Gangnam=1, Seocho=2, Nowon=3?"):
        st.markdown("""
        **Good question!** This is called "Label Encoding" and it has a problem:
        
        If Gangnam=1, Seocho=2, Nowon=3:
        - The model thinks: Nowon (3) > Seocho (2) > Gangnam (1)
        - It implies an ORDER that doesn't exist!
        
        **One-Hot Encoding treats all districts equally** - no artificial ordering.
        
        The model learns EACH district's effect on price independently!
        """)


def display_training_process(df: pd.DataFrame) -> None:
    """Show training process with code and visualization."""
    st.header("🎓 Step 3: Training")
    
    st.markdown("""
    Let's train the model with Area + District!
    """)
    
    # Show the training code
    st.markdown("### 📝 The Training Code")
    
    st.code("""
# Step 1: Prepare features
X_area = df[['area_m2']]  # Area column

# Step 2: One-Hot Encode districts
encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
X_district = encoder.fit_transform(df[['district']])

# Step 3: Combine features
X = np.hstack([X_area, X_district])  # Combine horizontally

# Step 4: Target
y = df['price_10k_krw']

# Step 5: Train!
model = LinearRegression()
model.fit(X, y)

# Step 6: Get results
w_area = model.coef_[0]                    # Weight for area
w_districts = model.coef_[1:]              # Weights for each district
b = model.intercept_                        # Bias
""", language='python')
    
    with st.expander("🧩 What is np.hstack? (Beginner Tip)"):
        st.markdown("""
        **hstack = Horizontal Stack (Paste Side-by-Side)**
        
        We have two separate pieces of information:
        1. **Area** (a vertical column)
        2. **District Info** (many vertical columns from One-Hot Encoding)
        
        The model needs **ONE big spreadsheet** to learn from.
        `np.hstack` takes these separate columns and glues them together **horizontally** to make one wide table.
        
        *   **Before**: `[Area]`    and    `[Is_Gangnam, Is_Seocho...]`
        *   **After**: `[Area, Is_Gangnam, Is_Seocho...]` (All in one row!)
        """)
    
    st.info("""
    **What the model learns:**
    
    Price = (w_area × Area) + (w_gangnam × Is_Gangnam) + (w_seocho × Is_Seocho) + ... + b
    
    Each district gets its own weight! Expensive districts get higher weights.
    """)


@st.cache_resource
def train_model(df: pd.DataFrame):
    """Train the Level 3 model."""
    # Prepare data
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    X_district = encoder.fit_transform(df[['district']])
    X_area = df[['area_m2']].values
    X = np.hstack([X_area, X_district])
    y = df['price_10k_krw'].values
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE
    )
    
    # Train
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    rmse_train = calculate_rmse(y_train, y_pred_train)
    rmse_test = calculate_rmse(y_test, y_pred_test)
    
    # Get district names
    district_names = encoder.categories_[0]
    
    return model, encoder, district_names, rmse_train, rmse_test, (y_test, y_pred_test)


def display_coefficient_analysis(model, district_names) -> None:
    """Visualize what the model learned."""
    st.header("📊 What Did the Model Learn?")
    
    st.markdown("""
    Let's look at the coefficients (weights) the model learned:
    """)
    
    # Area coefficient
    area_coef = model.coef_[0]
    district_coefs = model.coef_[1:]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Area Coefficient", f"{area_coef:,.1f}")
        st.caption(f"Each additional m² adds ~{area_coef:,.0f} (10k KRW) to price")
    
    with col2:
        st.metric("Bias (Base Price)", f"{model.intercept_:,.0f}")
        st.caption("Starting price before features")
    
    st.markdown("---")
    
    st.markdown("### 🏘️ District Coefficients")
    
    st.markdown("""
    **Interpretation**: Higher coefficient = More expensive district
    
    These values show the "premium" or "discount" for each district!
    """)
    
    # Sort by coefficient
    district_df = pd.DataFrame({
        'District': district_names,
        'Coefficient': district_coefs
    }).sort_values('Coefficient', ascending=True)
    
    # Map Korean to English for display
    district_name_map = {
        '강남구': 'Gangnam', '서초구': 'Seocho', '송파구': 'Songpa', '용산구': 'Yongsan',
        '성동구': 'Seongdong', '광진구': 'Gwangjin', '마포구': 'Mapo', '양천구': 'Yangcheon',
        '영등포구': 'Yeongdeungpo', '동작구': 'Dongjak', '종로구': 'Jongno', '중구': 'Jung',
        '서대문구': 'Seodaemun', '동대문구': 'Dongdaemun', '성북구': 'Seongbuk', '강동구': 'Gangdong',
        '강서구': 'Gangseo', '구로구': 'Guro', '금천구': 'Geumcheon', '관악구': 'Gwanak',
        '은평구': 'Eunpyeong', '노원구': 'Nowon', '도봉구': 'Dobong', '강북구': 'Gangbuk', 
        '중랑구': 'Jungnang'
    }
    district_df['District_En'] = district_df['District'].map(district_name_map).fillna(district_df['District'])
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 8))
    colors = ['#4CAF50' if c > 0 else '#F44336' for c in district_df['Coefficient']]
    bars = ax.barh(district_df['District_En'], district_df['Coefficient'], color=colors)
    ax.axvline(x=0, color='black', linewidth=0.5)
    ax.set_xlabel('Coefficient (Price Effect)')
    ax.set_title('District Price Effect (Green = Premium, Red = Discount)')
    ax.grid(True, alpha=0.3, axis='x')
    
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Insights
    top_3 = district_df.tail(3)['District_En'].tolist()
    bottom_3 = district_df.head(3)['District_En'].tolist()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
        <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px;">
            <b>🏆 Most Expensive Districts</b><br>
            1. {top_3[2]}<br>
            2. {top_3[1]}<br>
            3. {top_3[0]}
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px;">
            <b>💰 Most Affordable Districts</b><br>
            1. {bottom_3[0]}<br>
            2. {bottom_3[1]}<br>
            3. {bottom_3[2]}
        </div>
        """, unsafe_allow_html=True)


def display_evaluation(rmse_train: float, rmse_test: float, y_test, y_pred) -> None:
    """Show model performance."""
    st.header("📏 Model Performance")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Train RMSE", f"{rmse_train:,.0f}")
        st.caption("Error on training data")
    
    with col2:
        st.metric("Test RMSE", f"{rmse_test:,.0f}")
        st.caption("Error on new data")
    
    with col3:
        rel_err = rmse_test / np.mean(y_test) * 100
        st.metric("Relative Error", f"{rel_err:.1f}%")
    
    # Compare with Level 2
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 15px 0;">
        <b>📊 Comparison with Level 2</b><br>
        <span style="font-size: 13px;">
        Level 2 (Area only): RMSE ~ 42,000<br>
        Level 3 (Area + District): RMSE ~ {rmse_test:,.0f}<br><br>
        <b>Did adding District help? Check if Level 3 RMSE is lower!</b>
        </span>
    </div>
    """.format(rmse_test=rmse_test), unsafe_allow_html=True)
    
    # Actual vs Predicted
    st.markdown("### 📈 Actual vs Predicted")
    
    st.markdown("""
    Points closer to the red line = Better predictions!
    """)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    sample_idx = np.random.choice(len(y_test), min(1000, len(y_test)), replace=False)
    ax.scatter(y_test[sample_idx], y_pred[sample_idx], alpha=0.3, s=15, c='steelblue')
    max_val = max(y_test.max(), y_pred.max())
    ax.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Perfect prediction')
    ax.set_xlabel('Actual Price (10k KRW)')
    ax.set_ylabel('Predicted Price (10k KRW)')
    ax.set_title('Actual vs Predicted: Level 3')
    ax.legend()
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Compare with other levels
    st.markdown("---")
    display_rmse_comparison(3, rmse_test)


def display_demo(df: pd.DataFrame, model, encoder) -> None:
    """Interactive prediction demo."""
    st.header("🔮 Try It Yourself")
    
    st.markdown("""
    See how **Location (District)** changes the price for the same apartment size!
    """)
    
    # Get districts with English names
    districts = sorted(df['district'].unique())
    district_name_map = {
        '강남구': 'Gangnam', '서초구': 'Seocho', '송파구': 'Songpa', '용산구': 'Yongsan',
        '성동구': 'Seongdong', '광진구': 'Gwangjin', '마포구': 'Mapo', '양천구': 'Yangcheon',
        '영등포구': 'Yeongdeungpo', '동작구': 'Dongjak', '종로구': 'Jongno', '중구': 'Jung',
        '서대문구': 'Seodaemun', '동대문구': 'Dongdaemun', '성북구': 'Seongbuk', '강동구': 'Gangdong',
        '강서구': 'Gangseo', '구로구': 'Guro', '금천구': 'Geumcheon', '관악구': 'Gwanak',
        '은평구': 'Eunpyeong', '노원구': 'Nowon', '도봉구': 'Dobong', '강북구': 'Gangbuk', 
        '중랑구': 'Jungnang'
    }
    
    district_options = {district_name_map.get(d, d): d for d in districts}
    inv_district_options = {v: k for k, v in district_options.items()} # Korean -> English
    
    tab1, tab2 = st.tabs(["👆 Single Prediction", "⚔️ District Battle"])
    
    with tab1:
        st.subheader("Estimate Price")
        col1, col2 = st.columns(2)
        
        with col1:
            selected_en = st.selectbox("Select District", list(district_options.keys()))
            selected_district = district_options[selected_en]
        
        with col2:
            selected_area = st.slider("Apartment Area (m²)", 10, 200, 84, key="area_single")
        
        # Predict
        X_d = encoder.transform([[selected_district]])
        X_input = np.hstack([[selected_area], X_d[0]]).reshape(1, -1)
        price = model.predict(X_input)[0]
        
        st.success(f"""
        ### 💰 Predicted Price: {price:,.0f} (10k KRW)
        ≈ **{price/10000:.2f} Billion KRW**
        """)
    
    with tab2:
        st.subheader("Compare Two Districts")
        st.markdown("How much more expensive is District A vs District B?")
        
        c1, c2, c3 = st.columns([1, 0.2, 1])
        
        with c1:
            dist_a_en = st.selectbox("District A (Blue)", list(district_options.keys()), index=0)
            dist_a = district_options[dist_a_en]
            
        with c3:
            dist_b_en = st.selectbox("District B (Red)", list(district_options.keys()), index=21) # Default Nowon
            dist_b = district_options[dist_b_en]
            
        area_battle = st.slider("Area for both (m²)", 10, 200, 84, key="area_battle")
        
        # Predict A
        Xa_d = encoder.transform([[dist_a]])
        Xa_in = np.hstack([[area_battle], Xa_d[0]]).reshape(1, -1)
        price_a = model.predict(Xa_in)[0]
        
        # Predict B
        Xb_d = encoder.transform([[dist_b]])
        Xb_in = np.hstack([[area_battle], Xb_d[0]]).reshape(1, -1)
        price_b = model.predict(Xb_in)[0]
        
        # Visualize
        fig, ax = plt.subplots(figsize=(6, 4))
        bars = ax.bar([dist_a_en, dist_b_en], [price_a, price_b], color=['#2196F3', '#F44336'])
        
        # Add labels
        ax.bar_label(bars, fmt='{:,.0f}', padding=3)
        ax.set_ylabel("Price (10k KRW)")
        ax.set_title(f"Price Gap: {abs(price_a - price_b):,.0f} (10k KRW)")
        ax.set_ylim(0, max(price_a, price_b) * 1.2)
        
        st.pyplot(fig, use_container_width=True)
        
        diff = price_a - price_b
        if diff > 0:
            st.info(f"**{dist_a_en}** is **{diff/10000:.2f} Billion KRW** more expensive than {dist_b_en}!")
        elif diff < 0:
            st.info(f"**{dist_b_en}** is **{abs(diff)/10000:.2f} Billion KRW** more expensive than {dist_a_en}!")
        else:
            st.info("Same price!")


def display_limitations() -> None:
    """Show limitations of Level 3."""
    st.header("🤔 Questions You Might Have")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q1: What about building age?</b><br>
        <span style="font-size: 13px;">
        A 20-year-old apartment and a brand new one in the same district 
        still get the same price prediction. That doesn't seem right!<br>
        <i>→ Level 4 adds Building Year!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q2: What about floor number?</b><br>
        <span style="font-size: 13px;">
        Higher floors often have better views and cost more. But we're not using that information!<br>
        <i>→ We'll add more features later!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q3: How do we visualize 3+ dimensions?</b><br>
        <span style="font-size: 13px;">
        With Area alone (Level 2), we drew a 2D scatter plot.<br>
        With Area + District (Level 3), visualization is getting harder.<br>
        <i>→ Level 4 explores 3D visualization!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
    ### 🚀 What's Next in Level 4?
    
    | This Level (3) | Next Level (4) |
    |----------------|----------------|
    | Area + District | Area + District + Building Year |
    | 2D visualization | 3D visualization with Plotly |
    | Can't see 3rd dimension | Can rotate and explore 3D space! |
    
    Ready to add another dimension? Let's go to Level 4! →
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_why_level3()
        st.markdown("---")
        display_onehot_explanation(df)
        st.markdown("---")
        display_training_process(df)
        st.markdown("---")
        
        # Train model
        with st.spinner("Training Level 3 model..."):
            model, encoder, district_names, rmse_train, rmse_test, (y_test, y_pred) = train_model(df)
        
        display_coefficient_analysis(model, district_names)
        st.markdown("---")
        display_evaluation(rmse_train, rmse_test, y_test, y_pred)
        st.markdown("---")
        display_demo(df, model, encoder)
        st.markdown("---")
        display_limitations()
        
        # Code Link
        
        display_code_link("Level_3_Multi_Features.ipynb")
        
        
        
        # Next level teaser
        display_next_level_teaser(3)
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
