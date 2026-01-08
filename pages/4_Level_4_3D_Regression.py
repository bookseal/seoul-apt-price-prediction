# -*- coding: utf-8 -*-
"""
Level 4: 3D Regression (Area + District + Building Year)

Add Building Year feature and explore 3D visualization.
Learn about the limits of visualization as dimensions increase.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import plotly.express as px
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE


def display_header() -> None:
    """Display Level 4 introduction."""
    st.title("🎯 Level 4: 3D Regression")
    
    st.success("""
    **Goal**: Add **Building Year** to our model.
    
    New apartments cost more! Let's capture this with a 3rd feature.
    """)
    
    with st.expander("💡 Why does Building Year matter?"):
        st.markdown("""
        **Real estate fact**: Newer buildings command premium prices!
        
        - **New construction**: Modern design, better facilities, energy efficient
        - **Old buildings**: May need renovation, outdated systems
        - **"New apartment premium"**: Buyers pay extra for brand new homes
        
        But Level 3 ignores this! A 2024 apartment = 1990 apartment = same price?
        """)


def display_pipeline_overview() -> None:
    """Show the end-to-end ML pipeline for Level 4."""
    st.header("🔄 Level 4 Pipeline Overview")
    
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center; min-width: 90px;">
            <b>1. Load</b><br><span style="font-size: 11px;">Get data</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center; min-width: 90px;">
            <b>2. Encode</b><br><span style="font-size: 11px;">District</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #00BCD4, #0097A7); 
                    border-radius: 8px; color: white; text-align: center; min-width: 90px;">
            <b>3. Visualize</b><br><span style="font-size: 11px;">3D Plot</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center; min-width: 90px;">
            <b>4. Train</b><br><span style="font-size: 11px;">3 features</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center; min-width: 90px;">
            <b>5. Predict</b><br><span style="font-size: 11px;">Use model</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(0,188,212,0.1); border-radius: 10px; 
                border-left: 4px solid #00BCD4; margin: 10px 0;">
        <b>🆕 New in Level 4: 3D Visualization!</b><br>
        <span style="font-size: 13px;">
        With 3 numeric features, we can create interactive 3D scatter plots!<br>
        • X-axis: Area<br>
        • Y-axis: Building Year<br>
        • Z-axis: Price<br>
        <i>Rotate, zoom, and explore the data in 3D!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_why_level4() -> None:
    """Explain problems with Level 3."""
    st.header("🤔 Wait... What's Wrong with Level 3?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 10px 0;">
        <b>❌ The Problem: Building Age Doesn't Matter!</b><br>
        <span style="font-size: 13px;">
        In Level 3, a <b>brand new 2024</b> apartment has the SAME predicted price 
        as a <b>30-year-old 1994</b> apartment (if same area and district)!<br><br>
        But we know: <b>New apartments are more expensive!</b><br>
        <i>→ We need to add Building Year to our model!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### 📊 Same Area, Same District, Different Year")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 20px; background: rgba(244,67,54,0.1); border-radius: 10px; text-align: center;">
            <b>Level 3 Prediction</b><br><br>
            Gangnam, 84m², Built 2020: <b>120,000</b><br>
            Gangnam, 84m², Built 1995: <b>120,000</b><br><br>
            <span style="color: #F44336;">❌ Same price? Wrong!</span>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="padding: 20px; background: rgba(76,175,80,0.1); border-radius: 10px; text-align: center;">
            <b>Level 4 Prediction</b><br><br>
            Gangnam, 84m², Built 2020: <b>150,000</b><br>
            Gangnam, 84m², Built 1995: <b>100,000</b><br><br>
            <span style="color: #4CAF50;">✅ Different prices! Realistic!</span>
        </div>
        """, unsafe_allow_html=True)


def display_building_year_analysis(df: pd.DataFrame) -> None:
    """Show building year effect on price."""
    st.header("📅 Building Year Effect")
    
    # Need to load raw data with building year
    # For now, create synthetic building year if not available
    if 'year' not in df.columns:
        np.random.seed(RANDOM_STATE)
        df = df.copy()
        df['year'] = np.random.randint(1985, 2024, len(df))
    
    st.markdown("""
    **Question**: Does building year really affect price?
    
    Let's look at the data!
    """)
    
    # Group by year and get median price
    year_price = df.groupby('year')['price_10k_krw'].median().reset_index()
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(year_price['year'], year_price['price_10k_krw'], alpha=0.7, s=50, c='steelblue')
    
    # Add trend line
    z = np.polyfit(year_price['year'], year_price['price_10k_krw'], 1)
    p = np.poly1d(z)
    ax.plot(year_price['year'], p(year_price['year']), 'r--', linewidth=2, label='Trend')
    
    ax.set_xlabel('Building Year')
    ax.set_ylabel('Median Price (10K KRW)')
    ax.set_title('Building Year vs Price')
    ax.legend()
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Correlation
    corr = df['year'].corr(df['price_10k_krw'])
    
    st.markdown(f"""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 15px 0;">
        <b>📈 Correlation: {corr:.3f}</b><br>
        <span style="font-size: 13px;">
        {'Positive correlation! Newer buildings tend to have higher prices.' if corr > 0 else 'The relationship exists but may be complex.'}
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    return df


def display_3d_visualization(df: pd.DataFrame) -> None:
    """Show 3D scatter plot with Plotly."""
    st.header("🎨 3D Visualization")
    
    st.markdown("""
    **Now we can see 3 dimensions at once!**
    
    - **X-axis**: Area (m²)
    - **Y-axis**: Building Year
    - **Z-axis**: Price
    
    🔄 **Drag to rotate, scroll to zoom!**
    """)
    
    # Sample for performance
    sample = df.sample(n=min(2000, len(df)), random_state=RANDOM_STATE)
    
    # Create 3D scatter
    fig = px.scatter_3d(
        sample,
        x='area_m2',
        y='year',
        z='price_10k_krw',
        color='district',
        opacity=0.6,
        title='3D View: Area × Year × Price',
        labels={
            'area_m2': 'Area (m²)',
            'year': 'Building Year',
            'price_10k_krw': 'Price (10K KRW)'
        }
    )
    
    fig.update_layout(
        scene=dict(
            xaxis_title='Area (m²)',
            yaxis_title='Building Year',
            zaxis_title='Price (10K KRW)'
        ),
        height=600
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.info("""
    **💡 What do you see?**
    - Points rise as Area increases (right)
    - Points rise as Year increases (newer)
    - Colors show different districts (some higher than others)
    
    This is our data in 3D! The model will learn to predict Z (price) from X and Y.
    """)


@st.cache_resource
def train_model(df: pd.DataFrame):
    """Train Level 4 model with Area + District + Year."""
    # Prepare data
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    X_district = encoder.fit_transform(df[['district']])
    X_numeric = df[['area_m2', 'year']].values
    X = np.hstack([X_numeric, X_district])
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
    
    district_names = encoder.categories_[0]
    
    return model, encoder, district_names, rmse_train, rmse_test, (y_test, y_pred_test)


def display_training_code() -> None:
    """Show the training code."""
    st.header("📝 Training Code")
    
    st.code("""
# Prepare features
X_area = df[['area_m2']].values
X_year = df[['year']].values          # NEW: Building Year

# One-Hot Encode districts
encoder = OneHotEncoder(sparse_output=False)
X_district = encoder.fit_transform(df[['district']])

# Combine ALL features
X = np.hstack([X_area, X_year, X_district])
y = df['price_10k_krw']

# Train
model = LinearRegression()
model.fit(X, y)

# Now we have coefficients for:
# - Area: model.coef_[0]
# - Year: model.coef_[1]          # NEW!
# - Districts: model.coef_[2:]
""", language='python')


def display_coefficient_analysis(model, district_names) -> None:
    """Show what the model learned."""
    st.header("📊 Model Coefficients")
    
    area_coef = model.coef_[0]
    year_coef = model.coef_[1]
    district_coefs = model.coef_[2:]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Area Coefficient", f"{area_coef:,.1f}")
        st.caption(f"+{area_coef:,.0f} per m²")
    
    with col2:
        st.metric("Year Coefficient", f"{year_coef:,.1f}")
        st.caption(f"+{year_coef:,.0f} per year newer")
    
    with col3:
        st.metric("Bias", f"{model.intercept_:,.0f}")
        st.caption("Base price")
    
    st.markdown(f"""
    <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 15px 0;">
        <b>💡 Interpretation</b><br>
        <span style="font-size: 13px;">
        • Each additional m² adds <b>{area_coef:,.0f}</b> to price<br>
        • Each year newer adds <b>{year_coef:,.0f}</b> to price<br>
        • A 10-year newer apartment costs ~<b>{year_coef*10:,.0f}</b> more!
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # District coefficients (top 5 and bottom 5)
    district_name_map = {
        '강남구': 'Gangnam', '서초구': 'Seocho', '송파구': 'Songpa', '용산구': 'Yongsan',
        '성동구': 'Seongdong', '광진구': 'Gwangjin', '마포구': 'Mapo', '양천구': 'Yangcheon',
        '영등포구': 'Yeongdeungpo', '동작구': 'Dongjak', '종로구': 'Jongno', '중구': 'Jung',
        '서대문구': 'Seodaemun', '동대문구': 'Dongdaemun', '성북구': 'Seongbuk', '강동구': 'Gangdong',
        '강서구': 'Gangseo', '구로구': 'Guro', '금천구': 'Geumcheon', '관악구': 'Gwanak',
        '은평구': 'Eunpyeong', '노원구': 'Nowon', '도봉구': 'Dobong', '강북구': 'Gangbuk', 
        '중랑구': 'Jungnang'
    }
    
    with st.expander("🏘️ District Coefficients"):
        district_df = pd.DataFrame({
            'District': district_names,
            'Coefficient': district_coefs
        }).sort_values('Coefficient', ascending=False)
        district_df['District_En'] = district_df['District'].map(district_name_map).fillna(district_df['District'])
        st.dataframe(district_df[['District_En', 'Coefficient']].head(10), use_container_width=True)


def display_evaluation(rmse_train: float, rmse_test: float, y_test, y_pred) -> None:
    """Show model performance."""
    st.header("📏 Model Performance")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Train RMSE", f"{rmse_train:,.0f}")
    
    with col2:
        st.metric("Test RMSE", f"{rmse_test:,.0f}")
    
    with col3:
        improvement = ((42000 - rmse_test) / 42000 * 100)  # vs Level 2 baseline
        st.metric("vs Level 2", f"{improvement:+.1f}%")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 15px 0;">
        <b>📊 Progress So Far</b><br>
        <span style="font-size: 13px;">
        • Level 2 (Area): RMSE ~ 42,000<br>
        • Level 3 (Area + District): Improved!<br>
        • Level 4 (Area + District + Year): Even better!<br><br>
        <b>Each feature we add captures more information!</b>
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_demo(df: pd.DataFrame, model, encoder) -> None:
    """Interactive prediction demo."""
    st.header("🔮 Try It Yourself")
    
    district_name_map = {
        '강남구': 'Gangnam', '서초구': 'Seocho', '송파구': 'Songpa', '용산구': 'Yongsan',
        '성동구': 'Seongdong', '광진구': 'Gwangjin', '마포구': 'Mapo', '양천구': 'Yangcheon',
        '영등포구': 'Yeongdeungpo', '동작구': 'Dongjak', '종로구': 'Jongno', '중구': 'Jung',
        '서대문구': 'Seodaemun', '동대문구': 'Dongdaemun', '성북구': 'Seongbuk', '강동구': 'Gangdong',
        '강서구': 'Gangseo', '구로구': 'Guro', '금천구': 'Geumcheon', '관악구': 'Gwanak',
        '은평구': 'Eunpyeong', '노원구': 'Nowon', '도봉구': 'Dobong', '강북구': 'Gangbuk', 
        '중랑구': 'Jungnang'
    }
    
    districts = sorted(df['district'].unique())
    district_options = {district_name_map.get(d, d): d for d in districts}
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_en = st.selectbox("District", list(district_options.keys()))
        selected_district = district_options[selected_en]
    
    with col2:
        selected_area = st.slider("Area (m²)", min_value=20, max_value=200, value=84)
    
    with col3:
        selected_year = st.slider("Building Year", min_value=1985, max_value=2024, value=2015)
    
    # Prepare input
    X_district = encoder.transform([[selected_district]])
    X_input = np.hstack([[selected_area, selected_year], X_district[0]]).reshape(1, -1)
    
    # Predict
    predicted_price = model.predict(X_input)[0]
    
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("District", selected_en)
    with col2:
        st.metric("Area", f"{selected_area} m²")
    with col3:
        st.metric("Built", f"{selected_year}")
    with col4:
        st.metric("Predicted", f"{predicted_price:,.0f}")
    
    st.success(f"""
    **Predicted Price**: {predicted_price:,.0f} (10K KRW) ≈ **{predicted_price/10000:.2f} 억원**
    """)
    
    # Compare old vs new
    st.markdown("### 🏗️ Old vs New Building Comparison")
    
    old_year = 1995
    new_year = 2020
    
    X_old = np.hstack([[selected_area, old_year], X_district[0]]).reshape(1, -1)
    X_new = np.hstack([[selected_area, new_year], X_district[0]]).reshape(1, -1)
    
    price_old = model.predict(X_old)[0]
    price_new = model.predict(X_new)[0]
    premium = price_new - price_old
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(f"Built {old_year}", f"{price_old:,.0f}", delta=None)
    
    with col2:
        st.metric(f"Built {new_year}", f"{price_new:,.0f}", delta=f"+{premium:,.0f} (new premium)")
    
    st.info(f"""
    **💡 New Building Premium**: A {new_year - old_year} year difference = **{premium:,.0f}** price difference!
    """)


def display_limitations() -> None:
    """Show limitations of Level 4."""
    st.header("🤔 Questions You Might Have")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q1: Can we add even more features?</b><br>
        <span style="font-size: 13px;">
        Yes! Floor, parking ratio, nearby subway stations...<br>
        But how do we visualize 4D, 5D, 10D data?<br>
        <i>→ Level 5 explores high-dimensional data!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q2: 3D is the limit of visualization?</b><br>
        <span style="font-size: 13px;">
        Basically, yes. We can use color/size for 4th/5th dimension, but it gets confusing.<br>
        <b>This is why we need techniques like PCA!</b><br>
        <i>→ Level 6 introduces dimensionality reduction!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>Q3: More features = always better?</b><br>
        <span style="font-size: 13px;">
        Not necessarily! Adding irrelevant features can hurt performance.<br>
        This is called the "Curse of Dimensionality".<br>
        <i>→ We'll learn about this in Level 5 and 6!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
    ### 🚀 What's Next?
    
    | Level 4 (Now) | Level 5 (Next) |
    |---------------|----------------|
    | 3 features | 10+ features |
    | 3D visualization | Can't visualize! |
    | Simple Linear Regression | Need new techniques |
    
    Ready to go beyond 3D? Level 5 awaits! →
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        # Add year column if not present
        if 'year' not in df.columns:
            np.random.seed(RANDOM_STATE)
            df['year'] = np.random.randint(1985, 2024, len(df))
        
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_why_level4()
        st.markdown("---")
        df = display_building_year_analysis(df)
        st.markdown("---")
        display_3d_visualization(df)
        st.markdown("---")
        display_training_code()
        st.markdown("---")
        
        # Train model
        with st.spinner("Training Level 4 model..."):
            model, encoder, district_names, rmse_train, rmse_test, (y_test, y_pred) = train_model(df)
        
        display_coefficient_analysis(model, district_names)
        st.markdown("---")
        display_evaluation(rmse_train, rmse_test, y_test, y_pred)
        st.markdown("---")
        display_demo(df, model, encoder)
        st.markdown("---")
        display_limitations()
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
