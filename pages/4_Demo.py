import streamlit as st
import pandas as pd
from src.io import load_sample_dataset

def display_demo_header() -> None:
    """
    Render the page title and the purpose of the interactive demo.

    Visual Guide:
    [ User Input ] -> [ Mock/ML Inference ] -> [ Predicted Price ]
    """
    st.title("🏠 Demo: Real-time Price Predictor")
    st.markdown("""
    Experience how AI estimates apartment prices! 
    Enter the details of an apartment below, and our model will calculate its estimated value.
    *(Note: This is a prototype version for demonstration purposes.)*
    """)
    
def get_user_inputs(df: pd.DataFrame) -> dict:
    """
    Collect apartment features from the user via Streamlit widgets.

    Process:
    - Select District (from dataset)
    - Input Area (m2)
    - Select Floor & Built Year
    """
    st.sidebar.header("Input Features")
    
    # 1. District Selection
    districts = sorted(df['district'].unique())
    selected_district = st.sidebar.selectbox("📍 Select District (구)", districts)
    
    # 2. Area Input
    selected_area = st.sidebar.number_input("📏 Exclusive Area (전용면적 ㎡)", min_value=10.0, max_value=300.0, value=84.0)
    
    # 3. Floor Slider
    selected_floor = st.sidebar.slider("🏢 Floor (층)", min_value=-1, max_value=70, value=10)
    
    # 4. Built Year Input
    selected_year = st.sidebar.number_input("🏗️ Built Year (건축년도)", min_value=1960, max_value=2026, value=2010)
    
    return {
        "district": selected_district,
        "area": selected_area,
        "floor": selected_floor,
        "built_year": selected_year
    }

def mock_inference(inputs: dict, df: pd.DataFrame) -> float:
    """
    Calculate a heuristic price based on median district prices.
    (Placeholder until the XGBoost model is fully integrated)

    Logic:
    Estimated Price = Median_Price_per_m2(District) * Selected_Area
    """
    # Calculate median price per m2 for the selected district
    df['price_per_m2'] = df['price_10k_krw'] / df['area_m2']
    median_unit_price = df[df['district'] == inputs['district']]['price_per_m2'].median()
    
    # Basic prediction logic (Heuristic)
    prediction = median_unit_price * inputs['area']
    return prediction

def render_demo_page() -> None:
    """
    Orchestrate the Demo page rendering and logic.
    """
    try:
        df = load_sample_dataset()
        display_demo_header()
        
        # 1. Get Inputs
        user_data = get_user_inputs(df)
        
        # 2. Prediction UI
        st.subheader("🔮 Prediction Result")
        
        # 3. Calculation (Mock Inference)
        estimated_price = mock_inference(user_data, df)
        
        # 4. Result Display
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Estimated Price", f"{int(estimated_price):,} 만원")
        with col2:
            st.write(f"**District:** {user_data['district']}")
            st.write(f"**Area:** {user_data['area']} ㎡")
            
        st.success(f"Based on current data, an apartment in {user_data['district']} is estimated at approx. {int(estimated_price/10000)} billion KRW.")
        
    except Exception as e:
        st.error(f"Please prepare the data first: {e}")

if __name__ == "__main__":
    render_demo_page()