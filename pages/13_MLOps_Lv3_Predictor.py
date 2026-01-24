# -*- coding: utf-8 -*-
"""
MLOps Level 3: Today's Predictor
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sklearn.ensemble import RandomForestRegressor
from src.mlops_utils import generate_seongsu_data, get_architect_note
from src.navigation import display_mlops_sidebar

def train_backend_model(df):
    """Hidden backend training."""
    X = df[['temp_c', 'rain_mm', 'is_holiday']]
    y = df['passengers']
    model = RandomForestRegressor(n_estimators=50, random_state=42)
    model.fit(X, y)
    return model

def display_dashboard(model):
    st.header("📲 Today's Forecast Dashboard")
    st.markdown("Imagine this is what the Station Manager sees every morning.")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        temp = st.slider("🌡️ Temperature (C)", -15.0, 40.0, 20.0)
    with col2:
        rain = st.slider("☔ Rain (mm)", 0.0, 100.0, 0.0)
    with col3:
        is_holiday = st.checkbox("🎉 Weekend/Holiday?")
        
    # Predict
    input_data = pd.DataFrame({
        'temp_c': [temp],
        'rain_mm': [rain],
        'is_holiday': [int(is_holiday)]
    })
    
    pred = model.predict(input_data)[0]
    
    # Gauge Chart
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = pred,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Predicted Passengers"},
        gauge = {
            'axis': {'range': [10000, 70000]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [10000, 30000], 'color': "lightgreen"},
                {'range': [30000, 50000], 'color': "yellow"},
                {'range': [50000, 70000], 'color': "red"}],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 60000}
        }
    ))
    
    st.plotly_chart(fig, use_container_width=True)
    
    if pred > 60000:
        st.error("🚨 **CROWD WARNING**: Deploy extra safety personnel!")
    elif pred > 40000:
        st.warning("⚠️ **BUSY**: Standard operation.")
    else:
        st.success("✅ **QUIET**: Good day for maintenance.")

def main():
    display_mlops_sidebar(13)
    
    st.title("📱 Level 13: Today's Predictor")
    st.markdown("**'The AI Product'**")
    
    if 'mlops_df' not in st.session_state:
        st.session_state['mlops_df'] = generate_seongsu_data()
    df = st.session_state['mlops_df']
    
    # Train model silently
    model = train_backend_model(df)
    
    display_dashboard(model)
    
    st.markdown("---")
    st.markdown(get_architect_note(13))

if __name__ == "__main__":
    main()
