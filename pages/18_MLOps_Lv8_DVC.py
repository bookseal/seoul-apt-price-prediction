# -*- coding: utf-8 -*-
"""
MLOps Level 8: DVC (Data Versioning)
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from src.mlops_utils import generate_seongsu_data, get_architect_note
from src.navigation import display_mlops_sidebar

def get_dataset(version):
    """Simulate dataset versions."""
    df = generate_seongsu_data()
    
    if version == "v1 (Clean)":
        return df # Original
    elif version == "v2 (Corrupted)":
        # Simulate data corruption (e.g., Sensor error sets temp to 0)
        df.loc[df['temp_c'] > 0, 'temp_c'] = 0 
        return df
    elif version == "v3 (Drifted)":
        # Simulate drift (e.g., Covid restrictions, less passengers)
        df['passengers'] = df['passengers'] * 0.5
        return df
    return df

def train_and_eval(df):
    model = LinearRegression()
    # Simplified train on full data for demo
    X = df[['temp_c', 'rain_mm', 'is_holiday']]
    y = df['passengers']
    model.fit(X, y)
    rmse = mean_squared_error(y, model.predict(X), squared=False)
    return rmse

def display_dvc_demo():
    st.header("1. Data Version Control (DVC)")
    st.markdown("Code has Git. Data has DVC. What happens if data changes?")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        version = st.radio("Select Data Version", ["v1 (Clean)", "v2 (Corrupted)", "v3 (Drifted)"])
        
    df = get_dataset(version)
    rmse = train_and_eval(df)
    
    with col2:
        st.subheader(f"Model Training on {version}")
        if version == "v1 (Clean)":
            st.success(f"✅ RMSE: {rmse:,.0f} (Normal)")
        elif version == "v2 (Corrupted)":
            st.error(f"❌ RMSE: {rmse:,.0f} (High Error!)")
            st.caption("Data corruption detected! We need to rollback to v1.")
        else:
            st.warning(f"⚠️ RMSE: {rmse:,.0f} (Concept Drift)")
            
    # Visualize Data Distribution
    fig = px.scatter(df, x='temp_c', y='passengers', title=f"Temp vs Passengers ({version})")
    st.plotly_chart(fig, use_container_width=True)
    
    if version != "v1 (Clean)":
        if st.button("⬅️ DVC Checkout v1.dvc"):
            st.toast("Restoring data from S3 bucket...")
            st.success("Data restored to v1! Model is safe.")

def main():
    display_mlops_sidebar(18)
    
    st.title("💾 Level 18: DVC (Data Versioning)")
    st.markdown("**'Time Machine for Data'**")
    
    display_dvc_demo()
    
    st.markdown("---")
    st.markdown(get_architect_note(18))

if __name__ == "__main__":
    main()
