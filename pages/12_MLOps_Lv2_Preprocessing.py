# -*- coding: utf-8 -*-
"""
MLOps Level 2: Preprocessing
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from src.mlops_utils import generate_seongsu_data, get_architect_note
from src.navigation import display_mlops_sidebar

def display_scaling_demo(df):
    st.header("1. Scaling: Making Numbers Comparable")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Temp (C)**: -10 ~ 35")
    with col2:
        st.markdown("**Rain (mm)**: 0 ~ 100")
        
    st.info("Machine Learning models get confused if one number is huge (100) and another is small (1). We scale them!")
    
    # Scaling
    scaler_std = StandardScaler()
    scaler_mm = MinMaxScaler()
    
    df_scale = df[['temp_c', 'rain_mm']].copy()
    df_scale['temp_std'] = scaler_std.fit_transform(df[['temp_c']])
    df_scale['rain_std'] = scaler_std.fit_transform(df[['rain_mm']])
    
    # Tabs
    tab1, tab2 = st.tabs(["Raw Data Distribution", "Scaled Distribution"])
    
    with tab1:
        st.write("Notice the different ranges on X/Y axis.")
        fig = px.scatter(df_scale, x='temp_c', y='rain_mm', title="Raw Data", color_discrete_sequence=['blue'])
        st.plotly_chart(fig, use_container_width=True)
        
    with tab2:
        st.write("Notice both are centered around 0 (StandardScaler).")
        fig = px.scatter(df_scale, x='temp_std', y='rain_std', title="Scaled Data (Standard)", color_discrete_sequence=['green'])
        st.plotly_chart(fig, use_container_width=True)

def display_encoding_demo(df):
    st.header("2. Encoding: Text to Numbers")
    st.markdown("Computers only understand numbers. How do we extract 'Weekend' info?")
    
    # Feature Engineering logic
    df_enc = df.head(5).copy()
    df_enc['day_name'] = df_enc['date'].dt.day_name()
    df_enc['is_weekend'] = df_enc['date'].dt.weekday >= 5
    
    st.markdown("##### Raw Date")
    st.dataframe(df_enc[['date', 'day_name']], use_container_width=True)
    
    st.markdown("⬇️ **Transformation**")
    
    st.markdown("##### Processed Features")
    st.dataframe(df_enc[['date', 'is_weekend']].replace({True: 1, False: 0}), use_container_width=True)

def main():
    display_mlops_sidebar(12)
    
    st.title("⚙️ Level 12: Preprocessing")
    st.markdown("**'Making data model-ready'**")
    
    if 'mlops_df' not in st.session_state:
        st.session_state['mlops_df'] = generate_seongsu_data()
    df = st.session_state['mlops_df']
    
    display_scaling_demo(df)
    st.markdown("---")
    display_encoding_demo(df)
    
    st.markdown("---")
    st.markdown(get_architect_note(12))

if __name__ == "__main__":
    main()
