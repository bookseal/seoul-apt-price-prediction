# -*- coding: utf-8 -*-
"""
MLOps Level 1: Data Creation
"""
import streamlit as st
import pandas as pd
from src.mlops_utils import generate_seongsu_data, get_architect_note
from src.navigation import display_mlops_sidebar

def display_raw_data_view():
    st.subheader("1. Raw Data Sources (Simulation)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### 🚇 Seoul Metro Server (JSON)")
        st.code("""
{
    "station": "Seongsu",
    "date": "2024-01-01",
    "exit_count": 34102,
    "entry_count": 32001
}, ...
        """, language="json")
        
    with col2:
        st.markdown("##### 🌤️ Weather API (JSON)")
        st.code("""
{
    "loc": "Seoul",
    "date": "2024-01-01",
    "temp": 2.5,
    "rain": 0.0,
    "condition": "Cloudy"
}, ...
        """, language="json")

def display_merged_data(df):
    st.subheader("2. Merged Data (The 'Gold' Table)")
    st.markdown("We combine these sources into a single DataFrame for training.")
    
    st.dataframe(df.head(10), use_container_width=True)
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Rows", len(df))
    col2.metric("Date Range", f"{df['date'].min().date()} ~ {df['date'].max().date()}")
    col3.metric("Avg Passengers", f"{int(df['passengers'].mean()):,}")

def main():
    display_mlops_sidebar(11) # Sidebar Navigation
    
    st.title("🏗️ Level 11: Data Creation")
    st.markdown("**'Where does the data come from?'**")
    
    st.info("""
    Before AI, there is Data Engineering.
    We need to collect data from **multiple sources** (Subway, Weather, Calendar) 
    and merge them into a single dataset.
    """)
    
    # 1. Raw View
    display_raw_data_view()
    
    st.markdown("---")
    
    # 2. Generate Data
    if 'mlops_df' not in st.session_state:
        st.session_state['mlops_df'] = generate_seongsu_data()
        
    df = st.session_state['mlops_df']
    display_merged_data(df)
    
    st.markdown("---")
    st.markdown(get_architect_note(11))

if __name__ == "__main__":
    main()
