# -*- coding: utf-8 -*-
"""
MLOps Level 9: Monitoring & Docker
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from src.mlops_utils import get_architect_note
from src.navigation import display_mlops_sidebar

def display_docker_concept():
    st.header("1. Docker: 'It works on my machine'")
    st.markdown("We package code + libraries + OS into a **Container**.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.code("""
# Dockerfile
FROM python:3.9
COPY . /app
RUN pip install -r requirements.txt
CMD ["python", "app.py"]
        """, language="dockerfile")
        
    with col2:
        st.info("""
        **Benefits**:
        - Isolated Environment
        - Same on Laptop & Cloud
        - Easy Scaling (Kubernetes)
        """)

def simulate_monitoring():
    st.header("2. Monitoring: Is the model dying?")
    st.markdown("**Data Drift**: When real-world data changes, model accuracy drops.")
    
    # Simulate drift
    dates = pd.date_range(start="2024-01-01", periods=30)
    baseline_acc = [0.9] * 20 # Stable
    drift_acc = [0.9 - (i * 0.05) for i in range(10)] # Crashing
    
    accuracy = baseline_acc + drift_acc
    
    df = pd.DataFrame({"Date": dates, "Accuracy": accuracy})
    
    # Plot
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['Date'], y=df['Accuracy'], mode='lines+markers', name='Model Accuracy'))
    fig.add_hline(y=0.8, line_dash="dash", line_color="red", annotation_text="Threshold (0.8)")
    
    st.plotly_chart(fig, use_container_width=True)
    
    current_acc = accuracy[-1]
    if current_acc < 0.8:
        st.error(f"🚨 **ALERT**: Model Accuracy ({current_acc:.2f}) is below threshold!")
        if st.button("♻️ Retrain Model"):
            with st.spinner("Triggering Airflow Pipeline..."):
                import time
                time.sleep(2)
            st.success("Retraining initiated! Accuracy should recover.")
    else:
        st.success("✅ System Healthy")

def main():
    display_mlops_sidebar(19)
    
    st.title("🐳 Level 19: Monitoring & Docker")
    st.markdown("**'Deploy & Watch'**")
    
    display_docker_concept()
    st.markdown("---")
    simulate_monitoring()
    
    st.markdown("---")
    st.markdown(get_architect_note(19))

if __name__ == "__main__":
    main()
