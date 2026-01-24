# -*- coding: utf-8 -*-
"""
MLOps Level 7: MLflow (Experiment Tracking)
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from src.mlops_utils import get_architect_note
from src.navigation import display_mlops_sidebar

def generate_experiments():
    # Simulate 5 runs
    np.random.seed(42)
    runs = []
    
    for i in range(1, 11):
        # Parameters
        n_estimators = np.random.choice([50, 100, 200])
        max_depth = np.random.choice([5, 10, 20, None])
        lr = np.random.choice([0.01, 0.1, 0.2])
        
        # Metric simulation (Higher depth/estimators -> generally lower RMSE but risk of overfitting)
        rmse_base = 5000
        rmse = rmse_base - (n_estimators * 2) - (0 if max_depth is None else max_depth * 50) 
        rmse += np.random.normal(0, 100)
        
        runs.append({
            "Run ID": f"run_{i:02d}",
            "n_estimators": n_estimators,
            "max_depth": str(max_depth),
            "learning_rate": lr,
            "RMSE": rmse
        })
        
    return pd.DataFrame(runs)

def display_mlflow_dashboard(df):
    st.header("1. MLflow Dashboard Simulator")
    st.markdown("Tracks every experiment's parameters and metrics.")
    
    st.dataframe(df, use_container_width=True)
    
    best_run = df.loc[df['RMSE'].idxmin()]
    st.success(f"🏆 Best Run: **{best_run['Run ID']}** (RMSE: {best_run['RMSE']:.1f})")

def display_parallel_coordinates(df):
    st.header("2. Analysis: Params vs Performance")
    
    # Convert depth 'None' to -1 for plotting
    plot_df = df.copy()
    plot_df['max_depth'] = plot_df['max_depth'].replace('None', 30).astype(int)
    
    fig = px.parallel_coordinates(
        plot_df, 
        dimensions=['n_estimators', 'max_depth', 'learning_rate', 'RMSE'],
        color='RMSE',
        color_continuous_scale=px.colors.diverging.Tealrose,
        title="Parallel Coordinates Plot (Darker Line = Better RMSE)"
    )
    st.plotly_chart(fig, use_container_width=True)

def main():
    display_mlops_sidebar(17)
    
    st.title("🧪 Level 17: MLflow")
    st.markdown("**'Stop guessing, Start tracking.'**")
    
    st.info("In MLOps, we run hundreds of experiments. MLflow saves the history of everything.")
    
    if st.button("🔄 Run 10 New Experiments"):
        st.session_state['mlflow_runs'] = generate_experiments()
        
    if 'mlflow_runs' not in st.session_state:
        st.session_state['mlflow_runs'] = generate_experiments()
        
    df = st.session_state['mlflow_runs']
    
    display_mlflow_dashboard(df)
    st.markdown("---")
    display_parallel_coordinates(df)
    
    st.markdown("---")
    st.markdown(get_architect_note(17))

if __name__ == "__main__":
    main()
