# -*- coding: utf-8 -*-
"""
MLOps Level 4: AutoML & RMSE
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error
from src.mlops_utils import generate_seongsu_data, get_architect_note
from src.navigation import display_mlops_sidebar

def run_automl(df):
    st.header("1. AutoML Arena")
    st.markdown("We train 3 different models and compete them against each other.")
    
    X = df[['temp_c', 'rain_mm', 'is_holiday']]
    y = df['passengers']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    models = {
        "Linear Regression": LinearRegression(),
        "Random Forest": RandomForestRegressor(n_estimators=50, random_state=42),
        "Gradient Boosting": GradientBoostingRegressor(n_estimators=50, random_state=42)
    }
    
    results = []
    
    progress = st.progress(0)
    for i, (name, model) in enumerate(models.items()):
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        rmse = mean_squared_error(y_test, pred, squared=False)
        results.append({"Model": name, "RMSE": rmse})
        progress.progress((i + 1) / len(models))
        
    st.success("AutoML Complete!")
    return pd.DataFrame(results).sort_values("RMSE")

def display_results(results_df):
    st.header("2. Leaderboard")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.dataframe(results_df.style.format({"RMSE": "{:,.0f}"}), use_container_width=True)
        best_model = results_df.iloc[0]['Model']
        st.info(f"🏆 Best Model: **{best_model}**")
        
    with col2:
        fig = px.bar(results_df, x='RMSE', y='Model', orientation='h', 
                     color='RMSE', color_continuous_scale='RdYlGn_r', 
                     title="Lower RMSE is Better")
        st.plotly_chart(fig, use_container_width=True)

def main():
    display_mlops_sidebar(14)
    
    st.title("🏎️ Level 14: AutoML & RMSE")
    st.markdown("**'Survival of the Fittest'**")
    
    if 'mlops_df' not in st.session_state:
        st.session_state['mlops_df'] = generate_seongsu_data()
    df = st.session_state['mlops_df']
    
    if st.button("🚀 Run AutoML"):
        results_df = run_automl(df)
        st.session_state['automl_results'] = results_df
        
    if 'automl_results' in st.session_state:
        display_results(st.session_state['automl_results'])
        
    st.markdown("---")
    st.markdown(get_architect_note(14))

if __name__ == "__main__":
    main()
