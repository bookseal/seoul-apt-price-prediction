# -*- coding: utf-8 -*-
"""
Level 10: The Final Boss (Ultimate Linear Model vs AutoML)

The mathematical limit of linear modeling vs The Brute Force of AutoML.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE
from src.comparison import display_rmse_comparison
from src.navigation import display_code_link

def display_header() -> None:
    st.title("👑 Level 10: Ultimate Linear Model & AutoML")
    st.caption("The Final Showdown: White Box vs Black Box")
    st.markdown("""
    **Goal**: Achieve the absolute limit of what a Linear Model can do (RMSE < 19,000).
    
    Then, we challenge it with **AutoML (PyCaret)** to see if a complex "Black Box" model can beat our carefully crafted "White Box" model.
    """)

def display_toc() -> None:
    st.markdown("""
    ### 📑 Table of Contents
    1.  [**Step 1: Pipeline Overview**](#step-1-pipeline-overview)
    2.  [**Step 2: The Winning Strategy (Linear)**](#step-2-the-winning-strategy-linear)
    3.  [**Step 3: Linear Pipeline Execution**](#step-3-linear-pipeline-execution)
    4.  [**Step 4: Linear Final Evaluation**](#step-4-linear-final-evaluation)
    5.  [**Step 5: The AutoML Challenger**](#step-5-the-automl-challenger)
    """)

def display_pipeline_overview() -> None:
    """Show the pipeline."""
    st.header("Step 1: Pipeline Overview")
    
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>1. Load</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #F44336, #D32F2F); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>2. Strict Filter</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>3. Poly Degree 5</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>4. Ridge Optim</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>5. vs AutoML</b>
        </div>
    </div>
    """, unsafe_allow_html=True)

def display_pipeline_concept() -> None:
    st.header("Step 2: The Winning Strategy (Linear)")
    st.markdown("""
    After rigorous experimentation (Level 9), we found the optimal configuration to minimize **Real Price RMSE**:
    
    1.  **Target Variable**: **Direct Price** (No Log Transform).
        *   *Why?* Log-transform minimizes *percentage error*, which helps cheaper apartments but punishes expensive ones less in absolute terms. To win the RMSE game, we must target the raw numbers directly.
    2.  **Cleaning (Level 7)**: Strict Outlier Removal (**IQR 1.5**) on the Raw Price.
    3.  **Feature Engineering (Level 5 & 8)**: Interaction Terms (`Area * Year`) are crucial.
    4.  **Model Complexity (Level 9)**: **Polynomial Degree 5**. This is extreme curvature.
    5.  **Regularization**: **Ridge** (L2). Lasso knocks out features, but we need *every bit of signal* from those polynomials. Ridge keeps them but tames them.
    """)

def run_ultimate_linear_model(df):
    st.header("Step 3: Linear Pipeline Execution")
    
    # --- 1. Data Cleaning ---
    st.markdown("##### 1. Data Cleaning (Target: Raw Price)")
    
    with st.expander("Show Code: Data Cleaning"):
        st.code("""
# Outlier Removal (IQR 1.5 on Raw Price)
def remove_outliers(data, column):
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    return data[(data[column] >= lower) & (data[column] <= upper)]

df_clean = remove_outliers(df, 'price_10k_krw')
        """, language='python')

    df_clean = df.copy()
    target_col = 'price_10k_krw'
    
    # Outlier Removal
    for col in [target_col, 'area_m2']:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]
    
    st.write(f"Data shape after cleaning: `{df_clean.shape}`")

    # --- 2. Feature Engineering ---
    st.markdown("##### 2. Feature Engineering (Interactions)")
    
    with st.expander("Show Code: Interaction Features"):
        st.code("""
# Creating Interaction Terms
df_clean['area_x_year'] = df_clean['area_m2'] * df_clean['year']
features = ['area_m2', 'year', 'floor', 'area_x_year']
        """, language='python')

    df_clean['area_x_year'] = df_clean['area_m2'] * df_clean['year']
    features = ['area_m2', 'year', 'floor', 'area_x_year']
    
    X = df_clean[features].values
    y = df_clean[target_col].values 
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # --- 3. Model Training ---
    st.markdown("##### 3. Training the Optimized Model")
    st.info("Pipeline: `Degree=5` + `Ridge(alpha=0.0001)`")
    
    with st.expander("Show Code: The Training Pipeline"):
        st.code("""
model = Pipeline([
    ('poly', PolynomialFeatures(degree=5, include_bias=False)),
    ('scaler', StandardScaler()),
    ('model', Ridge(alpha=0.0001))
])
model.fit(X_train, y_train)
        """, language='python')
    
    model = Pipeline([
        ('poly', PolynomialFeatures(degree=5, include_bias=False)),
        ('scaler', StandardScaler()),
        ('model', Ridge(alpha=0.0001, random_state=42))
    ])
    
    with st.spinner("Fitting 5th Degree Polynomials... (This is math heavy!)"):
        model.fit(X_train, y_train)
    
    # Predict
    y_pred = model.predict(X_test)
    
    # No Inverse Transform needed (Direct Target)
    rmse = calculate_rmse(y_test, y_pred)
    
    st.metric("Ultimate Linear Model RMSE", f"{rmse:,.0f}", delta=f"{24000 - rmse:,.0f} Improvement vs Level 9", delta_color="normal")
    
    # --- 4. Validation ---
    st.header("Step 4: Linear Final Evaluation")
    
    # RMSE Comparison
    display_rmse_comparison(10, rmse)
    
    # Residual Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(y_test, y_pred, alpha=0.5, color='#4CAF50')
    ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2)
    ax.set_xlabel('Actual Price (10k KRW)')
    ax.set_ylabel('Predicted Price (10k KRW)')
    ax.set_title('Actual vs Predicted (Direct Optimization)')
    ax.grid(True, alpha=0.3)
    st.pyplot(fig)
    
    st.caption("""
    **Graph Interpretation**:
    1.  **Linearity**: Points hug the diagonal line tightly, indicating high accuracy.
    2.  **Homoscedasticity**: The spread of errors is relatively consistent across price ranges (thanks to targeting raw price).
    3.  **Outliers**: Very few points are far from the line, showing the effectiveness of our IQR cleaning.
    """)
    
    return rmse

def run_automl_challenger(linear_rmse):
    st.header("Step 5: The AutoML Challenger")
    st.markdown("""
    **Can modern AutoML beat our specialized Linear Model?**
    
    We ran `PyCaret` to test advanced algorithms like **CatBoost**, **XGBoost**, and **LightGBM** on the exact same data.
    """)
    
    with st.expander("Show Code: PyCaret AutoML Setup"):
        st.code("""
from pycaret.regression import *

# 1. Setup (Auto-Preprocessing)
exp = setup(data, target='target', session_id=42)

# 2. Battle Royale (Compare All Models)
best_model = compare_models(sort='RMSE')
        """, language='python')
    
    # Hardcoded results from the notebook execution to avoid long wait times
    automl_rmse = 16842 # CatBoost Result
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.info("### 🤖 AutoML (CatBoost)")
        st.metric("Best AutoML RMSE", f"{automl_rmse:,.0f}", delta=f"{linear_rmse - automl_rmse:,.0f} Better", delta_color="inverse")
        st.caption("Model: CatBoost Regressor (Black Box)")
        
    with col2:
        st.success("### 👑 Ultimate Linear Model")
        st.metric("Our Linear RMSE", f"{linear_rmse:,.0f}", delta="Benchmark")
        st.caption("Model: Poly5 + Ridge (White Box)")
        
    st.markdown("""
    > **Note on AutoML Winner**: The winning model found by PyCaret is **CatBoost**, which uses **Gradient Boosting**. 
    > Unlike linear models, it builds thousands of decision trees, where each new tree corrects the errors of the previous ones.
    """)
    
    st.markdown("""
    ### 🏆 The Verdict
    
    1.  **Performance**: The **AutoML (CatBoost)** wins purely on numbers (~11% better). Non-linear tree models are simply more flexible and can capture sharp discontinuities that Polynomials cannot.
    2.  **Explainability**: Our **Linear Model** is a formula we can write down ($y = w_1x_1 + w_2x_2^2 ...$). AutoML is a "Black Box" - hard to explain *why* it predicts what it predicts.
    3.  **Efficiency**: Linear Model trains in seconds. AutoML took minutes to hours to search.
    
    **Conclusion**: For a Linear Model to get this close to a state-of-the-art Boosting model is an incredible achievement. It remains the best choice when **Interpretability** is key.
    """)
    
    st.markdown("---")
    display_questions()
    display_summary()
    st.markdown("---")
    st.header("🎉 Part 1 Complete: You are now a Data Scientist!")
    st.balloons()
    
    st.markdown("""
    You have mastered the **Art of Modeling**:
    *   **Level 1-4**: You learned to frame problems and use Regression.
    *   **Level 5-6**: You conquered High Dimensionality and PCA.
    *   **Level 7-9**: You mastered Data Cleaning and Regularization.
    *   **Level 10**: You reached the mathematical limit of White Box models.
    
    ### 🚀 What's Next? The Science of Production
    Building a model is only 20% of the work. Now you need to **deploy** it, **monitor** it, and **automate** it.
    
    **Welcome to Part 2: MLOps.**
    """)
    
    if st.button("Start Part 2: MLOps Track (Level 11)", type="primary", use_container_width=True):
        st.switch_page("pages/11_MLOps_Lv1_Data.py")


def display_questions() -> None:
    """Show common questions."""
    st.header("🤔 Questions You Might Have")

    st.markdown("""
    <div style="padding: 15px; background: rgba(255,193,7,0.1); border-radius: 10px; 
                border-left: 4px solid #FFC107; margin: 10px 0;">
        <b>Q1: "Is AutoML better than me?"</b><br>
        <span style="color: #FFC107;">→ Usually yes for baseline!</span> 
        It tries 20+ models in minutes. But it can't fix bad data or understand business context. 
        You are the **Pilot**, AutoML is the **Autopilot**.
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,193,7,0.1); border-radius: 10px; 
                border-left: 4px solid #FFC107; margin: 10px 0;">
        <b>Q2: "What models are tried?"</b><br>
        <span style="color: #FFC107;">→ Everything!</span> 
        Decision Trees, Random Forests, Gradient Boosting (XGBoost, CatBoost, LightGBM), SVM, KNN...
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="padding: 15px; background: rgba(255,193,7,0.1); border-radius: 10px; 
                border-left: 4px solid #FFC107; margin: 10px 0;">
        <b>Q3: "Why use Linear Model if AutoML is better?"</b><br>
        <span style="color: #FFC107;">→ Trust & Speed.</span> 
        Banks and Hospitals often require **Explainable AI**. You can explain a formula, but you can't easy explain a Black Box.
    </div>
    """, unsafe_allow_html=True)


def display_summary() -> None:
    """Show summary."""
    st.markdown("""
    ### 🎓 Summary
    
    You've completed Level 10! You learned:
    
    1.  **Ultimate Linear Model**: Pushing a simple model to its limits (Poly 5 + Ridge).
    2.  **AutoML**: Using automatic tools (PyCaret) to find the best model.
    3.  **Trade-off**: Accuracy (AutoML) vs Explainability (Linear).
    
    **Problem:** We have a great model... on my laptop. How do we share it?
    **Next:** Part 2: MLOps covers Deployment, Automation, and Monitoring!
    """)

def main() -> None:
    try:
        df = load_sample_dataset()
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_toc()
        st.markdown("---")
        display_pipeline_concept()
        st.markdown("---")
        linear_rmse = run_ultimate_linear_model(df)
        st.markdown("---")
        run_automl_challenger(linear_rmse)
        
        st.markdown("---")
        display_code_link("Level_10_The_Final_Boss.ipynb")
        
    except Exception as e:
        st.error(f"Error: {e}")

if __name__ == "__main__":
    main()
