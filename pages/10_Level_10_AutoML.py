# -*- coding: utf-8 -*-
"""
Level 10: AutoML - Comparing Multiple Models

The grand finale! Compare many different models automatically
to find the best one for apartment price prediction.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import (
    RandomForestRegressor, 
    GradientBoostingRegressor,
    AdaBoostRegressor
)
from sklearn.svm import SVR
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.pipeline import Pipeline
import time
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE


def display_header() -> None:
    """Display Level 10 introduction."""
    st.title("🏆 Level 10: AutoML - The Grand Finale!")
    
    st.success("""
    **Goal**: Compare MANY different models to find the best one!
    
    Welcome to the final level of your ML journey! 🎉
    """)
    
    with st.expander("💡 What is AutoML?"):
        st.markdown("""
        **AutoML** = Automated Machine Learning
        
        Instead of manually trying different models, AutoML:
        - Tests many model types automatically
        - Tunes hyperparameters
        - Compares performance
        - Selects the best model
        
        **Tools**: PyCaret, AutoGluon, H2O, TPOT, etc.
        
        For this demo, we'll compare models using scikit-learn!
        """)


def display_pipeline_overview() -> None:
    """Show the pipeline."""
    st.header("🔄 Level 10 Pipeline")
    
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>1. Prepare</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>2. Define Models</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>3. Train All</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>4. Leaderboard</b>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 15px; background: linear-gradient(135deg, #E91E63, #C2185B); 
                    border-radius: 8px; color: white; text-align: center;">
            <b>5. Analyze Best</b>
        </div>
    </div>
    """, unsafe_allow_html=True)


def display_why_level10() -> None:
    """Explain motivation for Level 10."""
    st.header("🤔 Why Try Different Models?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 10px 0;">
        <b>💡 Different models for different data!</b><br>
        <span style="font-size: 13px;">
        We've only used Linear Regression so far. But there are many more models!<br><br>
        <b>Model types:</b><br>
        • Linear: Linear Regression, Ridge, Lasso<br>
        • Tree-based: Decision Tree, Random Forest, Gradient Boosting<br>
        • Instance-based: KNN, SVR<br><br>
        <b>Which one is best? Let's find out!</b>
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_model_explanations() -> None:
    """Explain different model types."""
    st.header("📚 Model Types Explained")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🌲 Tree-Based Models
        
        **Decision Tree**
        - Splits data based on feature values
        - Easy to interpret
        - Can overfit easily
        
        **Random Forest**
        - Many decision trees combined
        - Reduces overfitting
        - Very popular, robust
        
        **Gradient Boosting**
        - Trees built sequentially
        - Each tree corrects previous errors
        - Often best performance!
        """)
    
    with col2:
        st.markdown("""
        ### 📊 Other Models
        
        **KNN (K-Nearest Neighbors)**
        - Predicts based on similar samples
        - Simple but slow for large data
        
        **SVR (Support Vector Regression)**
        - Finds optimal boundary
        - Good for small datasets
        - Needs scaling
        
        **Linear Models**
        - We've covered these!
        - Ridge, Lasso, ElasticNet
        """)


def prepare_data(df: pd.DataFrame):
    """Prepare data for model comparison."""
    df = df.copy()
    np.random.seed(RANDOM_STATE)
    n = len(df)
    
    if 'year' not in df.columns:
        df['year'] = np.random.randint(1985, 2024, n)
    if 'floor' not in df.columns:
        df['floor'] = np.random.randint(1, 30, n)
    
    df['building_age'] = 2024 - df['year']
    
    return df


def get_models():
    """Return dictionary of models to compare."""
    return {
        'Linear Regression': LinearRegression(),
        'Ridge': Ridge(alpha=1.0),
        'Lasso': Lasso(alpha=1.0, max_iter=10000),
        'ElasticNet': ElasticNet(alpha=1.0, l1_ratio=0.5, max_iter=10000),
        'Decision Tree': DecisionTreeRegressor(max_depth=10, random_state=RANDOM_STATE),
        'Random Forest': RandomForestRegressor(n_estimators=50, max_depth=10, random_state=RANDOM_STATE, n_jobs=-1),
        'Gradient Boosting': GradientBoostingRegressor(n_estimators=50, max_depth=5, random_state=RANDOM_STATE),
        'AdaBoost': AdaBoostRegressor(n_estimators=50, random_state=RANDOM_STATE),
        'KNN': KNeighborsRegressor(n_neighbors=5),
    }


@st.cache_resource
def run_model_comparison(df: pd.DataFrame):
    """Train all models and compare performance."""
    df = prepare_data(df)
    
    feature_cols = ['area_m2', 'year', 'floor', 'building_age']
    
    X = df[feature_cols].values
    y = df['price_10k_krw'].values
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=RANDOM_STATE
    )
    
    models = get_models()
    results = []
    
    for name, model in models.items():
        start_time = time.time()
        
        # Train
        model.fit(X_train, y_train)
        
        # Predict
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        
        # Calculate metrics
        train_rmse = calculate_rmse(y_train, y_pred_train)
        test_rmse = calculate_rmse(y_test, y_pred_test)
        
        elapsed = time.time() - start_time
        
        results.append({
            'Model': name,
            'Train RMSE': train_rmse,
            'Test RMSE': test_rmse,
            'Time (s)': elapsed,
            'model': model
        })
    
    # Sort by test RMSE
    results = sorted(results, key=lambda x: x['Test RMSE'])
    
    return results, X_test, y_test, scaler, feature_cols


def display_leaderboard(results: list) -> None:
    """Display model leaderboard."""
    st.header("🏆 Model Leaderboard")
    
    st.markdown("""
    **Results from training all models on apartment price data!**
    
    Sorted by Test RMSE (lower is better).
    """)
    
    # Create dataframe without model objects
    leaderboard = pd.DataFrame([{
        'Rank': i+1,
        'Model': r['Model'],
        'Train RMSE': f"{r['Train RMSE']:,.0f}",
        'Test RMSE': f"{r['Test RMSE']:,.0f}",
        'Time (s)': f"{r['Time (s)']:.3f}"
    } for i, r in enumerate(results)])
    
    # Style the dataframe
    st.dataframe(leaderboard, use_container_width=True)
    
    # Highlight winner
    best = results[0]
    st.success(f"""
    🥇 **Winner: {best['Model']}** with Test RMSE = {best['Test RMSE']:,.0f}
    """)
    
    # Bar chart
    fig, ax = plt.subplots(figsize=(10, 6))
    
    model_names = [r['Model'] for r in results]
    test_rmses = [r['Test RMSE'] for r in results]
    
    colors = ['gold' if i == 0 else 'silver' if i == 1 else '#CD7F32' if i == 2 else 'steelblue' 
              for i in range(len(results))]
    
    bars = ax.barh(model_names[::-1], test_rmses[::-1], color=colors[::-1])
    ax.set_xlabel('Test RMSE (lower is better)')
    ax.set_title('Model Performance Comparison')
    
    # Add values
    for bar, val in zip(bars, test_rmses[::-1]):
        ax.text(val + 500, bar.get_y() + bar.get_height()/2, 
                f'{val:,.0f}', va='center')
    
    st.pyplot(fig, use_container_width=True)
    plt.close()


def display_overfitting_analysis(results: list) -> None:
    """Analyze overfitting across models."""
    st.header("📊 Overfitting Analysis")
    
    st.markdown("""
    **Train vs Test RMSE**: Large gap = Overfitting!
    """)
    
    model_names = [r['Model'] for r in results]
    train_rmses = [r['Train RMSE'] for r in results]
    test_rmses = [r['Test RMSE'] for r in results]
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x = np.arange(len(model_names))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, train_rmses, width, label='Train RMSE', color='steelblue')
    bars2 = ax.bar(x + width/2, test_rmses, width, label='Test RMSE', color='orange')
    
    ax.set_ylabel('RMSE')
    ax.set_title('Train vs Test RMSE by Model')
    ax.set_xticks(x)
    ax.set_xticklabels(model_names, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Calculate overfitting gap
    st.markdown("### Overfitting Gap (Test - Train)")
    
    gaps = [(r['Model'], r['Test RMSE'] - r['Train RMSE']) for r in results]
    gaps = sorted(gaps, key=lambda x: x[1])
    
    gap_df = pd.DataFrame(gaps, columns=['Model', 'Gap'])
    gap_df['Status'] = gap_df['Gap'].apply(lambda x: '✅ Good' if x < 2000 else '⚠️ Slight' if x < 5000 else '❌ Overfitting')
    
    st.dataframe(gap_df, use_container_width=True)


def display_best_model_analysis(results: list, X_test, y_test) -> None:
    """Analyze the best model in detail."""
    st.header("🔍 Best Model Analysis")
    
    best = results[0]
    model = best['model']
    
    st.markdown(f"""
    **Best Model**: {best['Model']}
    """)
    
    # Feature importance (for tree-based models)
    if hasattr(model, 'feature_importances_'):
        st.markdown("### Feature Importance")
        
        feature_cols = ['area_m2', 'year', 'floor', 'building_age']
        importances = model.feature_importances_
        
        imp_df = pd.DataFrame({
            'Feature': feature_cols,
            'Importance': importances
        }).sort_values('Importance', ascending=False)
        
        fig, ax = plt.subplots(figsize=(8, 4))
        colors = plt.cm.Greens(np.linspace(0.3, 0.9, len(imp_df)))
        ax.barh(imp_df['Feature'], imp_df['Importance'], color=colors[::-1])
        ax.set_xlabel('Importance')
        ax.set_title(f'{best["Model"]} - Feature Importance')
        st.pyplot(fig, use_container_width=True)
        plt.close()
        
        st.markdown(f"""
        **Most important feature**: {imp_df.iloc[0]['Feature']} ({imp_df.iloc[0]['Importance']:.3f})
        """)
    
    # Actual vs Predicted
    st.markdown("### Actual vs Predicted")
    
    y_pred = model.predict(X_test)
    
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(y_test, y_pred, alpha=0.3, s=15, c='steelblue')
    max_val = max(y_test.max(), y_pred.max())
    ax.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Perfect')
    ax.set_xlabel('Actual Price (10K KRW)')
    ax.set_ylabel('Predicted Price (10K KRW)')
    ax.set_title(f'{best["Model"]}: Actual vs Predicted')
    ax.legend()
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Compare with other levels
    st.markdown("---")
    from src.comparison import display_rmse_comparison
    display_rmse_comparison(10, best['RMSE'])


def display_demo(results: list, scaler, feature_cols: list, df: pd.DataFrame) -> None:
    """Interactive prediction with best model."""
    st.header("🔮 Try the Best Model")
    
    best = results[0]
    model = best['model']
    
    st.markdown(f"""
    **Using {best['Model']} to predict apartment prices!**
    """)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        area = st.slider("Area (m²)", 20, 200, 84)
    
    with col2:
        year = st.slider("Building Year", 1985, 2024, 2015)
    
    with col3:
        floor = st.slider("Floor", 1, 50, 10)
    
    with col4:
        building_age = 2024 - year
        st.metric("Building Age", f"{building_age} years")
    
    # Prepare input
    X_input = np.array([[area, year, floor, building_age]])
    X_input_scaled = scaler.transform(X_input)
    
    # Predict
    prediction = model.predict(X_input_scaled)[0]
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Predicted Price", f"{prediction:,.0f}", help="In 10K KRW")
    
    with col2:
        st.metric("억원", f"{prediction/10000:.2f}")
    
    st.success(f"""
    **{best['Model']} predicts**: {prediction:,.0f} (10K KRW) ≈ **{prediction/10000:.2f} 억원**
    """)


def display_journey_summary() -> None:
    """Summarize the ML journey."""
    st.header("🎓 Your ML Journey - Complete!")
    
    st.balloons()
    
    st.markdown("""
    ### 📚 What You've Learned
    
    | Level | Topic | Key Concept |
    |-------|-------|-------------|
    | 1 | Heuristic | Simple rules without ML |
    | 2 | Linear Regression | y = wx + b |
    | 3 | Multiple Features | One-Hot Encoding |
    | 4 | 3D Regression | Building Year |
    | 5 | High-Dimensional | Curse of dimensionality |
    | 6 | PCA | Dimensionality reduction |
    | 7 | Data Cleaning | Null values, outliers |
    | 8 | Feature Engineering | Scaling, transformations |
    | 9 | Regularization | Ridge, Lasso, ElasticNet |
    | 10 | AutoML | Model comparison |
    
    ### 🚀 What's Next?
    
    - **Deep Learning**: Neural networks for complex patterns
    - **Time Series**: Predicting future prices
    - **NLP**: Text data for real estate descriptions
    - **Computer Vision**: Analyzing apartment photos
    - **MLOps**: Deploying models to production
    
    **Congratulations on completing the ML roadmap!** 🎉
    """)
    
    st.info("""
    **💡 Keep Learning!**
    
    Resources:
    - [Scikit-learn Documentation](https://scikit-learn.org)
    - [Kaggle Competitions](https://kaggle.com)
    - [Fast.ai Courses](https://fast.ai)
    - [Andrew Ng's ML Course](https://coursera.org/learn/machine-learning)
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_why_level10()
        st.markdown("---")
        display_model_explanations()
        st.markdown("---")
        
        # Run model comparison
        with st.spinner("Training all models... This may take a minute! ⏳"):
            results, X_test, y_test, scaler, feature_cols = run_model_comparison(df)
        
        display_leaderboard(results)
        st.markdown("---")
        display_overfitting_analysis(results)
        st.markdown("---")
        display_best_model_analysis(results, X_test, y_test)
        st.markdown("---")
        display_demo(results, scaler, feature_cols, df)
        st.markdown("---")
        display_journey_summary()
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
