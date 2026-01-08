# -*- coding: utf-8 -*-
"""
Level 3: Multiple Features Linear Regression

Improved ML model using multiple features: Area, Floor, Year, District.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from src.io import load_sample_dataset
from src.utils import calculate_rmse
from src.config import RANDOM_STATE


@st.cache_resource
def train_multi_feature_model(df: pd.DataFrame):
    """
    Train Linear Regression with multiple features.
    
    Features: area_m2, floor (if available), district (encoded)
    """
    df_train = df.copy()
    
    # Encode district
    le = LabelEncoder()
    df_train['district_encoded'] = le.fit_transform(df_train['district'])
    
    # Select features (use available columns)
    feature_cols = ['area_m2', 'district_encoded']
    if 'floor' in df_train.columns:
        # Handle missing floor values
        df_train['floor'] = df_train['floor'].fillna(df_train['floor'].median())
        feature_cols.append('floor')
    
    X = df_train[feature_cols].values
    y = df_train['price_10k_krw'].values
    
    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE
    )
    
    # Train model
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test)
    rmse = calculate_rmse(y_test, y_pred)
    
    return model, le, feature_cols, rmse, (y_test, y_pred)


def display_header() -> None:
    """Display Level 3 introduction."""
    st.title("🚀 Level 3: Multiple Features")
    
    st.success("""
    **Goal**: Improve predictions by adding more features!
    
    We now use: **Area + District + Floor**
    """)


def display_method() -> None:
    """Explain multi-feature approach."""
    st.header("🧮 The Method")
    
    st.markdown("""
    ### Multiple Linear Regression
    
    Instead of one feature, we use multiple features.
    """)
    
    st.latex(r"\text{Price} = w_1 \times \text{Area} + w_2 \times \text{District} + w_3 \times \text{Floor} + b")
    
    st.markdown("""
    **New additions:**
    - **District**: Encoded as numbers (Gangnam = higher weight)
    - **Floor**: Higher floors often cost more
    
    The model learns the importance of each feature!
    """)
    
    with st.expander("🤔 Why add more features?"):
        st.markdown("""
        **Level 2 Problem**: Same prediction for all districts!
        
        A 100m² apartment in Gangnam ≠ 100m² in other areas.
        
        By adding district, the model can learn:
        - Gangnam premium
        - Seocho premium
        - etc.
        """)


def display_feature_importance(model, feature_cols: list) -> None:
    """Show feature importance from coefficients."""
    st.header("📊 Feature Importance")
    
    # Get coefficients
    coefs = model.coef_
    
    # Create dataframe
    importance_df = pd.DataFrame({
        'Feature': feature_cols,
        'Coefficient': coefs
    }).sort_values('Coefficient', key=abs, ascending=False)
    
    fig, ax = plt.subplots(figsize=(8, 4))
    colors = ['green' if c > 0 else 'red' for c in importance_df['Coefficient']]
    ax.barh(importance_df['Feature'], importance_df['Coefficient'], color=colors)
    ax.set_xlabel('Coefficient Value')
    ax.set_title('Feature Coefficients')
    ax.axvline(x=0, color='black', linewidth=0.5)
    st.pyplot(fig)
    plt.close()
    
    st.markdown("""
    **Interpretation**:
    - **Positive coefficient**: Feature increases price
    - **Larger absolute value**: Feature has more impact
    """)


def display_performance_comparison(rmse_l3: float, y_test, y_pred) -> None:
    """Compare with previous levels."""
    st.header("📏 Performance Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Level 3 RMSE", f"{rmse_l3:,.0f}")
        st.caption("Multiple features")
        
        st.markdown("""
        **Lower RMSE = Better!**
        
        Adding features should reduce error.
        """)
    
    with col2:
        # Actual vs Predicted plot
        fig, ax = plt.subplots(figsize=(6, 5))
        sample_idx = np.random.choice(len(y_test), min(1000, len(y_test)), replace=False)
        ax.scatter(y_test[sample_idx], y_pred[sample_idx], alpha=0.3, s=10)
        max_val = max(y_test.max(), y_pred.max())
        ax.plot([0, max_val], [0, max_val], 'r--', linewidth=2)
        ax.set_xlabel('Actual Price')
        ax.set_ylabel('Predicted Price')
        ax.set_title('Actual vs Predicted')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)
        plt.close()


def display_demo(df: pd.DataFrame, model, le, feature_cols: list) -> None:
    """Interactive prediction demo."""
    st.header("🔮 Try It Yourself")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        districts = sorted(df['district'].unique())
        selected_district = st.selectbox("District", districts)
    
    with col2:
        selected_area = st.slider("Area (m²)", 10, 200, 84)
    
    with col3:
        if 'floor' in feature_cols:
            selected_floor = st.slider("Floor", 1, 50, 10)
        else:
            selected_floor = None
    
    # Prepare input
    district_encoded = le.transform([selected_district])[0]
    
    if selected_floor is not None:
        X_input = np.array([[selected_area, district_encoded, selected_floor]])
    else:
        X_input = np.array([[selected_area, district_encoded]])
    
    # Predict
    predicted_price = model.predict(X_input)[0]
    
    st.markdown("---")
    
    cols = st.columns(4 if selected_floor else 3)
    
    with cols[0]:
        st.metric("District", selected_district)
    
    with cols[1]:
        st.metric("Area", f"{selected_area} m²")
    
    if selected_floor:
        with cols[2]:
            st.metric("Floor", f"{selected_floor}F")
        with cols[3]:
            st.metric("Predicted", f"{predicted_price:,.0f}")
    else:
        with cols[2]:
            st.metric("Predicted", f"{predicted_price:,.0f}")
    
    st.success(f"""
    **Predicted Price**: {predicted_price:,.0f} (10K KRW) ≈ **{predicted_price/10000:.1f} 억원**
    """)


def display_next_steps() -> None:
    """Show what's next."""
    st.header("🚀 What's Next?")
    
    st.markdown("""
    ### Level 4+ Ideas (Coming Soon)
    
    | Improvement | Description |
    |-------------|-------------|
    | **More Features** | Add building age, nearby subway, etc. |
    | **Feature Engineering** | Create new features from existing ones |
    | **Better Algorithms** | Random Forest, XGBoost, LightGBM |
    | **Hyperparameter Tuning** | Optimize model settings |
    | **Cross-Validation** | More robust evaluation |
    | **Ensemble** | Combine multiple models |
    
    The journey continues! 🎓
    """)


def display_level_summary() -> None:
    """Summary of all levels."""
    st.header("📚 Level Summary")
    
    st.markdown("""
    | Level | Method | Features | Complexity |
    |-------|--------|----------|------------|
    | **1** | Heuristic | District + Area | No ML |
    | **2** | Linear Regression | Area only | Simple ML |
    | **3** | Linear Regression | Area + District + Floor | Multi-feature ML |
    | **4+** | Tree Models | Many features | Advanced ML |
    
    **Key Insight**: Each level adds complexity to improve predictions!
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_method()
        st.markdown("---")
        
        # Train model
        with st.spinner("Training model..."):
            model, le, feature_cols, rmse, (y_test, y_pred) = train_multi_feature_model(df)
        
        display_feature_importance(model, feature_cols)
        st.markdown("---")
        display_performance_comparison(rmse, y_test, y_pred)
        st.markdown("---")
        display_demo(df, model, le, feature_cols)
        st.markdown("---")
        display_level_summary()
        st.markdown("---")
        display_next_steps()
        
    except Exception as e:
        st.error(f"Error: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()
