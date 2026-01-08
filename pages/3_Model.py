import streamlit as st


def display_model_header() -> None:
    """
    Render the page title and modeling vision.

    Visual Guide:
    [ Baseline: RF ] -> [ Target: XGB/LGBM ] -> [ Final: Ensemble ]
    """
    st.title("🤖 Model: Predicting the Value")
    st.markdown(
        """
    In this stage, we transition from raw data to **Actionable Intelligence**. 
    Our goal is to minimize the prediction error using robust regression algorithms.
    """
    )


def display_evaluation_metric() -> None:
    """
    Explain RMSE (Root Mean Squared Error) with math and intuition.
    """
    st.subheader("📏 Evaluation Metric: RMSE (Root Mean Squared Error)")
    
    # Intuitive explanation first
    st.markdown("""
    ### 🎯 What is RMSE?
    
    Think of RMSE as a **"report card"** for your model's predictions. It tells you, on average, how far off your predictions are from reality.
    
    **Real-world example:**
    - Actual apartment price: **50,000** (×10,000 KRW = 500M KRW)
    - Your model predicts: **48,000** (×10,000 KRW = 480M KRW)
    - Error: **2,000** (×10,000 KRW = 20M KRW off)
    
    RMSE aggregates all these errors across thousands of predictions into one number.
    """)
    
    # Visual step-by-step process
    st.markdown("### 📊 How RMSE Works (Step-by-Step)")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: rgba(33, 150, 243, 0.1); border-radius: 8px; border-left: 4px solid #2196F3;">
        <div style="font-size: 20px; margin-bottom: 5px;">1️⃣</div>
        <div style="font-weight: bold; font-size: 12px; color: #2196F3;">Calculate Error</div>
        <div style="font-size: 11px; margin-top: 8px;">Actual - Predicted<br/>= Error</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: rgba(156, 39, 176, 0.1); border-radius: 8px; border-left: 4px solid #9C27B0;">
        <div style="font-size: 20px; margin-bottom: 5px;">2️⃣</div>
        <div style="font-weight: bold; font-size: 12px; color: #9C27B0;">Square It</div>
        <div style="font-size: 11px; margin-top: 8px;">Error²<br/>(Penalize big errors)</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: rgba(255, 152, 0, 0.1); border-radius: 8px; border-left: 4px solid #FF9800;">
        <div style="font-size: 20px; margin-bottom: 5px;">3️⃣</div>
        <div style="font-weight: bold; font-size: 12px; color: #FF9800;">Average</div>
        <div style="font-size: 11px; margin-top: 8px;">Sum all / n<br/>(Mean Squared Error)</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: rgba(76, 175, 80, 0.1); border-radius: 8px; border-left: 4px solid #4CAF50;">
        <div style="font-size: 20px; margin-bottom: 5px;">4️⃣</div>
        <div style="font-weight: bold; font-size: 12px; color: #4CAF50;">Take √</div>
        <div style="font-size: 11px; margin-top: 8px;">√(MSE)<br/>(Back to original units)</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Concrete example with numbers
    st.markdown("""
    ### 💡 Concrete Example
    
    Let's say we predict 3 apartments:
    
    | Apartment | Actual Price | Predicted | Error | Error² |
    |-----------|-------------|-----------|-------|--------|
    | A | 50,000 | 48,000 | 2,000 | 4,000,000 |
    | B | 30,000 | 32,000 | -2,000 | 4,000,000 |
    | C | 70,000 | 71,000 | -1,000 | 1,000,000 |
    
    **Calculation:**
    1. Sum of squared errors: 4M + 4M + 1M = **9,000,000**
    2. Mean (÷3): 9M ÷ 3 = **3,000,000**
    3. Square root: √3M ≈ **1,732**
    
    **Result:** RMSE = **1,732** (×10k KRW) ≈ **17.3M KRW average error**
    """)
    
    st.divider()
    
    # Mathematical formula
    st.markdown("### 🧮 Mathematical Formula")
    st.latex(r"RMSE = \sqrt{\frac{1}{n} \sum_{i=1}^{n} (y_i - \hat{y}_i)^2}")

    st.markdown("""
    **Variables:**
    - $n$ = Number of predictions (apartments)
    - $y_i$ = Actual price (what it really sold for)
    - $\hat{y}_i$ = Predicted price (what our model guessed)
    """)

    st.markdown("""
    ### ⚡ Why Square the Errors?
    
    **Two key reasons:**
    1. **No cancellation:** Errors of +2,000 and -2,000 would cancel out if we just averaged them. Squaring makes all errors positive.
    2. **Penalize big mistakes:** An error of 10,000 becomes 100M when squared, so the model is punished more for wild guesses than small mistakes.
    
    **Why take the square root at the end?**  
    To bring the units back to the original scale (10,000 KRW), making it easier to interpret.
    """)
    
    # Comparison
    st.info("""
    **💭 In plain English:**  
    "On average, our predictions are off by about **X** units."  
    Lower RMSE = Better model = More accurate predictions.
    """)


def display_modeling_strategy() -> None:
    """
    Show the Roadmap from Baseline to SOTA.
    """
    st.subheader("🚀 Modeling Roadmap")

    roadmap = {
        "Phase 1: Baseline": "Random Forest Regressor (Current Approach)",
        "Phase 2: Boosting": "XGBoost & LightGBM for better accuracy",
        "Phase 3: Optimization": "Hyperparameter tuning & Feature Selection",
    }

    for stage, desc in roadmap.items():
        with st.expander(stage):
            st.write(desc)


def render_model_page() -> None:
    """
    Orchestrate the Model page rendering.
    """
    display_model_header()

    st.divider()
    display_evaluation_metric()

    st.divider()
    display_modeling_strategy()


if __name__ == "__main__":
    render_model_page()
