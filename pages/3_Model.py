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

    ASCII Infographic:
    Actual:   ●
    Error:    |  <-- Squared & Averaged
    Predicted:○
    """
    st.subheader("📏 Evaluation Metric: RMSE")
    st.latex(r"RMSE = \sqrt{\frac{1}{n} \sum_{i=1}^{n} (y_i - \hat{y}_i)^2}")

    st.info(
        """
    **Mathematical Variables:**
    - $n$ (Number of observations)
    - $y_i$ (Actual price)
    - $\hat{y}_i$ (Predicted price)
    """
    )

    st.markdown(
        """
    **Why RMSE?**
    It penalizes larger errors more heavily by squaring them, and the 'Root' brings the unit back to the original scale (10,000 KRW).
    """
    )


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
