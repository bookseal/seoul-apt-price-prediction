# -*- coding: utf-8 -*-
"""
Seoul Apartment Price Prediction - Linear Regression Learning Roadmap

End-to-end machine learning journey:
Linear Regression Roadmap (Seoul Apartments)

Usage:
    streamlit run app.py
"""
import streamlit as st

def set_premium_style():
    """Inject custom CSS for a premium look."""
    st.markdown("""
    <style>
        .main {
            background-color: #f8f9fa;
        }
        h1, h2, h3 {
            font-family: 'Helvetica Neue', sans-serif;
            font-weight: 700;
        }
        .card-container {
            background-color: white;
            border-radius: 20px;
            padding: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            transition: transform 0.3s;
            height: 100%;
            border: 1px solid #e0e0e0;
        }
        .card-container:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 15px rgba(0,0,0,0.15);
        }
        .track-title {
            font-size: 1.8rem;
            margin-bottom: 1rem;
            font-weight: 800;
        }
        .track-desc {
            color: #555;
            font-size: 1.1rem;
            line-height: 1.6;
            margin-bottom: 2rem;
        }
        .highlight-blue {
            color: #2962FF;
        }
        .highlight-pink {
            color: #D500F9;
        }
        .feature-list {
            margin-bottom: 25px;
            padding-left: 20px;
        }
        .feature-list li {
            margin-bottom: 8px;
            color: #333;
        }
    </style>
    """, unsafe_allow_html=True)

def home_page():
    """Render the portal home page."""
    set_premium_style()
    
    st.title("🚀 The Linear Regression Roadmap")
    st.markdown("### From First Formula to Ultimate White-Box Model")
    
    st.markdown("""
    Welcome to a focused machine learning curriculum for **Linear Regression mastery**.
    This roadmap takes you from intuition to robust modeling on Seoul apartment prices.
    """)
    
    
    st.markdown("---")
    
    st.markdown("""
    | Level | Topic | Question | Hypothesis | Result |
    | :--- | :--- | :--- | :--- | :--- |
    | 1 | [Heuristic](1_Level_1_Heuristic) | Can we catch the average? | Prices cluster around district means. | Baseline RMSE established. |
    | 2 | [Linear Regression](2_Level_2_Linear_Regression) | Is there a trend? | Price increases linearly with Area. | Simple line captures basic trend. |
    | 3 | [Multi-Features](3_Level_3_Multi_Features) | Does Year matter? | Newer apartments are more expensive. | Adding features improves accuracy. |
    | 4 | [3D Regression](4_Level_4_3D_Regression) | Area & Year interact? | We need a plane, not a line. | Visualizing the hyperplane of fit. |
    | 5 | [High Dimensionality](5_Level_5_High_Dimensional) | More features = Better? | Adding everything will solve it. | Curse of Dimensionality / Overfitting. |
    | 6 | [PCA](6_Level_6_PCA) | Can we compress info? | Many features are redundant. | Reduced dimensions not losing much variance. |
    | 7 | [Data Cleaning](7_Level_7_Data_Cleaning) | Are outliers hurting us? | Removing anomalies creates stability. | Massive improvement in RMSE. |
    | 8 | [Feature Engineering](8_Level_8_Feature_Engineering) | Create new insights? | Area * Year matters more than sum. | Interaction terms capture nuance. |
    | 9 | [Regularization](9_Level_9_Regularization) | Model too complex? | We need to penalize large weights. | Ridge/Lasso prevents overfitting. |
    | 10 | [Ultimate Model](10_Level_10_The_Final_Boss) | What is the limit? | Direct Price + Poly5 + Ridge is optimal. | Final Boss Defeated (Lowest RMSE). |
    """)

    st.markdown("---")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div class="card-container">
            <div class="track-title highlight-blue">🏙️ Linear Regression Track</div>
            <p class="track-desc">
                Dive deep into the mathematics and intuition of modeling.
                Build your best white-box predictor for <b>Seoul Apartment Prices</b>.
            </p>
            <ul class="feature-list">
                <li>🎯 <b>Level 1-4</b>: Foundational Regression</li>
                <li>🌌 <b>Level 5-6</b>: High-Dimensional Spaces & PCA</li>
                <li>🛡️ <b>Level 7-9</b>: Robustness & Regularization</li>
                <li>👑 <b>Level 10</b>: The Mathematical Limit</li>
            </ul>
            <p style="text-align: center; color: #888; font-style: italic; margin-top: 20px;">
                "I want to master linear regression end to end."
            </p>
        </div>
        """, unsafe_allow_html=True)
        st.write("") # Spacer
        if st.button("Start Linear Regression Roadmap", type="primary", use_container_width=True):
            st.switch_page("pages/1_Level_1_Heuristic.py")



def main() -> None:
    """Entry point with navigation."""
    st.set_page_config(
        page_title="ML Roadmap",
        page_icon="🚀",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Define pages
    pages = {
        "🏠 Dashboard": [
            st.Page(home_page, title="Home", icon="🏠"),
        ],
        "🏙️ Linear Regression Roadmap": [
            st.Page("pages/1_Level_1_Heuristic.py", title="L1: Heuristic", icon="🎯"),
            st.Page("pages/2_Level_2_Linear_Regression.py", title="L2: Linear Regression", icon="📐"),
            st.Page("pages/3_Level_3_Multi_Features.py", title="L3: Multi-Features", icon="🏘️"),
            st.Page("pages/4_Level_4_3D_Regression.py", title="L4: 3D Regression", icon="🧊"),
            st.Page("pages/5_Level_5_High_Dimensional.py", title="L5: High-Dimensional", icon="🌌"),
            st.Page("pages/6_Level_6_PCA.py", title="L6: PCA", icon="📉"),
            st.Page("pages/7_Level_7_Data_Cleaning.py", title="L7: Data Cleaning", icon="🧹"),
            st.Page("pages/8_Level_8_Feature_Engineering.py", title="L8: Feature Engineering", icon="⚗️"),
            st.Page("pages/9_Level_9_Regularization.py", title="L9: Regularization", icon="🛡️"),
            st.Page("pages/10_Level_10_The_Final_Boss.py", title="L10: The Final Boss", icon="👑"),
        ],
    }
    
    pg = st.navigation(pages)
    pg.run()

if __name__ == "__main__":
    main()
