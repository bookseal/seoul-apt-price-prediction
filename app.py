# -*- coding: utf-8 -*-
"""
Seoul Apartment Price Prediction & Yo-Zeum Seongsu - ML Learning Roadmap

End-to-end machine learning journey:
Part 1: Static Modeling (Seoul Apartments)
Part 2: MLOps & Pipelines (Seongsu Trends)

Usage:
    streamlit run app.py
"""
import streamlit as st


def home_page():
    """Render the portal home page."""
    st.title("🚀 The ML Engineer Roadmap")
    
    st.markdown("""
    Welcome! This platform takes you from **Data Science Beginner** to **MLOps Engineer**.
    
    Choose your track:
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 20px; background: rgba(33, 150, 243, 0.1); 
                    border-radius: 15px; border: 2px solid #2196F3; height: 100%;">
            <h2 style="color: #2196F3;">🏙️ Part 1: Data Science</h2>
            <p><b>Project: Seoul Apartment Prices</b></p>
            <ul>
                <li>Linear Regression</li>
                <li>Feature Engineering</li>
                <li>Model Training</li>
                <li>AutoML</li>
            </ul>
            <p><i>"I want to build the best model."</i></p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("Start Part 1 (Level 1)", type="primary", use_container_width=True):
            st.switch_page("pages/1_Level_1_Heuristic.py")

    with col2:
        st.markdown("""
        <div style="padding: 20px; background: rgba(233, 30, 99, 0.1); 
                    border-radius: 15px; border: 2px solid #E91E63; height: 100%;">
            <h2 style="color: #E91E63;">🛍️ Part 2: MLOps</h2>
            <p><b>Project: Yo-Zeum Seongsu (Trends)</b></p>
            <ul>
                <li>Pipelines (Metaflow)</li>
                <li>Automation (Airflow)</li>
                <li>CI/CD & Testing</li>
                <li>Monitoring & Drift</li>
            </ul>
            <p><i>"I want to run the model automatically."</i></p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("Start Part 2 (Level 11)", use_container_width=True):
            st.switch_page("pages/11_Level_11_MLOps_Intro.py")

    st.markdown("---")
    st.markdown("### 🗺️ Full Curriculum")
    
    tab1, tab2 = st.tabs(["Part 1: Apartments", "Part 2: Seongsu"])
    
    with tab1:
        st.info("Levels 1-10: Mastering the Modeling Process")
        st.markdown("| Level | Topic | Key Concept |\n|---|---|---|\n| 1 | Heuristic | Rule-based |\n| 2 | Linear Reg | y=wx+b |\n| ... | ... | ... |\n| 10 | AutoML | Optimization |")
        
    with tab2:
        st.error("Levels 11-15: Mastering the Operations")
        st.markdown("| Level | Topic | Key Concept |\n|---|---|---|\n| 11 | Intro | Manual Pain |\n| 12 | Metaflow | Orchestration |\n| 13 | CI/CD | Testing |\n| 14 | Airflow | Automation |\n| 15 | Monitoring | Data Drift |")


def main() -> None:
    """Entry point with navigation."""
    st.set_page_config(
        page_title="ML Roadmap",
        page_icon="🚀",
        layout="wide",
    )
    
    # Define pages
    pages = {
        "🏠 Home": [
            st.Page(home_page, title="Portal", icon="🏠"),
        ],
        "🏙️ Part 1: Data Science": [
            st.Page("pages/1_Level_1_Heuristic.py", title="L1: Heuristic", icon="🎯"),
            st.Page("pages/2_Level_2_Linear_Regression.py", title="L2: Linear Regression", icon="📐"),
            st.Page("pages/3_Level_3_Multi_Features.py", title="L3: Multi-Features", icon="🏘️"),
            st.Page("pages/4_Level_4_3D_Regression.py", title="L4: 3D Regression", icon="🧊"),
            st.Page("pages/5_Level_5_High_Dimensional.py", title="L5: High-Dimensional", icon="🌌"),
            st.Page("pages/6_Level_6_PCA.py", title="L6: PCA", icon="📉"),
            st.Page("pages/7_Level_7_Data_Cleaning.py", title="L7: Data Cleaning", icon="🧹"),
            st.Page("pages/8_Level_8_Feature_Engineering.py", title="L8: Feature Engineering", icon="⚗️"),
            st.Page("pages/9_Level_9_Regularization.py", title="L9: Regularization", icon="🛡️"),
            st.Page("pages/10_Level_10_AutoML.py", title="L10: AutoML", icon="🏆"),
        ],
        "🛍️ Part 2: MLOps (Seongsu)": [
            st.Page("pages/11_Level_11_MLOps_Intro.py", title="L11: Why MLOps?", icon="😱"),
            st.Page("pages/12_Level_12_Orchestration.py", title="L12: Metaflow", icon="🌊"),
            st.Page("pages/13_Level_13_CI_CD.py", title="L13: CI/CD & Test", icon="🧪"),
            st.Page("pages/14_Level_14_Automation.py", title="L14: Airflow", icon="🌬️"),
            st.Page("pages/15_Level_15_Monitoring.py", title="L15: Monitoring", icon="👀"),
        ],
    }
    
    pg = st.navigation(pages)
    pg.run()


if __name__ == "__main__":
    main()
