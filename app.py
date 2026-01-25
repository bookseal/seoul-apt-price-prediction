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
    
    st.title("🚀 The ML Engineer Roadmap")
    st.markdown("### From Beginner to MLOps Architect")
    
    st.markdown("""
    Welcome to the ultimate machine learning curriculum. 
    This platform bridges the gap between **Data Science theory** and **Real-world MLOps systems**.
    """)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="card-container">
            <div class="track-title highlight-blue">🏙️ Part 1: Data Science</div>
            <p class="track-desc">
                Dive deep into the mathematics and intuition of modeling. 
                Build the ultimate predictor for <b>Seoul Apartment Prices</b>.
            </p>
            <ul class="feature-list">
                <li>🎯 <b>Level 1-4</b>: Foundational Regression</li>
                <li>🌌 <b>Level 5-6</b>: High-Dimensional Spaces & PCA</li>
                <li>🛡️ <b>Level 7-9</b>: Robustness & Regularization</li>
                <li>👑 <b>Level 10</b>: The Mathematical Limit</li>
            </ul>
            <p style="text-align: center; color: #888; font-style: italic; margin-top: 20px;">
                "I want to build the most accurate model possible."
            </p>
        </div>
        """, unsafe_allow_html=True)
        st.write("") # Spacer
        if st.button("Start Part 1: Modeling Track", type="primary", use_container_width=True):
            st.switch_page("pages/1_Level_1_Heuristic.py")

    with col2:
        st.markdown("""
        <div class="card-container">
            <div class="track-title highlight-pink">🛍️ Part 2: MLOps</div>
            <p class="track-desc">
                Take a model out of the notebook and into production.
                Automate the trend analysis for <b>Yo-Zeum Seongsu</b>.
            </p>
            <ul class="feature-list">
                <li>🏗️ <b>Level 11-13</b>: Scalable Data Pipelines</li>
                <li>🤖 <b>Level 14-16</b>: AutoML & CI/CD Automation</li>
                <li>🧪 <b>Level 17-18</b>: Experiment Tracking (MLflow)</li>
                <li>🏭 <b>Level 19-20</b>: Orchestration (Airflow)</li>
            </ul>
            <p style="text-align: center; color: #888; font-style: italic; margin-top: 20px;">
                "I want to build systems that run themselves."
            </p>
        </div>
        """, unsafe_allow_html=True)
        st.write("") # Spacer
        if st.button("Start Part 2: MLOps Track", use_container_width=True):
            st.switch_page("pages/11_MLOps_Lv1_Data.py")

    st.markdown("---")
    
    with st.expander("🗺️ View Full Curriculum Detail"):
        tab1, tab2 = st.tabs(["Part 1: The Modeling Journey", "Part 2: The Systems Journey"])
        
        with tab1:
            st.dataframe([
                {"Level": 1, "Topic": "Heuristic", "Goal": "Beat the average", "Description": "Baseline model using simple rules (e.g. mean price by district).", "Tech": "Pandas, NumPy"},
                {"Level": 2, "Topic": "Linear Regression", "Goal": "Draw the best line", "Description": "Introduction to OLS, loss functions, and basic training.", "Tech": "Scikit-Learn"},
                {"Level": 3, "Topic": "Multi-Features", "Goal": "Use multiple signals", "Description": "Moving from univariate to multivariate regression.", "Tech": "Scikit-Learn"},
                {"Level": 4, "Topic": "3D Regression", "Goal": "Visualize complexity", "Description": "Understanding hyperplanes and 3D visualization of errors.", "Tech": "Plotly"},
                {"Level": 5, "Topic": "High Dimensionality", "Goal": "Handle many features", "Description": "The curse of dimensionality and feature importance.", "Tech": "Scikit-Learn"},
                {"Level": 6, "Topic": "PCA", "Goal": "Dimensionality Reduction", "Description": "Principal Component Analysis to compress features.", "Tech": "PCA"},
                {"Level": 7, "Topic": "Data Cleaning", "Goal": "Fix outliers & skews", "Description": "Handling missing values, outliers (IQR), and log-transforms.", "Tech": "Pandas"},
                {"Level": 8, "Topic": "Feature Engineering", "Goal": "Create new signals", "Description": "Interaction terms, polynomial features, and domain knowledge.", "Tech": "Feature Engines"},
                {"Level": 9, "Topic": "Regularization", "Goal": "Tame complexity", "Description": "Ridge, Lasso, and ElasticNet to prevent overfitting.", "Tech": "Regularization"},
                {"Level": 10, "Topic": "The Final Boss", "Goal": "Mathematical Perfection", "Description": "Combining all techniques to reach the limit of linear modeling.", "Tech": "Pipeline, GridSearch"},
            ], use_container_width=True)
            
        with tab2:
            st.dataframe([
                {"Level": 11, "Topic": "Data Creation", "Goal": "Build a dataset", "Description": "Generating synthetic trend data for 'Yo-Zeum Seongsu'.", "Tech": "Faker, Pandas"},
                {"Level": 12, "Topic": "Preprocessing", "Goal": "Scalable cleaning", "Description": "Building robust preprocessing pipelines for production.", "Tech": "Scikit-Learn Pipelines"},
                {"Level": 13, "Topic": "Predictor", "Goal": "Interactive Dashboards", "Description": "Serving models via Streamlit for end-users.", "Tech": "Streamlit"},
                {"Level": 14, "Topic": "AutoML", "Goal": "Model Selection", "Description": "Automated model comparison and selection (PyCaret/AutoGluon style).", "Tech": "AutoML"},
                {"Level": 15, "Topic": "Automation", "Goal": "Scheduling Scripts", "Description": "Writing scripts to run jobs automatically (Crontab basics).", "Tech": "Python Scripts"},
                {"Level": 16, "Topic": "CI/CD", "Goal": "GitHub Actions", "Description": "Continuous Integration and Deployment for ML.", "Tech": "GitHub Actions"},
                {"Level": 17, "Topic": "MLflow", "Goal": "Experiment Tracking", "Description": "Logging params, metrics, and artifacts.", "Tech": "MLflow"},
                {"Level": 18, "Topic": "DVC", "Goal": "Data Versioning", "Description": "Versioning large datasets like code (Data Version Control).", "Tech": "DVC"},
                {"Level": 19, "Topic": "Monitoring", "Goal": "Drift Detection", "Description": "Detecting concept drift and data quality issues.", "Tech": "Evidently AI"},
                {"Level": 20, "Topic": "Airflow", "Goal": "Complex DAGs", "Description": "Orchestrating complex ML workflows with DAGs.", "Tech": "Apache Airflow"},
            ], use_container_width=True)

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
            st.Page("pages/10_Level_10_The_Final_Boss.py", title="L10: The Final Boss", icon="👑"),
        ],
        "🛍️ Part 2: MLOps (Seongsu)": [
            st.Page("pages/11_MLOps_Lv1_Data.py", title="L11: Data Creation", icon="🏗️"),
            st.Page("pages/12_MLOps_Lv2_Preprocessing.py", title="L12: Preprocessing", icon="⚙️"),
            st.Page("pages/13_MLOps_Lv3_Predictor.py", title="L13: Predictor", icon="📱"),
            st.Page("pages/14_MLOps_Lv4_AutoML.py", title="L14: AutoML & RMSE", icon="🏎️"),
            st.Page("pages/15_MLOps_Lv5_Automation.py", title="L15: Automation", icon="🤖"),
            st.Page("pages/16_MLOps_Lv6_CICD.py", title="L16: CI/CD", icon="♾️"),
            st.Page("pages/17_MLOps_Lv7_MLflow.py", title="L17: MLflow", icon="🧪"),
            st.Page("pages/18_MLOps_Lv8_DVC.py", title="L18: DVC", icon="💾"),
            st.Page("pages/19_MLOps_Lv9_Monitoring.py", title="L19: Monitoring", icon="🐳"),
            st.Page("pages/20_MLOps_Lv10_Airflow.py", title="L20: Airflow", icon="🏭"),
        ],
    }
    
    pg = st.navigation(pages)
    pg.run()

if __name__ == "__main__":
    main()
