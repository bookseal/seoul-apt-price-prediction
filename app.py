# -*- coding: utf-8 -*-
"""
Seoul Apartment Price Prediction - Learning Roadmap

A step-by-step guide to learn machine learning through apartment price prediction.
Built for beginners who want to become AI developers.

Usage:
    streamlit run app.py
"""
import streamlit as st


def render_home() -> None:
    """Render the home page with learning roadmap."""
    st.set_page_config(
        page_title="Seoul Apt Price Prediction - ML Roadmap",
        page_icon="📚",
        layout="wide",
    )
    
    st.title("📚 Seoul Apartment Price Prediction")
    st.markdown("### Your Journey to Become an AI Developer")
    
    st.markdown("""
    Welcome! This project guides you through machine learning step by step.
    Start from **Level 1** and work your way up to becoming an ML practitioner.
    """)
    
    st.markdown("---")
    
    # Level 1 Section
    st.header("📖 Level 1: Understanding Data")
    st.markdown("""
    Before building models, you need to understand your data.
    Learn the fundamentals of data exploration and analysis.
    """)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style="padding: 20px; background: rgba(33, 150, 243, 0.1); 
                    border-radius: 10px; border-left: 4px solid #2196F3;">
        <h4>📂 1.1 Explore Data</h4>
        <p style="font-size: 14px;">Learn to examine dataset structure, 
        understand columns, and read basic statistics.</p>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/1_1_Explore_Data.py", label="Start Chapter →")
    
    with col2:
        st.markdown("""
        <div style="padding: 20px; background: rgba(33, 150, 243, 0.1); 
                    border-radius: 10px; border-left: 4px solid #2196F3;">
        <h4>📊 1.2 EDA</h4>
        <p style="font-size: 14px;">Discover patterns through visualizations. 
        Learn about distributions and correlations.</p>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/1_2_EDA.py", label="Start Chapter →")
    
    with col3:
        st.markdown("""
        <div style="padding: 20px; background: rgba(33, 150, 243, 0.1); 
                    border-radius: 10px; border-left: 4px solid #2196F3;">
        <h4>🎲 1.3 Sampling</h4>
        <p style="font-size: 14px;">Learn efficient data handling with 
        stratified sampling and Parquet format.</p>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/1_3_Sampling.py", label="Start Chapter →")
    
    st.markdown("---")
    
    # Level 2 Section
    st.header("🤖 Level 2: Building Your First Model")
    st.markdown("""
    Time to build your first machine learning model!
    Learn the fundamentals of Linear Regression.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 20px; background: rgba(76, 175, 80, 0.1); 
                    border-radius: 10px; border-left: 4px solid #4CAF50;">
        <h4>🎯 2.1 Feature Selection</h4>
        <p style="font-size: 14px;">Learn to choose the right features. 
        Understand correlation and feature importance.</p>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/2_1_Feature_Selection.py", label="Start Chapter →")
    
    with col2:
        st.markdown("""
        <div style="padding: 20px; background: rgba(76, 175, 80, 0.1); 
                    border-radius: 10px; border-left: 4px solid #4CAF50;">
        <h4>📐 2.2 Linear Regression</h4>
        <p style="font-size: 14px;">Build your first ML model. 
        Understand the math behind predictions.</p>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/2_2_Linear_Regression.py", label="Start Chapter →")
    
    col3, col4 = st.columns(2)
    
    with col3:
        st.markdown("""
        <div style="padding: 20px; background: rgba(76, 175, 80, 0.1); 
                    border-radius: 10px; border-left: 4px solid #4CAF50;">
        <h4>📏 2.3 Model Evaluation</h4>
        <p style="font-size: 14px;">Learn to measure performance with RMSE. 
        Analyze residuals and errors.</p>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/2_3_Model_Evaluation.py", label="Start Chapter →")
    
    with col4:
        st.markdown("""
        <div style="padding: 20px; background: rgba(76, 175, 80, 0.1); 
                    border-radius: 10px; border-left: 4px solid #4CAF50;">
        <h4>🔮 2.4 Prediction Demo</h4>
        <p style="font-size: 14px;">Use your model to make predictions. 
        See it in action!</p>
        </div>
        """, unsafe_allow_html=True)
        st.page_link("pages/2_4_Prediction_Demo.py", label="Start Chapter →")
    
    st.markdown("---")
    
    # Coming Soon Section
    st.header("🚀 Coming Soon")
    
    st.markdown("""
    <div style="padding: 20px; background: rgba(156, 39, 176, 0.1); 
                border-radius: 10px; border-left: 4px solid #9C27B0; opacity: 0.7;">
    <h4>Level 3: Advanced Models</h4>
    <p>• Multiple features & Feature Engineering<br/>
    • Tree-based models (Random Forest, XGBoost)<br/>
    • Cross-validation & Hyperparameter tuning</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<br/>", unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 20px; background: rgba(255, 152, 0, 0.1); 
                border-radius: 10px; border-left: 4px solid #FF9800; opacity: 0.7;">
    <h4>Level 4: Expert Techniques</h4>
    <p>• Ensemble methods & Stacking<br/>
    • Deep Learning approaches<br/>
    • MLOps & Model deployment</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Footer
    st.markdown("""
    ### 🎯 Learning Philosophy
    
    **"Start Simple, Scale Smart"**
    
    - Begin with the simplest working model
    - Understand why it works (or doesn't)
    - Gradually add complexity
    - Learn from each iteration
    
    ---
    
    **Built with** Streamlit · Python · scikit-learn · Pandas
    
    [📚 GitHub Repository](https://github.com/bookseal/seoul-apt-price-prediction)
    """)


def main() -> None:
    """Entry point for Streamlit."""
    render_home()


if __name__ == "__main__":
    main()
