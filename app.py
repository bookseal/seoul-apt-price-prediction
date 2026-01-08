# -*- coding: utf-8 -*-
"""
Seoul Apartment Price Prediction - Learning Roadmap

A step-by-step guide to learn machine learning through apartment price prediction.
Built for beginners who want to become AI developers.

Usage:
    streamlit run app.py
"""
import streamlit as st


def home_page():
    """Render the home page with learning roadmap."""
    st.title("📚 Seoul Apartment Price Prediction")
    st.markdown("### Your Journey to Become an AI Developer")
    
    st.markdown("""
    Welcome! This project guides you through machine learning step by step.
    Start from **Level 1** and work your way up to becoming an ML practitioner.
    
    ---
    
    ### 🎯 Learning Philosophy
    
    **"Start Simple, Scale Smart"**
    
    - Begin with the simplest working model
    - Understand why it works (or doesn't)
    - Gradually add complexity
    - Learn from each iteration
    
    ---
    
    ### 📖 How to Use This Guide
    
    1. **Follow the sidebar** - chapters are ordered for progressive learning
    2. **Read the explanations** - understand concepts before code
    3. **Try the quizzes** - test your understanding
    4. **Experiment** - modify code and see what happens
    
    ---
    
    ### 🗺️ Roadmap Overview
    
    | Level | Focus | What You'll Learn |
    |-------|-------|-------------------|
    | **Level 1** | Data | Exploration, EDA, Sampling |
    | **Level 2** | First Model | Linear Regression, RMSE |
    | **Level 3** | Better Models | Trees, Tuning (Coming Soon) |
    | **Level 4** | Production | MLOps, Deploy (Coming Soon) |
    
    ---
    
    **Built with** Streamlit · Python · scikit-learn · Pandas
    
    [📚 GitHub Repository](https://github.com/bookseal/seoul-apt-price-prediction)
    """)


def main() -> None:
    """Entry point with navigation."""
    st.set_page_config(
        page_title="ML Roadmap - Seoul Apt Price",
        page_icon="📚",
        layout="wide",
    )
    
    # Define pages with sections
    pages = {
        "🏠 Home": [
            st.Page(home_page, title="Welcome", icon="🏠"),
        ],
        "📖 Level 1: Understanding Data": [
            st.Page("pages/1-1_📂_Data_Overview.py", title="1.1 Data Overview", icon="📂"),
            st.Page("pages/1-2_📊_Charts_and_Patterns.py", title="1.2 Charts & Patterns", icon="📊"),
            st.Page("pages/1-3_🎲_Sampling_101.py", title="1.3 Sampling 101", icon="🎲"),
        ],
        "🤖 Level 2: First ML Model": [
            st.Page("pages/2-1_🎯_Choose_Features.py", title="2.1 Choose Features", icon="🎯"),
            st.Page("pages/2-2_📐_First_ML_Model.py", title="2.2 Linear Regression", icon="📐"),
            st.Page("pages/2-3_📏_Is_It_Good.py", title="2.3 Model Evaluation", icon="📏"),
            st.Page("pages/2-4_🔮_Try_Predictions.py", title="2.4 Try Predictions", icon="🔮"),
        ],
    }
    
    pg = st.navigation(pages)
    pg.run()


if __name__ == "__main__":
    main()
