# -*- coding: utf-8 -*-
"""
Seoul Apartment Price Prediction - ML Learning Roadmap

End-to-end machine learning journey from simple heuristics to advanced models.
Each level is a complete pipeline with increasing complexity.

Usage:
    streamlit run app.py
"""
import streamlit as st


def home_page():
    """Render the home page."""
    st.title("📚 Seoul Apartment Price Prediction")
    st.markdown("### End-to-End ML Learning Roadmap")
    
    st.markdown("""
    Welcome! This project teaches machine learning through **apartment price prediction**.
    
    ---
    
    ## 🎯 Key Concept: Each Level is End-to-End
    
    Every level is a **complete pipeline**:
    
    **Data → Method → Prediction**
    
    The difference is **complexity** - start simple, then improve!
    
    ---
    
    ## 📊 Level Overview
    """)
    
    # Level cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style="padding: 20px; background: rgba(76, 175, 80, 0.1); 
                    border-radius: 10px; border-left: 4px solid #4CAF50; height: 200px;">
        <h3>🎯 Level 1</h3>
        <p><b>Heuristic</b></p>
        <p style="font-size: 14px;">
        District Median × Area<br/>
        No ML needed<br/>
        Simplest baseline
        </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="padding: 20px; background: rgba(33, 150, 243, 0.1); 
                    border-radius: 10px; border-left: 4px solid #2196F3; height: 200px;">
        <h3>📐 Level 2</h3>
        <p><b>Linear Regression</b></p>
        <p style="font-size: 14px;">
        Area only<br/>
        First ML model<br/>
        Single feature
        </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="padding: 20px; background: rgba(156, 39, 176, 0.1); 
                    border-radius: 10px; border-left: 4px solid #9C27B0; height: 200px;">
        <h3>🚀 Level 3</h3>
        <p><b>Multi-Feature LR</b></p>
        <p style="font-size: 14px;">
        Area + District + Floor<br/>
        Multiple features<br/>
        Better accuracy
        </p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("""
    ---
    
    ## 🎓 Learning Philosophy
    
    **"Start Simple, Scale Smart"**
    
    1. **Level 1**: No ML - just statistics
    2. **Level 2**: Simplest ML possible
    3. **Level 3**: Add features to improve
    4. **Level 4+**: Advanced techniques (coming soon)
    
    Each level builds on the previous one. Don't skip ahead!
    
    ---
    
    ## 🚀 Quick Start
    
    👈 **Select a Level from the sidebar to begin!**
    
    Start with **Level 1** if you're new to ML.
    
    ---
    
    **Built with** Streamlit · Python · scikit-learn
    
    [📚 GitHub](https://github.com/bookseal/seoul-apt-price-prediction)
    """)


def main() -> None:
    """Entry point with navigation."""
    st.set_page_config(
        page_title="ML Roadmap - Seoul Apt Price",
        page_icon="📚",
        layout="wide",
    )
    
    # Define pages with clear level structure
    pages = {
        "🏠 Home": [
            st.Page(home_page, title="Welcome", icon="🏠"),
        ],
        "📊 Levels": [
            st.Page("pages/1_Level_1_Heuristic.py", title="Level 1: Heuristic", icon="🎯"),
            st.Page("pages/2_Level_2_Linear_Regression.py", title="Level 2: Linear Regression", icon="📐"),
            st.Page("pages/3_Level_3_Multi_Features.py", title="Level 3: Multi-Features", icon="🚀"),
        ],
    }
    
    pg = st.navigation(pages)
    pg.run()


if __name__ == "__main__":
    main()
