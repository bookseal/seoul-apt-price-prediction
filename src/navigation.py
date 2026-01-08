# -*- coding: utf-8 -*-
"""
Navigation helper functions for the ML Roadmap.
"""
import streamlit as st


# Level information for next level navigation
LEVEL_INFO = {
    1: {
        "next": 2,
        "next_title": "Level 2: Linear Regression",
        "next_icon": "📐",
        "next_page": "pages/2_Level_2_Linear_Regression.py",
        "teaser": "Same area, different district = Same price? That can't be right!",
        "solution": "Let the computer **learn** the best formula from data!"
    },
    2: {
        "next": 3,
        "next_title": "Level 3: Multi-Features",
        "next_icon": "🏘️",
        "next_page": "pages/3_Level_3_Multi_Features.py",
        "teaser": "Gangnam and Nowon get the same prediction? Location matters!",
        "solution": "Add **District** as a feature with One-Hot Encoding!"
    },
    3: {
        "next": 4,
        "next_title": "Level 4: 3D Regression",
        "next_icon": "🎯",
        "next_page": "pages/4_Level_4_3D_Regression.py",
        "teaser": "A 30-year-old apartment = Brand new apartment? Age matters!",
        "solution": "Add **Building Year** and visualize in 3D!"
    },
    4: {
        "next": 5,
        "next_title": "Level 5: High-Dimensional",
        "next_icon": "🌌",
        "next_page": "pages/5_Level_5_High_Dimensional.py",
        "teaser": "Only 3 features? There are SO many more factors affecting price!",
        "solution": "Use **10+ features** - but can we even visualize that?"
    },
    5: {
        "next": 6,
        "next_title": "Level 6: PCA",
        "next_icon": "📉",
        "next_page": "pages/6_Level_6_PCA.py",
        "teaser": "Can't visualize 10 dimensions? Too many features causing problems?",
        "solution": "**PCA** compresses dimensions while keeping information!"
    },
    6: {
        "next": 7,
        "next_title": "Level 7: Data Cleaning",
        "next_icon": "🧹",
        "next_page": "pages/7_Level_7_Data_Cleaning.py",
        "teaser": "What if the data has missing values? Or crazy outliers?",
        "solution": "Learn to **clean** your data before training!"
    },
    7: {
        "next": 8,
        "next_title": "Level 8: Feature Engineering",
        "next_icon": "⚗️",
        "next_page": "pages/8_Level_8_Feature_Engineering.py",
        "teaser": "Raw features might not be optimal. Can we create BETTER ones?",
        "solution": "**Feature Engineering** transforms data for better predictions!"
    },
    8: {
        "next": 9,
        "next_title": "Level 9: Regularization",
        "next_icon": "🛡️",
        "next_page": "pages/9_Level_9_Regularization.py",
        "teaser": "Too many features = Overfitting! The model memorizes, not learns.",
        "solution": "**Regularization** controls complexity and prevents overfitting!"
    },
    9: {
        "next": 10,
        "next_title": "Level 10: AutoML",
        "next_icon": "🏆",
        "next_page": "pages/10_Level_10_AutoML.py",
        "teaser": "We've only used Linear Regression. What about other models?",
        "solution": "**AutoML** compares many models automatically!"
    },
}


def display_next_level_teaser(current_level: int) -> None:
    """
    Display a teaser section that encourages users to go to the next level.
    
    Args:
        current_level: The current level number (1-9)
    """
    if current_level not in LEVEL_INFO:
        return
    
    info = LEVEL_INFO[current_level]
    
    st.markdown("---")
    
    # Teaser section with gradient background
    st.markdown(f"""
    <div style="padding: 25px; background: linear-gradient(135deg, rgba(255,152,0,0.15), rgba(255,87,34,0.15)); 
                border-radius: 15px; border: 2px solid #FF9800; margin: 20px 0;">
        <h3 style="margin-top: 0; color: #FF9800;">🤔 Wait... Something's Still Wrong!</h3>
        <p style="font-size: 16px; margin: 15px 0;">
            <b>{info['teaser']}</b>
        </p>
        <p style="font-size: 14px; color: #666; margin: 15px 0;">
            Curious how to fix this? 👀
        </p>
        <div style="background: rgba(76,175,80,0.15); padding: 15px; border-radius: 10px; margin-top: 15px;">
            <b style="color: #4CAF50;">✨ {info['next_title']} Solution:</b><br>
            <span style="font-size: 14px;">{info['solution']}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Big button to next level
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.page_link(
            info['next_page'],
            label=f"{info['next_icon']} Go to {info['next_title']} →",
            use_container_width=True
        )
    
    st.markdown("""
    <div style="text-align: center; color: #888; font-size: 12px; margin-top: 10px;">
        Ready to level up? Click the button above! 🚀
    </div>
    """, unsafe_allow_html=True)


def display_journey_complete() -> None:
    """Display the journey complete message for Level 10."""
    st.markdown("---")
    
    st.markdown("""
    <div style="padding: 30px; background: linear-gradient(135deg, rgba(76,175,80,0.2), rgba(33,150,243,0.2)); 
                border-radius: 15px; border: 3px solid #4CAF50; margin: 20px 0; text-align: center;">
        <h2 style="margin-top: 0;">🎉 Congratulations!</h2>
        <p style="font-size: 18px;">
            You've completed the entire ML Roadmap!
        </p>
        <p style="font-size: 14px; color: #666;">
            From simple heuristics to AutoML - you've come a long way!
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.page_link(
            "app.py",
            label="🏠 Back to Home",
            use_container_width=True
        )
