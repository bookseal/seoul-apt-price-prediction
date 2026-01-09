# -*- coding: utf-8 -*-
"""
Seoul Apartment Price Prediction - ML Learning Roadmap

End-to-end machine learning journey from simple heuristics to advanced models.
Each level is a complete pipeline with increasing complexity.

Usage:
    streamlit run app.py
"""
import streamlit as st


def render_flow_box(emoji: str, title: str, subtitle: str, color: str) -> str:
    """Create a styled flow box."""
    return f"""
    <div style="text-align: center; padding: 15px; 
                background: rgba({color}, 0.15); border-radius: 10px; 
                border: 2px solid rgba({color}, 0.5); min-width: 100px;">
        <div style="font-size: 24px;">{emoji}</div>
        <div style="font-weight: bold; font-size: 13px;">{title}</div>
        <div style="font-size: 11px; color: gray;">{subtitle}</div>
    </div>
    """


def render_arrow() -> str:
    """Create an arrow element."""
    return """
    <div style="text-align: center; padding: 10px; font-size: 24px; color: #666;">
        →
    </div>
    """


def home_page():
    """Render the home page."""
    st.title("📚 Seoul Apartment Price Prediction")
    st.markdown("### End-to-End ML Learning Roadmap")
    
    st.markdown("""
    Welcome! This project teaches machine learning through **apartment price prediction**.
    
    ---
    
    ## 🎯 Key Concept: Each Level is End-to-End
    """)
    
    # Main pipeline concept
    st.markdown("""
    <div style="display: flex; align-items: center; justify-content: center; 
                gap: 10px; padding: 20px; background: rgba(76, 175, 80, 0.1); 
                border-radius: 15px; border: 2px solid #4CAF50; margin: 20px 0;">
        <div style="text-align: center; padding: 15px;">
            <div style="font-size: 30px;">📊</div>
            <div style="font-weight: bold;">Data</div>
        </div>
        <div style="font-size: 30px; color: #4CAF50;">→</div>
        <div style="text-align: center; padding: 15px;">
            <div style="font-size: 30px;">🧮</div>
            <div style="font-weight: bold;">Method</div>
        </div>
        <div style="font-size: 30px; color: #4CAF50;">→</div>
        <div style="text-align: center; padding: 15px;">
            <div style="font-size: 30px;">🔮</div>
            <div style="font-weight: bold;">Prediction</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.info("**The difference between levels is COMPLEXITY, not steps!**")
    
    st.markdown("---")
    
    # Level Overview
    st.markdown("## 📊 Level Overview")
    
    # ========== LEVEL 1 ==========
    st.markdown("### 🎯 Level 1: Heuristic (No ML)")
    
    st.markdown("""
    <div style="display: flex; align-items: center; justify-content: center; 
                flex-wrap: wrap; gap: 8px; padding: 20px; 
                background: rgba(76, 175, 80, 0.08); border-radius: 15px; 
                border-left: 4px solid #4CAF50; margin: 15px 0;">
        <div style="text-align: center; padding: 12px; background: rgba(76, 175, 80, 0.15); 
                    border-radius: 10px; min-width: 80px;">
            <div style="font-size: 20px;">🏠</div>
            <div style="font-size: 11px; font-weight: bold;">Data</div>
            <div style="font-size: 9px; color: gray;">100K apts</div>
        </div>
        <div style="font-size: 20px; color: #4CAF50;">→</div>
        <div style="text-align: center; padding: 12px; background: rgba(76, 175, 80, 0.15); 
                    border-radius: 10px; min-width: 80px;">
            <div style="font-size: 20px;">📍</div>
            <div style="font-size: 11px; font-weight: bold;">Group</div>
            <div style="font-size: 9px; color: gray;">by District</div>
        </div>
        <div style="font-size: 20px; color: #4CAF50;">→</div>
        <div style="text-align: center; padding: 12px; background: rgba(76, 175, 80, 0.15); 
                    border-radius: 10px; min-width: 80px;">
            <div style="font-size: 20px;">📊</div>
            <div style="font-size: 11px; font-weight: bold;">Median</div>
            <div style="font-size: 9px; color: gray;">$/m² calc</div>
        </div>
        <div style="font-size: 20px; color: #4CAF50;">→</div>
        <div style="text-align: center; padding: 12px; background: rgba(76, 175, 80, 0.25); 
                    border-radius: 10px; min-width: 100px; border: 2px solid #4CAF50;">
            <div style="font-size: 20px;">🔮</div>
            <div style="font-size: 11px; font-weight: bold;">Price</div>
            <div style="font-size: 9px; color: #4CAF50;">Median × Area</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Method**: `Price = Median($/m² by district) × Area`")
        st.markdown("- No machine learning needed\n- Uses location as the key factor\n- Simplest possible baseline")
    with col2:
        st.success("✅ Start here!")
    
    st.markdown("---")
    
    # ========== LEVEL 2 ==========
    st.markdown("### 📐 Level 2: Linear Regression (Single Feature)")
    
    st.markdown("""
    <div style="display: flex; align-items: center; justify-content: center; 
                flex-wrap: wrap; gap: 8px; padding: 20px; 
                background: rgba(33, 150, 243, 0.08); border-radius: 15px; 
                border-left: 4px solid #2196F3; margin: 15px 0;">
        <div style="text-align: center; padding: 12px; background: rgba(33, 150, 243, 0.15); 
                    border-radius: 10px; min-width: 80px;">
            <div style="font-size: 20px;">🏠</div>
            <div style="font-size: 11px; font-weight: bold;">Data</div>
            <div style="font-size: 9px; color: gray;">Area, Price</div>
        </div>
        <div style="font-size: 20px; color: #2196F3;">→</div>
        <div style="text-align: center; padding: 12px; background: rgba(33, 150, 243, 0.15); 
                    border-radius: 10px; min-width: 80px;">
            <div style="font-size: 20px;">🎓</div>
            <div style="font-size: 11px; font-weight: bold;">Train</div>
            <div style="font-size: 9px; color: gray;">Find w, b</div>
        </div>
        <div style="font-size: 20px; color: #2196F3;">→</div>
        <div style="text-align: center; padding: 12px; background: rgba(33, 150, 243, 0.15); 
                    border-radius: 10px; min-width: 80px;">
            <div style="font-size: 20px;">📐</div>
            <div style="font-size: 11px; font-weight: bold;">Formula</div>
            <div style="font-size: 9px; color: gray;">y = wx + b</div>
        </div>
        <div style="font-size: 20px; color: #2196F3;">→</div>
        <div style="text-align: center; padding: 12px; background: rgba(33, 150, 243, 0.25); 
                    border-radius: 10px; min-width: 100px; border: 2px solid #2196F3;">
            <div style="font-size: 20px;">🔮</div>
            <div style="font-size: 11px; font-weight: bold;">Price</div>
            <div style="font-size: 9px; color: #2196F3;">w×Area + b</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Method**: `Price = w × Area + b` (learned from data)")
        st.markdown("- First machine learning model\n- Learns from data automatically\n- Single feature: exclusive area")
    with col2:
        st.info("📐 First ML!")
    
    st.markdown("---")
    
    # ========== LEVEL 3 ==========
    st.markdown("### 🚀 Level 3: Multi-Feature Model")
    
    st.markdown("""
    <div style="display: flex; align-items: center; justify-content: center; 
                flex-wrap: wrap; gap: 8px; padding: 20px; 
                background: rgba(156, 39, 176, 0.08); border-radius: 15px; 
                border-left: 4px solid #9C27B0; margin: 15px 0;">
        <div style="text-align: center; padding: 12px; background: rgba(156, 39, 176, 0.15); 
                    border-radius: 10px; min-width: 80px;">
            <div style="font-size: 20px;">🏠</div>
            <div style="font-size: 11px; font-weight: bold;">Data</div>
            <div style="font-size: 9px; color: gray;">Area, Dist, Floor</div>
        </div>
        <div style="font-size: 20px; color: #9C27B0;">→</div>
        <div style="text-align: center; padding: 12px; background: rgba(156, 39, 176, 0.15); 
                    border-radius: 10px; min-width: 80px;">
            <div style="font-size: 20px;">⚙️</div>
            <div style="font-size: 11px; font-weight: bold;">Encode</div>
            <div style="font-size: 9px; color: gray;">Dist→Num</div>
        </div>
        <div style="font-size: 20px; color: #9C27B0;">→</div>
        <div style="text-align: center; padding: 12px; background: rgba(156, 39, 176, 0.15); 
                    border-radius: 10px; min-width: 80px;">
            <div style="font-size: 20px;">🎓</div>
            <div style="font-size: 11px; font-weight: bold;">Train</div>
            <div style="font-size: 9px; color: gray;">w₁,w₂,w₃,b</div>
        </div>
        <div style="font-size: 20px; color: #9C27B0;">→</div>
        <div style="text-align: center; padding: 12px; background: rgba(156, 39, 176, 0.25); 
                    border-radius: 10px; min-width: 100px; border: 2px solid #9C27B0;">
            <div style="font-size: 20px;">🔮</div>
            <div style="font-size: 11px; font-weight: bold;">Price</div>
            <div style="font-size: 9px; color: #9C27B0;">Σ(wᵢ×xᵢ)+b</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Method**: `Price = w₁×Area + w₂×District + w₃×Floor + b`")
        st.markdown("- Multiple features combined\n- Model learns importance of each\n- Better predictions!")
    with col2:
        st.warning("🚀 Best accuracy!")
    
    st.markdown("---")
    
    # Learning progression
    st.markdown("## 📈 Learning Progression")
    
    st.markdown("""
    <div style="display: flex; justify-content: center; align-items: center; 
                gap: 15px; padding: 25px; flex-wrap: wrap;">
        <div style="text-align: center; padding: 20px; background: rgba(76, 175, 80, 0.15); 
                    border-radius: 15px; border: 3px solid #4CAF50; min-width: 120px;">
            <div style="font-size: 28px;">🎯</div>
            <div style="font-weight: bold; margin: 5px 0;">Level 1</div>
            <div style="font-size: 12px; color: #4CAF50;">Heuristic</div>
            <div style="font-size: 10px; color: gray;">No ML</div>
        </div>
        <div style="font-size: 30px; color: #666;">→</div>
        <div style="text-align: center; padding: 20px; background: rgba(33, 150, 243, 0.15); 
                    border-radius: 15px; border: 3px solid #2196F3; min-width: 120px;">
            <div style="font-size: 28px;">📐</div>
            <div style="font-weight: bold; margin: 5px 0;">Level 2</div>
            <div style="font-size: 12px; color: #2196F3;">Linear Reg</div>
            <div style="font-size: 10px; color: gray;">1 Feature</div>
        </div>
        <div style="font-size: 30px; color: #666;">→</div>
        <div style="text-align: center; padding: 20px; background: rgba(156, 39, 176, 0.15); 
                    border-radius: 15px; border: 3px solid #9C27B0; min-width: 120px;">
            <div style="font-size: 28px;">🚀</div>
            <div style="font-weight: bold; margin: 5px 0;">Level 3</div>
            <div style="font-size: 12px; color: #9C27B0;">Multi-Feature</div>
            <div style="font-size: 10px; color: gray;">3 Features</div>
        </div>
        <div style="font-size: 30px; color: #666;">→</div>
        <div style="text-align: center; padding: 20px; background: rgba(255, 152, 0, 0.15); 
                    border-radius: 15px; border: 3px solid #FF9800; min-width: 120px;">
            <div style="font-size: 28px;">🏆</div>
            <div style="font-weight: bold; margin: 5px 0;">Level 10</div>
            <div style="font-size: 12px; color: #FF9800;">AutoML</div>
            <div style="font-size: 10px; color: gray;">Finale!</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    ---
    
    ## 🎓 Full Roadmap
    
    | Level | Topic | Key Concept |
    |-------|-------|-------------|
    | 1 | Heuristic | Simple rule: median $/m² |
    | 2 | Linear Regression | y = wx + b |
    | 3 | Multi-Features | One-Hot Encoding |
    | 4 | 3D Regression | Building Year, Plotly |
    | 5 | High-Dimensional | 10+ features |
    | 6 | PCA | Dimensionality reduction |
    | 7 | Data Cleaning | Nulls, outliers |
    | 8 | Feature Engineering | Scaling, transforms |
    | 9 | Regularization | Ridge, Lasso |
    | 10 | AutoML | Model comparison |
    
    ---
    
    ## 🚀 Quick Start
    """)
    
    # Start button for Level 1
    st.markdown("""
    <div style="text-align: center; padding: 30px; margin: 20px 0;">
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🎯 Start Level 1: Heuristic", use_container_width=True, type="primary"):
            st.switch_page("pages/1_Level_1_Heuristic.py")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        if st.button("📐 Go to Level 2: Linear Regression", use_container_width=True):
            st.switch_page("pages/2_Level_2_Linear_Regression.py")
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    st.markdown("""
    👈 Or select any Level from the sidebar!
    
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
        "📊 Beginner": [
            st.Page("pages/1_Level_1_Heuristic.py", title="Level 1: Heuristic", icon="🎯"),
            st.Page("pages/2_Level_2_Linear_Regression.py", title="Level 2: Linear Regression", icon="📐"),
            st.Page("pages/3_Level_3_Multi_Features.py", title="Level 3: Multi-Features", icon="🏘️"),
        ],
        "📈 Intermediate": [
            st.Page("pages/4_Level_4_3D_Regression.py", title="Level 4: 3D Regression", icon="🎯"),
            st.Page("pages/5_Level_5_High_Dimensional.py", title="Level 5: High-Dimensional", icon="🌌"),
            st.Page("pages/6_Level_6_PCA.py", title="Level 6: PCA", icon="📉"),
        ],
        "🚀 Advanced": [
            st.Page("pages/7_Level_7_Data_Cleaning.py", title="Level 7: Data Cleaning", icon="🧹"),
            st.Page("pages/8_Level_8_Feature_Engineering.py", title="Level 8: Feature Engineering", icon="⚗️"),
            st.Page("pages/9_Level_9_Regularization.py", title="Level 9: Regularization", icon="🛡️"),
            st.Page("pages/10_Level_10_AutoML.py", title="Level 10: AutoML", icon="🏆"),
        ],
    }
    
    pg = st.navigation(pages)
    pg.run()


if __name__ == "__main__":
    main()
