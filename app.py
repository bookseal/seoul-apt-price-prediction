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
    """)
    
    # Main concept diagram
    st.markdown("""
    ```mermaid
    flowchart LR
        subgraph Pipeline["Every Level = Complete Pipeline"]
            A[📊 Data] --> B[🧮 Method] --> C[🔮 Prediction]
        end
        
        style Pipeline fill:#e8f5e9,stroke:#4caf50,stroke-width:2px
    ```
    """)
    
    st.info("**The difference between levels is COMPLEXITY, not steps!**")
    
    st.markdown("---")
    
    # Level Overview
    st.markdown("## 📊 Level Overview")
    
    # Level 1
    st.markdown("### 🎯 Level 1: Heuristic (No ML)")
    
    st.markdown("""
    ```mermaid
    flowchart LR
        subgraph L1["Level 1: Heuristic"]
            D1[("🏠 Data<br/>100K apartments")] --> M1["📍 Group by<br/>District"]
            M1 --> C1["📊 Calculate<br/>Median $/m²"]
            C1 --> P1["🔮 Price =<br/>Median × Area"]
        end
        
        style L1 fill:#e8f5e9,stroke:#4caf50,stroke-width:2px
    ```
    """)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **Method**: District Median × Area
        
        - No machine learning needed
        - Uses location as the key factor
        - Simplest possible baseline
        """)
    with col2:
        st.success("✅ Start here if new to ML!")
    
    st.markdown("---")
    
    # Level 2
    st.markdown("### 📐 Level 2: Linear Regression (Single Feature)")
    
    st.markdown("""
    ```mermaid
    flowchart LR
        subgraph L2["Level 2: Linear Regression"]
            D2[("🏠 Data<br/>Area, Price")] --> T2["🎓 Train<br/>Find w, b"]
            T2 --> F2["📐 Formula<br/>y = wx + b"]
            F2 --> P2["🔮 Price =<br/>w×Area + b"]
        end
        
        style L2 fill:#e3f2fd,stroke:#2196f3,stroke-width:2px
    ```
    """)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **Method**: Linear Regression with Area only
        
        - First machine learning model
        - Learns from data automatically
        - Single feature: exclusive area
        """)
    with col2:
        st.info("📐 First ML model!")
    
    st.markdown("---")
    
    # Level 3
    st.markdown("### 🚀 Level 3: Multi-Feature Model")
    
    st.markdown("""
    ```mermaid
    flowchart LR
        subgraph L3["Level 3: Multiple Features"]
            D3[("🏠 Data<br/>Area, District,<br/>Floor")] --> E3["⚙️ Encode<br/>District→Number"]
            E3 --> T3["🎓 Train<br/>Find w₁,w₂,w₃,b"]
            T3 --> P3["🔮 Price =<br/>w₁×Area +<br/>w₂×District +<br/>w₃×Floor + b"]
        end
        
        style L3 fill:#f3e5f5,stroke:#9c27b0,stroke-width:2px
    ```
    """)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **Method**: Linear Regression with Multiple Features
        
        - Area + District + Floor
        - Model learns importance of each
        - Better predictions!
        """)
    with col2:
        st.warning("🚀 More features = Better accuracy!")
    
    st.markdown("---")
    
    # Progression diagram
    st.markdown("## 📈 Learning Progression")
    
    st.markdown("""
    ```mermaid
    flowchart TB
        subgraph Journey["Your ML Journey"]
            L1["🎯 Level 1<br/>Heuristic<br/>No ML"] --> L2["📐 Level 2<br/>Linear Regression<br/>1 Feature"]
            L2 --> L3["🚀 Level 3<br/>Multi-Feature<br/>3 Features"]
            L3 --> L4["🔥 Level 4+<br/>Coming Soon<br/>Advanced ML"]
        end
        
        style L1 fill:#c8e6c9,stroke:#4caf50
        style L2 fill:#bbdefb,stroke:#2196f3
        style L3 fill:#e1bee7,stroke:#9c27b0
        style L4 fill:#ffe0b2,stroke:#ff9800
    ```
    """)
    
    st.markdown("""
    ---
    
    ## 🎓 Learning Philosophy
    
    **"Start Simple, Scale Smart"**
    
    | Level | Complexity | Features | ML? |
    |-------|------------|----------|-----|
    | 1 | ⭐ | District + Area | ❌ |
    | 2 | ⭐⭐ | Area only | ✅ |
    | 3 | ⭐⭐⭐ | Area + District + Floor | ✅ |
    | 4+ | ⭐⭐⭐⭐ | Many + Engineering | ✅✅ |
    
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
