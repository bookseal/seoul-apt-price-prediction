# -*- coding: utf-8 -*-
"""
RMSE comparison utilities across levels.
"""
import streamlit as st
import matplotlib.pyplot as plt
import numpy as np


# Baseline RMSE values for each level (approximate values)
# These are reference values - actual may vary slightly based on data sampling
LEVEL_RMSE = {
    1: {"name": "Heuristic", "rmse": 38000, "icon": "🎯"},
    2: {"name": "Linear (Area)", "rmse": 38311, "icon": "📐"},
    3: {"name": "Multi-Feature", "rmse": 35000, "icon": "🏘️"},
    4: {"name": "3D (+Year)", "rmse": 34000, "icon": "🎯"},
    5: {"name": "High-Dim", "rmse": 32666, "icon": "🌌"},
    6: {"name": "PCA", "rmse": 33330, "icon": "📉"},
    7: {"name": "Cleaned", "rmse": 25084, "icon": "🧹"},
    8: {"name": "Engineered", "rmse": 24184, "icon": "⚗️"},
    9: {"name": "Regularized", "rmse": 23903, "icon": "🛡️"},
    10: {"name": "AutoML (Black Box)", "rmse": 20004, "icon": "👑"},
}


def display_rmse_comparison(current_level: int, current_rmse: float) -> None:
    """
    Display RMSE comparison with previous levels.
    
    Args:
        current_level: Current level number (1-10)
        current_rmse: Current level's test RMSE
    """
    st.markdown("### 📊 RMSE Comparison Across Levels")
    
    # Prepare data
    levels = list(range(1, current_level + 1))
    rmse_values = []
    labels = []
    colors = []
    
    for lvl in levels:
        if lvl == current_level:
            rmse_values.append(current_rmse)
            labels.append(f"L{lvl}: {LEVEL_RMSE[lvl]['name']} (NOW)")
            colors.append('#4CAF50')  # Green for current
        else:
            rmse_values.append(LEVEL_RMSE[lvl]['rmse'])
            labels.append(f"L{lvl}: {LEVEL_RMSE[lvl]['name']}")
            colors.append('#2196F3')  # Blue for others
    
    # Create bar chart
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(labels, rmse_values, color=colors)
    
    # Add value labels
    for bar, val in zip(bars, rmse_values):
        ax.text(val + 500, bar.get_y() + bar.get_height()/2, 
                f'{val:,.0f}', va='center', fontsize=10)
    
    ax.set_xlabel('Test RMSE (lower is better)')
    ax.set_title(f'How Does Level {current_level} Compare?')
    ax.grid(True, alpha=0.3, axis='x')
    ax.invert_yaxis()
    
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Calculate improvement
    if current_level > 1:
        prev_rmse = LEVEL_RMSE[current_level - 1]['rmse']
        improvement = prev_rmse - current_rmse
        pct = (improvement / prev_rmse) * 100
        
        if improvement > 0:
            st.success(f"""
            **✅ Improved by {improvement:,.0f} ({pct:.1f}%) vs Level {current_level - 1}!**
            """)
        elif improvement < 0:
            st.warning(f"""
            **⚠️ RMSE increased by {-improvement:,.0f} ({-pct:.1f}%) vs Level {current_level - 1}**
            
            This can happen! Possible reasons:
            - Added noise from irrelevant features
            - Different data split
            - Overfitting
            
            Don't worry - this is part of learning! The goal is to understand WHY.
            """)
        else:
            st.info("**Same as previous level** - no significant change")
    
    # Best so far
    best_level = min(range(1, current_level + 1), 
                     key=lambda x: current_rmse if x == current_level else LEVEL_RMSE[x]['rmse'])
    best_rmse = current_rmse if best_level == current_level else LEVEL_RMSE[best_level]['rmse']
    
    st.markdown(f"""
    <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 10px 0;">
        <b>🏆 Best So Far: Level {best_level} ({LEVEL_RMSE[best_level]['name']}) - RMSE: {best_rmse:,.0f}</b>
    </div>
    """, unsafe_allow_html=True)


def display_simple_rmse_comparison(current_level: int, current_rmse: float) -> None:
    """
    Display a simpler RMSE comparison (just metrics).
    
    Args:
        current_level: Current level number
        current_rmse: Current level's test RMSE
    """
    if current_level <= 1:
        return
    
    st.markdown("### 📈 Progress Check")
    
    # Show comparison with Level 2 (baseline ML) and previous level
    cols = st.columns(3)
    
    with cols[0]:
        l2_rmse = LEVEL_RMSE[2]['rmse']
        diff = l2_rmse - current_rmse
        st.metric(
            "vs Level 2 (Baseline)", 
            f"{diff:+,.0f}",
            delta=f"{(diff/l2_rmse)*100:+.1f}%",
            delta_color="normal" if diff >= 0 else "inverse"
        )
    
    with cols[1]:
        prev_rmse = LEVEL_RMSE[current_level - 1]['rmse']
        diff = prev_rmse - current_rmse
        st.metric(
            f"vs Level {current_level - 1}", 
            f"{diff:+,.0f}",
            delta=f"{(diff/prev_rmse)*100:+.1f}%",
            delta_color="normal" if diff >= 0 else "inverse"
        )
    
    with cols[2]:
        st.metric("Current RMSE", f"{current_rmse:,.0f}")
