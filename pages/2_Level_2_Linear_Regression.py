# -*- coding: utf-8 -*-
"""
Level 2: Linear Regression (Single Feature)

First ML model using only exclusive area to predict price.
Formula: Price = weight × Area + bias
"""
import streamlit as st
import random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from src.io import load_sample_dataset
from src.model import load_trained_model, get_model_info, calculate_metrics
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser, display_code_link
from src.comparison import display_rmse_comparison


def display_header() -> None:
    """Display Level 2 introduction."""
    st.title("📐 Level 2: Linear Regression")
    
    # Table of Contents
    # Table of Contents
    st.markdown("""
    **📋 Table of Contents**
    
    1. [🧮 The Method](#the-method)
    2. [📊 Check Correlation](#data-area-vs-price)
    3. [🎓 Interactive Simulator](#step-3-training-interactive-simulator)
    4. [🤖 Trained Model Info](#step-4-trained-model)
    5. [📏 Performance Evaluation](#model-performance)
    6. [🔮 Prediction Demo](#try-it-yourself)
    """)
    
    st.success("""
    **Goal**: Predict apartment price using machine learning.
    
    We use **Linear Regression** - the simplest ML algorithm!
    """)
    
    with st.expander("💡 What is Linear Regression?"):
        st.markdown("""
        **Linear Regression** = Finding the best straight line through data points.
        
        Instead of us deciding the formula, **the computer learns** the optimal values.
        
        - **Input**: Data (area, price pairs)
        - **Output**: Best w and b in `Price = w × Area + b`
        - **Learning**: Minimizes prediction errors automatically
        """)


def display_pipeline_overview() -> None:
    """Show the end-to-end ML pipeline for Level 2."""
    st.header("🔄 Level 2 Pipeline Overview")
    
    st.markdown("""
    **What we'll do step by step:**
    """)
    
    # Pipeline flow diagram
    st.markdown("""
    <div style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0;">
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #2196F3, #1976D2); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>1. Load</b><br><span style="font-size: 11px;">Get data</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #9C27B0, #7B1FA2); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>2. Check</b><br><span style="font-size: 11px;">Correlation</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #FF9800, #F57C00); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>3. Train</b><br><span style="font-size: 11px;">Learn w, b</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #E91E63, #C2185B); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>4. Evaluate</b><br><span style="font-size: 11px;">Check RMSE</span>
        </div>
        <div style="padding: 12px 5px; color: #666;">→</div>
        <div style="padding: 12px 20px; background: linear-gradient(135deg, #4CAF50, #388E3C); 
                    border-radius: 8px; color: white; text-align: center; min-width: 100px;">
            <b>5. Predict</b><br><span style="font-size: 11px;">Use model</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Step explanations
    # Key difference from Level 1
    st.info("""
    **🆚 Key Difference from Level 1:**
    
    Level 1: We calculated median manually → No training needed
    
    Level 2: Computer **learns** w and b from data → This is Machine Learning!
    """)

    # What happens at each step - Single column layout with motivating questions
    st.markdown("### 🔍 What happens at each step?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 10px 0;">
        <b>📥 Step 1: Load Data</b><br>
        <span style="font-size: 13px;">
        Same as Level 1 - load apartment price data with Area and Price columns.
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(156,39,176,0.1); border-radius: 10px; 
                border-left: 4px solid #9C27B0; margin: 10px 0;">
        <b>🔍 Step 2: Check Correlation</b><br>
        <span style="font-size: 13px;">
        <b>Is there a pattern?</b> Before training, we check if Area and Price are related.<br>
        If correlation is near 0, Linear Regression won't help!
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(255,152,0,0.1); border-radius: 10px; 
                border-left: 4px solid #FF9800; margin: 10px 0;">
        <b>🎓 Step 3: Train Model</b> ⭐ <i>This is the ML part!</i><br>
        <span style="font-size: 13px;">
        <b>Computer learns the best w and b</b> by looking at all data points.<br>
        Goal: Find the line that minimizes prediction errors.
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(233,30,99,0.1); border-radius: 10px; 
                border-left: 4px solid #E91E63; margin: 10px 0;">
        <b>📏 Step 4: Evaluate Model</b><br>
        <span style="font-size: 13px;">
        <b>How good is our model?</b> We measure RMSE (Root Mean Squared Error).<br>
        Lower RMSE = Better predictions!
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 10px 0;">
        <b>🔮 Step 5: Predict</b><br>
        <span style="font-size: 13px;">
        <b>Use the trained model!</b> Input any area → Get predicted price.<br>
        Formula: Price = w × Area + b (where w and b are learned values)
        </span>
    </div>
    """, unsafe_allow_html=True)


def display_why_level2() -> None:
    """Explain problems with Level 1 and motivation for Level 2."""
    st.header("🤔 Wait... What's Wrong with Level 1?")
    
    st.markdown("""
    Level 1 worked! But think about these problems...
    """)
    
    # Problem boxes in single column for readability
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 10px 0;">
        <b>❌ Problem 1: We Decided the Formula</b><br>
        <span style="font-size: 13px;">
        In Level 1, <b>WE</b> decided: "Price = Median × Area"<br>
        But is this the best formula? Who knows! We just guessed.<br>
        <i>→ What if a computer could find a BETTER formula?</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 10px 0;">
        <b>❌ Problem 2: Same District = Same Price/m²</b><br>
        <span style="font-size: 13px;">
        In Gangnam: ALL apartments have the same price per m²<br>
        A 20-year-old apartment = A brand new apartment? Really?<br>
        <i>→ We're ignoring individual apartment differences!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; 
                border-left: 4px solid #F44336; margin: 10px 0;">
        <b>❌ Problem 3: No "Learning" from Data</b><br>
        <span style="font-size: 13px;">
        Level 1 just calculates statistics (median, mean).<br>
        It doesn't "learn" patterns. It doesn't improve.<br>
        <i>→ Can we make the computer LEARN the best prediction?</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # The solution
    st.markdown("""
    <div style="padding: 20px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                border-left: 4px solid #4CAF50; margin: 15px 0;">
        <b>✅ Level 2 Solution: Let the Computer Learn!</b><br><br>
        <span style="font-size: 14px;">
        Instead of <b>us</b> deciding the formula, we let the <b>computer find the optimal values</b>.<br><br>
        <b>Formula</b>: Price = <span style="color:#4CAF50">w</span> × Area + <span style="color:#4CAF50">b</span><br><br>
        <b>w</b> and <b>b</b> are NOT chosen by us!<br>
        The computer finds the BEST w and b by looking at ALL the data.<br><br>
        <i>This is the first step into Machine Learning! 🎉</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Quick comparison table
    st.markdown("### 📊 Quick Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Level 1 (Heuristic)**
        - ❌ We decide the formula
        - ❌ Same price/m² in district
        - ❌ No learning
        - ✅ Uses district info
        """)
    
    with col2:
        st.markdown("""
        **Level 2 (Linear Regression)**
        - ✅ Computer finds best formula
        - ✅ Considers each apartment
        - ✅ Learns from data
        - ❌ Ignores district (for now!)
        """)


def display_method() -> None:
    """Explain Linear Regression."""
    st.header("🧮 The Method")
    
    st.markdown("### 1. What's in a Name?")
    
    st.markdown("""
    Why is it called **"Linear Regression"**? Let's break it down:
    """)
    
    col1, col2 = st.columns(2)
    with col1:
        st.info("""
        **📏 Linear**
        
        The relationship is a **Straight Line**.
        *   If Area goes up, Price goes up (proportionally).
        *   Formula: $y = ax + b$
        """)
    with col2:
        st.info("""
        **🎯 Regression**
        
        We are predicting a **Number** (Continuous Quantity).
        *   "How much?" (Price, Temperature, Height)
        *   *vs Classification ("Cat or Dog?")*
        """)

    # Visualization
    st.graphviz_chart("""
    digraph G {
        rankdir=TB;
        node [fontname="Sans", shape=box, style="filled,rounded", fillcolor="white", margin=0.2];
        edge [fontname="Sans", color="#666666"];
        bgcolor="transparent";
        splines=ortho;
        
        # Title Node
        LR [label="Linear Regression", shape=doubleoctagon, fillcolor="#FFF3E0", fontsize=16, fontcolor="#E65100"];
        
        # Split
        {rank=same; Linear; Regression}
        
        Linear [label="Linear", fillcolor="#E3F2FD", color="#2196F3", penwidth=2];
        Regression [label="Regression", fillcolor="#E8F5E9", color="#4CAF50", penwidth=2];
        
        LR -> Linear [penwidth=2, color="#2196F3"];
        LR -> Regression [penwidth=2, color="#4CAF50"];
        
        # Details Linear
        L_desc [label="Straight Line\nRelationship", style=dashed];
        L_math [label="y = wx + b", shape=ellipse];
        
        Linear -> L_desc;
        L_desc -> L_math;
        
        # Details Regression
        R_desc [label="Predicting a\nNumber", style=dashed];
        R_ex [label="Output: 12.5, 99.9", shape=ellipse];
        
        Regression -> R_desc;
        R_desc -> R_ex;
    }
    """)
    
    st.markdown("### 2. The Formula")
    
    st.latex(r"\text{Price} = w \times \text{Area} + b")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **w (weight)**: Slope
        *   How much price increases per 1 m²
        """)
    
    with col2:
        st.markdown("""
        **b (bias)**: Intercept
        *   Base price when area = 0
        """)
    
    with st.expander("🤔 Why Linear Regression?"):
        st.markdown("""
        - **Simple**: Easy to understand and explain.
        - **Fast**: Computers can calculate it instantly.
        - **Baseline**: Always start simple! If a line works, you don't need complex AI.
        """)


def display_data_insight(df: pd.DataFrame) -> None:
    """Show feature-target relationship."""
    st.header("📊 Data: Area vs Price")
    
    st.markdown("""
    Before training, let's check: **Is there a relationship between Area and Price?**
    
    If Area and Price move together, Linear Regression will work!
    """)
    
    # Sample for visualization
    sample = df.sample(n=min(3000, len(df)), random_state=RANDOM_STATE)
    
    # Correlation calculation
    corr = sample['area_m2'].corr(sample['price_10k_krw'])
    
    # Scatter plot with regression line (full width)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(sample['area_m2'], sample['price_10k_krw'], 
               alpha=0.3, s=10, c='steelblue', label='Data points')
    
    # Add regression line to visualize the relationship
    x = sample['area_m2'].values
    y = sample['price_10k_krw'].values
    slope, intercept = np.polyfit(x, y, 1)
    line_x = np.array([x.min(), x.max()])
    line_y = slope * line_x + intercept
    ax.plot(line_x, line_y, 'r-', linewidth=2, label=f'Best fit: y = {slope:.1f}x + {intercept:.0f}')
    
    ax.set_xlabel('Exclusive Area (m²)')
    ax.set_ylabel('Price (10k KRW)')
    ax.set_title('Area vs Price with Regression Line')
    ax.legend()
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Correlation and slope explanation below the graph
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
        <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                    border-left: 4px solid #2196F3; margin: 10px 0;">
            <b>📈 Correlation (r) = {corr:.3f}</b><br><br>
            <span style="font-size: 13px;">
            <b>What is it?</b> How tightly points follow the line.<br>
            • r = 1.0: Perfect line (no scatter)<br>
            • r = 0.0: Random scatter<br>
            • r = -1.0: Perfect downward line<br><br>
            <b>Ours: {corr:.3f}</b> = Moderate-strong ✓
            </span>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; 
                    border-left: 4px solid #4CAF50; margin: 10px 0;">
            <b>📐 Slope (w) = {slope:.1f}</b><br><br>
            <span style="font-size: 13px;">
            <b>What is it?</b> The steepness of the red line.<br>
            • How much price increases per 1 m²<br>
            • This IS what Linear Regression learns!<br><br>
            <b>Ours: {slope:.1f}</b> = +{slope:.0f} per m² ✓
            </span>
        </div>
        """, unsafe_allow_html=True)
    
    st.info("""
    **💡 Correlation vs Slope**: 
    - **Correlation** tells you "is there a relationship?" (strength)
    - **Slope** tells you "by how much?" (the actual number)
    
    The red line above is what Linear Regression finds!
    """)
    
    with st.expander("🔢 How is Correlation Calculated?"):
        st.markdown("""
        **Pearson Correlation Formula:**
        """)
        st.latex(r"r = \frac{\sum(x_i - \bar{x})(y_i - \bar{y})}{\sqrt{\sum(x_i - \bar{x})^2 \cdot \sum(y_i - \bar{y})^2}}")
        st.markdown(f"""
        **In Python (what we used):**
        ```python
        correlation = df['area_m2'].corr(df['price_10k_krw'])
        # Result: {corr:.3f}
        ```
        
        Simply put: We compare how much each point deviates from the average,
        for both Area and Price, then see if they deviate in the same direction.
        """)


def get_loss_surface(X, y, param_range):
    """Calculate loss surface for visualization."""
    w_range = np.linspace(param_range['w_min'], param_range['w_max'], 20)
    b_range = np.linspace(param_range['b_min'], param_range['b_max'], 20)
    W, B = np.meshgrid(w_range, b_range)
    Z = np.zeros_like(W)
    
    for i in range(W.shape[0]):
        for j in range(W.shape[1]):
            pred = W[i, j] * X + B[i, j]
            Z[i, j] = np.mean((pred - y) ** 2)
            
    return W, B, Z

def display_training_process(df: pd.DataFrame) -> None:
    """Interactive Gradient Descent Simulator."""
    st.header("🎓 Step 3: Training (Interactive Simulator)")
    
    st.markdown("""
    **Experiencing Gradient Descent**
    
    Let's train the model ourselves! We will use **Gradient Descent** to find the best `w` and `b`.
    """)

    with st.expander("⚖️ Wait! Why do we need 'Scaling'?", expanded=True):
        st.markdown("""
        In the code (and in real life), you must **Scale** your data first. 
        
        *   **Area**: ~ 50 to 200 ($m^2$)
        *   **Price**: ~ 50,000 to 200,000 (10k KRW)
        
        These numbers are too different! 😱
        If we don't scale them, the computer gets confused. It's like trying to compare **Ants** and **Elephants**.
        
        **The Fix:** We use `StandardScaler`.
        It squashes both Area and Price to be around **-1 to +1**. This makes learning much smoother.
        """)
    
    with st.expander("🏔️ What is Gradient Descent? (The Mountain Hiker Analogy)", expanded=True):
        st.markdown("""
        Imagine you are on a **mountain at night** (blindfolded). You want to reach the **village at the bottom** (Lowest Error).
        
        1.  **Feel the slope**: You tap the ground with your foot to see which way connects "down".
        2.  **Take a step**: You take a step in the downhill direction.
        3.  **Repeat**: You keep doing this until the ground is flat (you reached the bottom!).
        
        **In Math:**
        """)
        st.latex(r"w_{new} = w_{old} - \text{Step Size} \times \text{Slope}")
        st.markdown("""
        *   **Step Size (Learning Rate)**: How big your step is.
            *   Too big? You might jump over the village! 🐇
            *   Too small? It takes forever. 🐢
        *   **Slope (Gradient)**: Which way is down?
        """)
    
    st.markdown("""
    *   **Goal**: Reach the center of the "Error Mountain" (Dark blue area).
    *   **Method**: Take steps downhill.
    """)
    
    # 1. Setup Simulation Data (Normalize for easier visualization/training)
    # We use a small sample for individual point visualization
    sample = df.sample(n=50, random_state=42)
    X = sample['area_m2'].values
    y = sample['price_10k_krw'].values
    
    # Pre-calculate optimal for reference
    optimal_w, optimal_b = np.polyfit(X, y, 1)
    
    # 2. Setup Session State
    if 'gd_w' not in st.session_state:
        st.session_state['gd_w'] = random.uniform(0, 2000)
    if 'gd_b' not in st.session_state:
        st.session_state['gd_b'] = random.uniform(-50000, 50000)
    if 'gd_epoch' not in st.session_state:
        st.session_state['gd_epoch'] = 0
    if 'gd_history' not in st.session_state:
        st.session_state['gd_history'] = []

    # 3. Controls
    st.markdown("##### 🕹️ Simulator Controls")
    
    col_lr, col_btn = st.columns([1, 2])
    
    with col_lr:
        step_speed = st.radio(
            "Step Size (Learning Rate)",
            ["🐢 Little Steps (Careful)", "🐇 Big Steps (Fast)"],
            index=0
        )
        
        # Set Learning Rate based on selection
        if step_speed == "🐢 Little Steps (Careful)":
            st.session_state['gd_lr'] = 0.00001
        else:
            st.session_state['gd_lr'] = 0.00005
            
    with col_btn:
        st.write("") # Spacing
        st.write("") 
        
        c1, c2, c3 = st.columns(3)
        
        with c1:
            if st.button("Step (1x)"):
                # Update step
                y_pred = st.session_state['gd_w'] * X + st.session_state['gd_b']
                error = y_pred - y
                w_grad = (2/len(X)) * np.sum(error * X)
                b_grad = (2/len(X)) * np.sum(error)
                
                st.session_state['gd_w'] -= st.session_state['gd_lr'] * w_grad
                st.session_state['gd_b'] -= st.session_state['gd_lr'] * b_grad * 100 
                st.session_state['gd_epoch'] += 1
                st.session_state['gd_history'].append((st.session_state['gd_w'], st.session_state['gd_b']))
                
        with c2:
            if st.button("Fast (10x)"):
                for _ in range(10):
                    y_pred = st.session_state['gd_w'] * X + st.session_state['gd_b']
                    error = y_pred - y
                    w_grad = (2/len(X)) * np.sum(error * X)
                    b_grad = (2/len(X)) * np.sum(error)
                    
                    st.session_state['gd_w'] -= st.session_state['gd_lr'] * w_grad
                    st.session_state['gd_b'] -= st.session_state['gd_lr'] * b_grad * 100
                
                st.session_state['gd_epoch'] += 10
                st.session_state['gd_history'].append((st.session_state['gd_w'], st.session_state['gd_b']))

        with c3:
            if st.button("Reset"):
                st.session_state['gd_w'] = random.uniform(0, 2000)
                st.session_state['gd_b'] = random.uniform(-50000, 50000)
                st.session_state['gd_epoch'] = 0
                st.session_state['gd_history'] = []
    
    # Show stats
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Epoch: {st.session_state['gd_epoch']}**")
    with col2:
        cur_mse = np.mean((st.session_state['gd_w'] * X + st.session_state['gd_b'] - y) ** 2)
        st.metric("Current RMSE", f"{np.sqrt(cur_mse):,.0f}")

    # 4. Visualization
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Plot 1: Loss Surface
    # Define range relative to optimal to ensure it's centered enough
    w_min, w_max = -500, 2500
    b_min, b_max = -100000, 100000
    
    W, B, Z = get_loss_surface(X, y, {'w_min': w_min, 'w_max': w_max, 'b_min': b_min, 'b_max': b_max})
    
    cp = ax1.contourf(W, B, np.sqrt(Z), levels=20, cmap='viridis_r')
    fig.colorbar(cp, ax=ax1, label='RMSE')
    
    # Plot path
    if st.session_state['gd_history']:
        path = np.array(st.session_state['gd_history'])
        ax1.plot(path[:, 0], path[:, 1], 'w-', alpha=0.5)
        
    # Current point
    ax1.plot(st.session_state['gd_w'], st.session_state['gd_b'], 'ro', markersize=10, markeredgecolor='white', label='Current')
    ax1.plot(optimal_w, optimal_b, 'b*', markersize=15, label='Optimal')
    
    ax1.set_title(f"Error Mountain (Loss Surface)\nCurrent: w={st.session_state['gd_w']:.0f}, b={st.session_state['gd_b']:.0f}")
    ax1.set_xlabel('Weight (w)')
    ax1.set_ylabel('Bias (b)')
    ax1.legend()
    
    # Plot 2: Regression Line
    ax2.scatter(X, y, alpha=0.3, c='steelblue', s=15)
    
    # Current line
    line_x = np.array([X.min(), X.max()])
    line_y = st.session_state['gd_w'] * line_x + st.session_state['gd_b']
    ax2.plot(line_x, line_y, 'r-', linewidth=3, label='Your Model')
    
    # Optimal line (ghost)
    opt_y = optimal_w * line_x + optimal_b
    ax2.plot(line_x, opt_y, 'k--', alpha=0.3, label='Best Possible')
    
    ax2.set_title("Resulting Model Line")
    ax2.set_xlabel("Area")
    ax2.set_ylabel("Price")
    ax2.legend()
    
    st.pyplot(fig)
    
    if st.session_state['gd_epoch'] > 0:
        if abs(st.session_state['gd_w'] - optimal_w) < 100 and abs(st.session_state['gd_b'] - optimal_b) < 10000:
             st.success("🎉 Converged! You found the best parameters!")
        else:
             st.info("Keep clicking 'Fast (10x)' to see the red dot slide down to the blue star!")

    # --- NEW: Educational Deep Dive ---
    st.markdown("---")
    st.subheader("📘 The Math & Logic behind Step 3")
    
    st.markdown("""
    You just performed **Stochastic Gradient Descent (SGD)** manually! 
    But what is actually happening inside the computer?
    """)
    
    tab1, tab2, tab3 = st.tabs(["1. The Formula", "2. The Code", "3. The Flow"])
    
    with tab1:
        st.markdown("### 1. The Update Rule")
        st.markdown("How do we know **how much** to change `w` and `b`?")
        
        st.latex(r"w_{new} = w_{old} - \text{learning\_rate} \times \text{gradient}")
        st.latex(r"b_{new} = b_{old} - \text{learning\_rate} \times \text{gradient}")
        
        st.markdown("""
        **In Math Notation:**
        """)
        st.latex(r"w \leftarrow w - \eta \cdot \frac{\partial L}{\partial w}")
        st.latex(r"b \leftarrow b - \eta \cdot \frac{\partial L}{\partial b}")
        
        st.markdown(r"""
        *   $w, b$: Weights (Slope, Intercept)
        *   $\eta$ (Eta): **Learning Rate** (Step Size)
        *   $\frac{\partial L}{\partial w}$: **Gradient** (Slope of the Error)
        """)
        
    with tab2:
        st.markdown("### 2. Python Implementation")
        st.markdown("This is essentially the code running every time you click **'Step'**:")
        
        st.code("""
# 1. Calculate Prediction
y_pred = w * X + b

# 2. Calculate Error
error = y_pred - y

# 3. Calculate Gradient (Slope of error)
w_grad = (2/N) * sum(error * X)
b_grad = (2/N) * sum(error)

# 4. Update Parameters (Take a step downhill)
w = w - learning_rate * w_grad
b = b - learning_rate * b_grad
""", language='python')

    with tab3:
        st.markdown("### 3. Execution Flow")
        st.markdown("The computer does this loop thousands of times:")
        
        st.graphviz_chart("""
        digraph SGD {
            rankdir=TB;
            newrank=true;
            splines=ortho;
            
            node [fontname="Arial", fontsize=12, shape=box, style="filled,rounded", 
                  fillcolor="white", color="#DDDDDD", penwidth=1.5, margin=0.2];
            edge [fontname="Arial", fontsize=10, color="#666666", penwidth=1.2];
            
            # Start Node
            Start [label="Start\n(Random w, b)", shape=pill, fillcolor="#E3F2FD", color="#2196F3", penwidth=2];
            
            # Main Loop Nodes
            Predict [label="1. Predict\ny = wx + b"];
            Error [label="2. Measure Error\n(Loss)"];
            Gradient [label="3. Compute Gradient\n(Slope)"];
            Update [label="4. Update w, b\n(- learning_rate × slope)", fillcolor="#FFF3E0", color="#FF9800", penwidth=2];
            
            # Decision Node
            Check [label="Error Low?", shape=diamond, height=0.8, fillcolor="#FFF8E1", color="#FFC107"];
            
            # Stop Node
            Stop [label="Done! 🎉\n(Converged)", shape=doublecircle, fillcolor="#E8F5E9", color="#4CAF50", penwidth=2];
            
            # Connections
            Start -> Predict [weight=2];
            Predict -> Error [weight=2];
            Error -> Gradient [weight=2];
            Gradient -> Update [weight=2];
            Update -> Check [weight=2];
            
            # The Loop Back (Red)
            Check -> Predict [label="No (Repeat)", color="#F44336", fontcolor="#F44336", constraint=false];
            
            # The Exit (Green)
            Check -> Stop [label="Yes", color="#4CAF50", fontcolor="#4CAF50", weight=2];
            
            # Invisible edge to align Start and Stop properly
            Start -> Stop [style=invis];
        }
        """, use_container_width=True)


def display_model_info(df: pd.DataFrame) -> None:
    """Show trained model parameters and training details."""
    st.header("🤖 Step 4: Trained Model")
    
    model = load_trained_model()
    
    if model is None:
        st.warning("⚠️ No trained model found. Run `python train.py` first.")
        st.code("cd seoul-apt-price-prediction\npython train.py", language='bash')
        return
    
    info = get_model_info(model)
    
    # Training summary box
    st.markdown("""
    <div style="padding: 20px; background: linear-gradient(135deg, rgba(76,175,80,0.1), rgba(33,150,243,0.1)); 
                border-radius: 15px; border: 2px solid #4CAF50; margin: 15px 0;">
        <h4 style="margin: 0 0 15px 0;">✅ Training Complete!</h4>
        <span style="font-size: 14px; color: gray;">
        Model has been trained and saved to <code>models/linear_area_model.pkl</code>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Training data info
    st.markdown("### 📊 Training Data Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Samples", "1,118,822")
    with col2:
        st.metric("Training Set", "895,057")
        st.caption("80% of data")
    with col3:
        st.metric("Validation Set", "223,765")
        st.caption("20% of data")
    with col4:
        st.metric("Feature Used", "Area (m²)")
    
    st.markdown("---")
    
    # Model parameters
    st.markdown("### 📐 Learned Parameters")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Weight (w)", f"{info['coefficient']:,.2f}")
        st.caption("Price increase per 1 m²")
    
    with col2:
        st.metric("Bias (b)", f"{info['intercept']:,.2f}")
        st.caption("Base price (intercept)")
    
    # The formula
    st.markdown(f"""
    <div style="padding: 25px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border: 2px solid #2196F3; margin: 15px 0; text-align: center;">
        <h3 style="margin: 0;">📝 Final Formula</h3>
        <p style="font-size: 22px; margin: 15px 0;">
            <b>Price</b> = <span style="color:#4CAF50">{info['coefficient']:,.2f}</span> × Area + <span style="color:#9C27B0">{info['intercept']:,.2f}</span>
        </p>
        <p style="font-size: 14px; color: gray; margin: 0;">
            +1 m² → +{info['coefficient']:,.0f} (10k KRW) → +{info['coefficient']/100:.1f} Million KRW
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Visualize the trained model
    st.markdown("### 📈 Trained Model Visualization")
    
    sample = df.sample(n=min(2000, len(df)), random_state=RANDOM_STATE)
    x = sample['area_m2'].values
    y = sample['price_10k_krw'].values
    y_pred = model.predict(x.reshape(-1, 1))
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(x, y, alpha=0.3, s=15, c='steelblue', label='Actual data')
    
    # Sort for line plotting
    sort_idx = np.argsort(x)
    ax.plot(x[sort_idx], y_pred[sort_idx], 'r-', linewidth=3, 
            label=f'Trained model: y = {info["coefficient"]:.1f}x + {info["intercept"]:.0f}')
    
    ax.set_xlabel('Area (m²)', fontsize=12)
    ax.set_ylabel('Price (10k KRW)', fontsize=12)
    ax.set_title('Trained Linear Regression Model', fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Example calculations
    st.markdown("### 🔢 Example Calculations")
    
    examples = [
        (30, "Small studio"),
        (60, "1-2 bedroom"),
        (84, "3 bedroom (typical)"),
        (120, "Large family"),
        (150, "Luxury/Penthouse")
    ]
    
    example_data = []
    for area, desc in examples:
        pred = model.predict([[area]])[0]
        example_data.append({
            "Type": desc,
            "Area (m²)": area,
            "Predicted Price": f"{pred:,.0f}",
            "Billion KRW": f"{pred/10000:.1f}"
        })
    
    st.dataframe(pd.DataFrame(example_data), use_container_width=True, hide_index=True)
    
    # How to load and use the model
    with st.expander("💻 How to Load and Use This Model"):
        st.markdown("**Load the saved model in your Python code:**")
        st.code("""
import joblib

# Load the trained model
model = joblib.load('models/linear_area_model.pkl')

# Make a prediction
area = 84  # m²
predicted_price = model.predict([[area]])[0]
print(f"Predicted price: {predicted_price:,.0f} (10k KRW)")

# Get model parameters
w = model.coef_[0]       # Weight
b = model.intercept_     # Bias
print(f"Formula: Price = {w:.2f} × Area + {b:.2f}")
""", language='python')
    
    # Training reproduction
    with st.expander("🔄 How to Reproduce Training"):
        st.markdown("**Run the training script to retrain the model:**")
        st.code("""
cd seoul-apt-price-prediction
python train.py
""", language='bash')
        st.markdown("""
        **What happens:**
        1. Loads 1.1M apartment transaction records
        2. Splits into 80% train / 20% validation
        3. Trains LinearRegression model
        4. Saves to `models/linear_area_model.pkl`
        5. Creates scatter plot in `output/figures/`
        """)


def display_evaluation(df: pd.DataFrame) -> None:
    """Show model performance metrics."""
    st.header("📏 Model Performance")
    
    model = load_trained_model()
    
    if model is None:
        return
    
    # Evaluate on sample
    sample = df.sample(n=min(10000, len(df)), random_state=RANDOM_STATE)
    X = sample['area_m2'].values.reshape(-1, 1)
    y_true = sample['price_10k_krw'].values
    y_pred = model.predict(X)
    
    metrics = calculate_metrics(y_true, y_pred)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("RMSE", f"{metrics['rmse']:,.0f}")
        st.caption("Average error (10k KRW)")
    
    with col2:
        st.metric("MAE", f"{metrics['mae']:,.0f}")
        st.caption("Mean Absolute Error")
    
    with col3:
        rel_err = metrics['rmse'] / y_true.mean() * 100
        st.metric("Relative Error", f"{rel_err:.1f}%")
    
    # Actual vs Predicted explanation and plot
    st.markdown("### 📈 How Good Are Our Predictions?")
    
    st.markdown("""
    <div style="padding: 15px; background: rgba(33,150,243,0.1); border-radius: 10px; 
                border-left: 4px solid #2196F3; margin: 10px 0;">
        <b>📊 Actual vs Predicted Chart Explained</b><br><br>
        <span style="font-size: 13px;">
        • <b>X-axis</b>: Real price (what the apartment actually sold for)<br>
        • <b>Y-axis</b>: Our prediction (what our model guessed)<br>
        • <b>Red dashed line</b>: Perfect prediction (Actual = Predicted)<br><br>
        <b>How to read it:</b><br>
        • Points ON the red line = Perfect predictions! 🎯<br>
        • Points ABOVE the line = We predicted TOO HIGH<br>
        • Points BELOW the line = We predicted TOO LOW<br><br>
        <i>The closer points are to the red line, the better our model!</i>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(y_true[:1000], y_pred[:1000], alpha=0.3, s=15, c='steelblue', label='Each apartment')
    max_val = max(y_true.max(), y_pred.max())
    ax.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Perfect prediction line')
    
    # Add annotation arrows
    ax.annotate('Points here = \nWe predicted TOO HIGH', 
                xy=(50000, 120000), fontsize=9, color='gray',
                ha='center')
    ax.annotate('Points here = \nWe predicted TOO LOW', 
                xy=(150000, 80000), fontsize=9, color='gray',
                ha='center')
    
    ax.set_xlabel('Actual Price (10k KRW)')
    ax.set_ylabel('Predicted Price (10k KRW)')
    ax.set_title('Actual vs Predicted: How Close Are We?')
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Summary insight
    above_line = np.sum(y_pred > y_true)
    below_line = np.sum(y_pred < y_true)
    total = len(y_true)
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Predicted Too High", f"{above_line/total*100:.1f}%", 
                  delta=f"{above_line} apartments")
    with col2:
        st.metric("Predicted Too Low", f"{below_line/total*100:.1f}%",
                  delta=f"{below_line} apartments")
    
    st.info("""
    **💡 What does this tell us?**
    
    The points spread out as prices get higher. This means our simple model 
    (using only Area) struggles with expensive apartments.
    
    Why? Because expensive apartments often have other factors: good location, 
    high floor, new building... things we're NOT using in Level 2!
    """)
    
    # Compare with other levels
    st.markdown("---")
    display_rmse_comparison(2, metrics['rmse'])


def display_demo(df: pd.DataFrame) -> None:
    """Interactive prediction demo."""
    st.header("🔮 Try It Yourself")
    
    model = load_trained_model()
    
    if model is None:
        st.warning("Train the model first!")
        return
    
    info = get_model_info(model)
    
    # Input
    selected_area = st.slider("Exclusive Area (m²)", 
                               min_value=10, max_value=200, value=84)
    
    # Predict
    predicted_price = model.predict([[selected_area]])[0]
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Input Area", f"{selected_area} m²")
    
    with col2:
        st.metric("Predicted Price", f"{predicted_price:,.0f} (10k KRW)")
    
    # Show calculation
    st.info(f"""
    **Calculation**:
    
    Price = {info['coefficient']:,.2f} × {selected_area} + {info['intercept']:,.2f}
    
    = {info['coefficient'] * selected_area:,.2f} + {info['intercept']:,.2f}
    
    = **{predicted_price:,.0f}** (10k KRW) ≈ **{predicted_price/10000:.1f} Billion KRW**
    """)


def display_comparison() -> None:
    """Compare with Level 1."""
    st.header("⚖️ Level 1 vs Level 2: The Shocking Truth")
    
    st.markdown("""
    You might notice something strange...
    **Level 1 (Simple Math) often has BETTER accuracy than Level 2 (Machine Learning)!**
    
    How is this possible? Isn't AI supposed to be smarter?
    """)
    
    st.markdown("### 🏆 The Battle: Data vs Algorithm")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="padding: 15px; background: rgba(76,175,80,0.1); border-radius: 10px; border-left: 4px solid #4CAF50;">
            <h4>🧠 Level 1: Heuristic</h4>
            <b>"I use a LOOKUP TABLE!"</b><br><br>
            Level 1 knows <b>Location (District)</b>.<br>
            It knows Gangnam is expensive and Dobong is cheap.<br>
            <br>
            <b>Algorithm</b>: Very Simple (Multiply)<br>
            <b>Data</b>: <b style="color:green">Rich (Uses Location)</b>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown("""
        <div style="padding: 15px; background: rgba(244,67,54,0.1); border-radius: 10px; border-left: 4px solid #F44336;">
            <h4>🤖 Level 2: Linear Regression</h4>
            <b>"I draw a STRAIGHT LINE!"</b><br><br>
            Level 2 <b>IGNORES District</b> (for now).<br>
            It treats 100m² in Gangnam exactly the same as 100m² in Dobong.<br>
            <br>
            <b>Algorithm</b>: Smart (Optimization)<br>
            <b>Data</b>: <b style="color:red">Poor (Ignores Location)</b>
        </div>
        """, unsafe_allow_html=True)
        
    st.markdown("### 🎨 Visualizing Why Level 2 Fails")
    
    st.graphviz_chart("""
    digraph L1_vs_L2 {
        rankdir=TD;
        node [fontname="Arial", shape=box, style=filled, fillcolor="white"];
        
        subgraph cluster_L1 {
            label = "Level 1: Heuristic";
            style = dashed;
            color = "#4CAF50";
            
            A1 [label="Input: District + Area"];
            B1 [label="Split by District"];
            C1 [label="Get District Price/m²", fillcolor="#E8F5E9"];
            D1 [label="Price = Area × District_Value", fillcolor="#C8E6C9"];
            Winner [label="Better Prediction? 🏆", style=filled, fillcolor="#4CAF50", fontcolor="white"];
            
            A1 -> B1;
            B1 -> C1;
            C1 -> D1;
            D1 -> Winner [label="Uses Location Info"];
        }
        
        subgraph cluster_L2 {
            label = "Level 2: Simple Linear Regression";
            style = dashed;
            color = "#F44336";
            
            A2 [label="Input: District + Area"];
            B2 [label="Ignore District!"];
            C2 [label="Only use Area", fillcolor="#FFEBEE"];
            D2 [label="Price = w × Area + b", fillcolor="#FFCDD2"];
            Loser [label="Worse Prediction? 📉", style=filled, fillcolor="#F44336", fontcolor="white"];
            
            A2 -> B2;
            B2 -> C2;
            C2 -> D2;
            D2 -> Loser [label="Ignores Location"];
        }
    }
    """)
    
    st.info("""
    **💡 The Lesson: Better Data > Better Algorithm**
    
    Even a smart AI cannot predict well if you hide important information (Location) from it!
    
    **This is why we need Level 3!**
    Level 3 will combine **Machine Learning** (Algorithm) with **District Info** (Data) to finally beat Level 1.
    """)


def main() -> None:
    """Page entry point."""
    try:
        df = load_sample_dataset()
        
        display_header()
        st.markdown("---")
        display_pipeline_overview()
        st.markdown("---")
        display_why_level2()
        st.markdown("---")
        display_method()
        st.markdown("---")
        display_data_insight(df)
        st.markdown("---")
        display_training_process(df)
        st.markdown("---")
        display_model_info(df)
        st.markdown("---")
        display_evaluation(df)
        st.markdown("---")
        display_demo(df)
        st.markdown("---")
        display_comparison()
        
        # Code Link
        
        display_code_link("Level_2_Linear_Regression.ipynb")
        
        
        
        # Next level teaser
        display_next_level_teaser(2)
        
    except Exception as e:
        st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
