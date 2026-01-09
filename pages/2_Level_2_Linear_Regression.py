# -*- coding: utf-8 -*-
"""
Level 2: Linear Regression (Single Feature)

First ML model using only exclusive area to predict price.
Formula: Price = weight × Area + bias
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from src.io import load_sample_dataset
from src.model import load_trained_model, get_model_info, calculate_metrics
from src.config import RANDOM_STATE
from src.navigation import display_next_level_teaser
from src.comparison import display_rmse_comparison


def display_header() -> None:
    """Display Level 2 introduction."""
    st.title("📐 Level 2: Linear Regression")
    
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
    
    # Key difference from Level 1
    st.info("""
    **🆚 Key Difference from Level 1:**
    
    Level 1: We calculated median manually → No training needed
    
    Level 2: Computer **learns** w and b from data → This is Machine Learning!
    """)


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
    
    st.markdown("""
    ### Linear Regression
    
    Find the **best straight line** that fits the data.
    """)
    
    st.latex(r"\text{Price} = w \times \text{Area} + b")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **w (weight)**: How much price increases per m²
        
        **b (bias)**: Base price when area = 0
        """)
    
    with col2:
        st.markdown("""
        **Training**: Find w and b that minimize prediction errors
        
        **RMSE**: Measures average prediction error
        """)
    
    with st.expander("🤔 Why Linear Regression?"):
        st.markdown("""
        - Simple and interpretable
        - Fast to train
        - Good baseline for comparison
        - Easy to explain: "Each m² adds X won to price"
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
    ax.set_ylabel('Price (10K KRW)')
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


def display_training_process(df: pd.DataFrame) -> None:
    """Show how training works with code examples."""
    st.header("🎓 Step 3: Training (The ML Part!)")
    
    st.markdown("""
    **This is where the magic happens!** The computer looks at all data 
    and finds the best w and b values.
    """)
    
    # Show the baseline code
    st.markdown("### 📝 The Training Code")
    
    st.code("""
# Step 1: Prepare data
X = df[['area_m2']]      # Input: Area (2D array for sklearn)
y = df['price_10k_krw']  # Output: Price

# Step 2: Create the model
from sklearn.linear_model import LinearRegression
model = LinearRegression()

# Step 3: TRAIN! (This is where learning happens)
model.fit(X, y)

# Step 4: Get learned values
w = model.coef_[0]       # Learned weight
b = model.intercept_     # Learned bias
""", language='python')
    
    # What happens inside fit()?
    with st.expander("🔍 What happens inside `model.fit(X, y)`?"):
        st.markdown("""
        **Goal**: Find w and b that minimize prediction errors.
        
        The computer tries MANY different w and b values and picks the best!
        """)
        
        st.latex(r"\text{Error} = \sum_{i=1}^{n} (\text{Actual}_i - \text{Predicted}_i)^2")
        
        st.markdown("""
        **Process (simplified)**:
        1. Start with random w and b
        2. Calculate total error for all data points
        3. Adjust w and b to reduce error
        4. Repeat until error is minimized
        
        **Result**: The line that best fits ALL data points!
        """)
    
    # Interactive demo: show different w, b values
    st.markdown("### 🎮 Try Different w and b Values")
    st.markdown("See how the line changes when you adjust w and b manually:")
    
    sample = df.sample(n=min(500, len(df)), random_state=42)
    
    col1, col2 = st.columns(2)
    with col1:
        user_w = st.slider("Weight (w)", min_value=0, max_value=2000, value=900, step=50)
    with col2:
        user_b = st.slider("Bias (b)", min_value=-50000, max_value=50000, value=0, step=5000)
    
    # Calculate errors
    x_vals = sample['area_m2'].values
    y_actual = sample['price_10k_krw'].values
    y_user = user_w * x_vals + user_b
    
    # Optimal values (from training)
    optimal_w, optimal_b = np.polyfit(x_vals, y_actual, 1)
    y_optimal = optimal_w * x_vals + optimal_b
    
    user_rmse = np.sqrt(np.mean((y_actual - y_user) ** 2))
    optimal_rmse = np.sqrt(np.mean((y_actual - y_optimal) ** 2))
    
    # Plot comparison
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(x_vals, y_actual, alpha=0.3, s=10, c='steelblue', label='Data')
    ax.plot([x_vals.min(), x_vals.max()], 
            [user_w * x_vals.min() + user_b, user_w * x_vals.max() + user_b],
            'r-', linewidth=2, label=f'Your line (RMSE: {user_rmse:,.0f})')
    ax.plot([x_vals.min(), x_vals.max()], 
            [optimal_w * x_vals.min() + optimal_b, optimal_w * x_vals.max() + optimal_b],
            'g--', linewidth=2, label=f'Optimal line (RMSE: {optimal_rmse:,.0f})')
    ax.set_xlabel('Area (m²)')
    ax.set_ylabel('Price (10K KRW)')
    ax.set_title('Your Line vs Optimal Line')
    ax.legend()
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)
    plt.close()
    
    # Show comparison
    col1, col2 = st.columns(2)
    with col1:
        delta = user_rmse - optimal_rmse
        st.metric("Your RMSE", f"{user_rmse:,.0f}", delta=f"+{delta:,.0f}" if delta > 0 else f"{delta:,.0f}")
    with col2:
        st.metric("Optimal RMSE", f"{optimal_rmse:,.0f}", delta="Best possible!")
    
    if user_rmse <= optimal_rmse * 1.05:
        st.success("🎉 Great! Your line is very close to optimal!")
    else:
        st.info(f"💡 Try w={optimal_w:.0f} and b={optimal_b:.0f} to match the optimal line!")


def display_model_info() -> None:
    """Show trained model parameters."""
    st.header("🤖 Trained Model")
    
    model = load_trained_model()
    
    if model is None:
        st.warning("⚠️ No trained model found. Run `python train.py` first.")
        return
    
    info = get_model_info(model)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Weight (w)", f"{info['coefficient']:,.2f}")
        st.caption("Price increase per 1 m²")
    
    with col2:
        st.metric("Bias (b)", f"{info['intercept']:,.2f}")
        st.caption("Base price")
    
    st.info(f"""
    **Model Equation**:
    
    Price = **{info['coefficient']:,.2f}** × Area + **{info['intercept']:,.2f}**
    
    **Interpretation**: Each additional m² increases price by ~{info['coefficient']:,.0f} (10K KRW)
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
        st.caption("Average error (10K KRW)")
    
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
    
    ax.set_xlabel('Actual Price (10K KRW)')
    ax.set_ylabel('Predicted Price (10K KRW)')
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
        st.metric("Predicted Price", f"{predicted_price:,.0f} (10K KRW)")
    
    # Show calculation
    st.info(f"""
    **Calculation**:
    
    Price = {info['coefficient']:,.2f} × {selected_area} + {info['intercept']:,.2f}
    
    = {info['coefficient'] * selected_area:,.2f} + {info['intercept']:,.2f}
    
    = **{predicted_price:,.0f}** (10K KRW) ≈ **{predicted_price/10000:.1f} 억원**
    """)


def display_comparison() -> None:
    """Compare with Level 1."""
    st.header("⚖️ Level 1 vs Level 2")
    
    st.markdown("""
    | Aspect | Level 1 (Heuristic) | Level 2 (Linear Regression) |
    |--------|---------------------|----------------------------|
    | Method | District median × Area | w × Area + b |
    | Uses District? | ✅ Yes | ❌ No |
    | Uses Area? | ✅ Yes | ✅ Yes |
    | ML? | ❌ No | ✅ Yes |
    | Interpretable? | ✅ Very | ✅ Yes |
    """)
    
    st.warning("""
    **Limitation**: Level 2 ignores district!
    
    A 100m² apartment in Gangnam costs much more than in other areas,
    but our model predicts the same price.
    
    **Next Level**: Add more features (district, floor, year)!
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
        display_model_info()
        st.markdown("---")
        display_evaluation(df)
        st.markdown("---")
        display_demo(df)
        st.markdown("---")
        display_comparison()
        
        # Next level teaser
        display_next_level_teaser(2)
        
    except Exception as e:
        st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
