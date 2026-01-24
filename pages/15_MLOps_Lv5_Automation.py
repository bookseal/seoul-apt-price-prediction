# -*- coding: utf-8 -*-
"""
MLOps Level 5: Local Automation (Crontab)
"""
import streamlit as st
import time
from src.mlops_utils import get_architect_note
from src.navigation import display_mlops_sidebar

def display_crontab_explanation():
    st.header("1. What is Crontab?")
    st.markdown("Crontab is a built-in scheduler in Linux/Mac.")
    
    st.code("0 6 * * * python /home/user/train_model.py >> /var/log/train.log 2>&1", language="bash")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Min", "0")
    col2.metric("Hour", "6 (6AM)")
    col3.metric("Day", "* (Any)")
    col4.metric("Month", "* (Any)")
    col5.metric("Weekday", "* (Any)")

def simulate_automation():
    st.header("2. Simulation: Day in the Life of a Server")
    
    if st.button("▶️ Start Daily Schedule"):
        log_placeholder = st.empty()
        logs = []
        
        days = ["Mon", "Tue", "Wed", "Thu", "Fri"]
        
        for day in days:
            # 06:00 AM
            logs.append(f"[{day} 06:00:00] ⏰ CRON START: train_model.py")
            log_placeholder.code("\n".join(logs))
            time.sleep(0.5)
            
            # 06:01 AM - Data Fetch
            logs.append(f"[{day} 06:01:12] 📥 Data Fetched: 34,102 records")
            log_placeholder.code("\n".join(logs))
            time.sleep(0.5)
            
            # 06:02 AM - Train
            logs.append(f"[{day} 06:02:45] 🏋️ Training Complete. RMSE: 4102.3")
            log_placeholder.code("\n".join(logs))
            time.sleep(0.5)
            
            # 06:03 AM - Save
            logs.append(f"[{day} 06:03:01] 💾 Model Saved: model_v{days.index(day)+1}.pkl")
            log_placeholder.code("\n".join(logs))
            time.sleep(1.0)
            
        st.success("Weekly automation simulation complete!")

def main():
    display_mlops_sidebar(15)
    
    st.title("🤖 Level 15: Local Automation")
    st.markdown("**'The Server never sleeps, but you do.'**")
    
    display_crontab_explanation()
    st.markdown("---")
    simulate_automation()
    
    st.markdown("---")
    st.markdown(get_architect_note(15))

if __name__ == "__main__":
    main()
