# -*- coding: utf-8 -*-
"""
Level 11: The MLOps 'Why' - The Morning Panic

Simulates a manual data science workflow that breaks when the world changes.
"""
import streamlit as st
import pandas as pd
import time
from src.mlops.data_simulator import generate_seongsu_data

def display_header():
    st.title("😱 Level 11: The MLOps 'Why'")
    st.warning("**Scenario**: You are the Junior Data Scientist at 'Hipster Inc'.")
    st.markdown("""
    Your boss loves your manual script that scrapes Seongsu Pop-up trends.
    Every morning, you come in, press a button, and email him the report.
    
    **Life is good... until today.**
    """)

def display_manual_process():
    st.header("🛠️ The Manual Script")
    
    day = st.radio("Select Day:", ["Day 1 (Monday)", "Day 2 (Tuesday)"], horizontal=True)
    
    if st.button("▶️ Run Daily Report"):
        progress_bar = st.progress(0)
        status = st.empty()
        
        status.info("1. Crawling Instagram/Blogs...")
        time.sleep(1)
        progress_bar.progress(30)
        
        # Simulate Data
        is_day_2 = "Day 2" in day
        df = generate_seongsu_data("2024-05-01", drift=is_day_2)
        
        status.info("2. Processing Data Schema...")
        time.sleep(1)
        progress_bar.progress(60)
        
        if is_day_2:
            # FAILURE SIMULATION
            status.error("❌ CRITICAL ERROR: KeyError: 'waiting_time_min'")
            st.code("""
            Traceback (most recent call last):
              File "daily_script.py", line 42, in <module>
                report['avg_wait'] = df['waiting_time']  # Column name changed on website!
              KeyError: 'waiting_time' (Expected 'waiting_time_min')
            """, language="python")
            
            st.error("""
            **😱 THE MORNING PANIC!**
            The website changed its HTML! Your script crashed.
            Your boss is asking for the report. You are fixing code while sweating.
            
            **This is why we need MLOps (Automated Pipelines & Tests).**
            """)
            
        else:
            # SUCCESS
            status.success("3. Generating Report... DONE!")
            progress_bar.progress(100)
            
            st.markdown("### 📊 Daily Trend Report")
            st.dataframe(df.head())
            st.bar_chart(df.set_index('category')['hashtag_count'])
            st.success("Email sent to Boss! (Phew, safe for today)")

    st.markdown("---")
    if "Day 2" in day:
        st.info("💡 **Next Level**: How do we fix this permanently? We need **Metaflow** (Pipelines).")

def main():
    display_header()
    st.markdown("---")
    display_manual_process()

if __name__ == "__main__":
    main()
