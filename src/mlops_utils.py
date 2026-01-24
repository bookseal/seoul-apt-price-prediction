# -*- coding: utf-8 -*-
"""
MLOps Utilities for Part 2 (Seongsu Station Guidebook)
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_seongsu_data(days=365):
    """
    Generate synthetic data for Seongsu Station passenger prediction.
    Features:
    - Date
    - Temp (C): Seasonal simulated
    - Rain (mm): Random sparse
    - Holiday (bool): Weekend rule
    - Target: Passenger Count (influenced by features)
    """
    np.random.seed(42)
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(days)]
    
    data = []
    
    for date in dates:
        # 1. Seasonality for Temp
        day_of_year = date.timetuple().tm_yday
        avg_temp = 15 + 15 * np.sin((day_of_year - 100) / 365 * 2 * np.pi) 
        temp = avg_temp + np.random.normal(0, 3) # Noise
        
        # 2. Rain (Rainy season logic simplified)
        is_summer = 6 <= date.month <= 8
        rain_prob = 0.3 if is_summer else 0.1
        rain = 0.0
        if np.random.rand() < rain_prob:
            rain = np.random.exponential(10) # mm
            
        # 3. Holiday/Weekend
        is_weekend = date.weekday() >= 5
        is_holiday = is_weekend # Simplified
        
        # 4. Target Generation (Disembarkment Count)
        # Baseline
        passengers = 40000 
        
        # Seasonality (More people in Spring/Fall)
        if 4 <= date.month <= 5 or 9 <= date.month <= 10:
            passengers += 5000
            
        # Weekend Effect (Seongsu is a hot place, busy on weekends too, but maybe less than commute?)
        # Let's assume Seongsu is BUSY on Weekends due to cafes.
        if is_weekend:
            passengers += 15000
        else:
            # Weekday Commute
            passengers += 10000
            
        # Weather Effect
        if rain > 5:
            passengers -= 5000 # Rain discourages outing
        if temp < -5 or temp > 30:
            passengers -= 3000 # Too cold/hot
            
        # Random noise
        passengers += np.random.normal(0, 2000)
        
        data.append({
            'date': date,
            'temp_c': round(temp, 1),
            'rain_mm': round(rain, 1),
            'is_holiday': int(is_holiday),
            'passengers': int(passengers)
        })
        
    return pd.DataFrame(data)

def get_architect_note(level: int):
    """Return the 'Architect's Note' for each level."""
    notes = {
        11: "### 👨‍💻 Architect's Note: Data is Fuel\nML starts with Data. In real MLOps, we build **Data Pipelines** (ETL) to automate this collection from APIs/DBs daily.",
        12: "### 👨‍💻 Architect's Note: Quality Control\nGarbage In, Garbage Out. Preprocessing must be **reproducible**. Use the same scaler for training AND prediction!",
        13: "### 👨‍💻 Architect's Note: The Product\nUsers don't care about RMSE. They care about **Usability**. This dashboard is the 'Interface' of your AI System.",
        14: "### 👨‍💻 Architect's Note: AutoML\nDon't marry one model. Let the data decide. AutoML creates a **Baseline** quickly so you can focus on Feature Engineering.",
        15: "### 👨‍💻 Architect's Note: Automation\nManual running = Human Error. **Crontab** is the grandfather of automation. Simple, effective, and runs while you sleep.",
        16: "### 👨‍💻 Architect's Note: CI/CD\nCode that isn't tested is broken code. **GitHub Actions** ensures every 'Git Push' is verified before it touches the server.",
        17: "### 👨‍💻 Architect's Note: Experiment Tracking\nML is Science. If you don't write down your experiments (params, metrics), you are just guessing. **MLflow** is your lab notebook.",
        18: "### 👨‍💻 Architect's Note: Data Versioning\nCode uses Git. Data uses **DVC**. When a model breaks, we need to know EXACTLY which dataset trained it.",
        19: "### 👨‍💻 Architect's Note: Monitoring\nA deployed model starts dying immediately (Data Drift). **Monitoring** wakes you up when the model needs retraining.",
        20: "### 👨‍💻 Architect's Note: Orchestration\n**Airflow** is the conductor. It ensures Data -> Train -> Deploy happens in order, handling failures gracefully."
    }
    return notes.get(level, "")
