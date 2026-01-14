import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_seongsu_data(date: str, drift: bool = False) -> pd.DataFrame:
    """
    Generate synthetic "Yo-Zeum Seongsu" pop-up store data for a specific date.
    
    Args:
        date (str): YYYY-MM-DD
        drift (bool): If True, simulate data drift (trend shift).
    
    Returns:
        pd.DataFrame: Simulated crawling data.
    """
    np.random.seed(int(date.replace("-", "")))
    
    # Base Categories
    categories = ["Food", "Fashion", "Beauty", "Art", "Tech"]
    
    # Drift Logic: If drift=True, "Experience" and "Tech" become popular, "Food" drops.
    if drift:
        weights = [0.1, 0.2, 0.2, 0.1, 0.4] # Tech boom
    else:
        weights = [0.4, 0.3, 0.2, 0.05, 0.05] # Food dominated
        
    n_stores = np.random.randint(20, 50)
    
    data = []
    for i in range(n_stores):
        cat = np.random.choice(categories, p=weights)
        
        # Hashtag Logic
        base_hash = 1000 if cat in ["Food", "Fashion"] else 200
        if drift and cat == "Tech": base_hash = 2000
        
        hashtags = int(np.random.normal(base_hash, base_hash*0.3))
        hashtags = max(0, hashtags)
        
        # Store Name Generator
        prefixes = ["Super", "Mega", "Tiny", "Hip", "Retro", "Future"]
        suffixes = ["Store", "Space", "Lab", "Market", "Studio"]
        name = f"{np.random.choice(prefixes)} {cat} {np.random.choice(suffixes)}"
        
        data.append({
            "date": date,
            "store_name": name,
            "category": cat,
            "hashtag_count": hashtags,
            "blog_reviews": int(hashtags * 0.1 + np.random.randint(0, 50)),
            "waiting_time_min": int(hashtags / 50)
        })
        
    return pd.DataFrame(data)
