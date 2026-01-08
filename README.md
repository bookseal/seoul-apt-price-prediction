# 📚 Seoul Apartment Price Prediction - ML Roadmap

A step-by-step guide to learn machine learning through apartment price prediction.
Built for beginners who want to become AI developers.

## 🔗 Live Demo
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://bookseal-seoul-apt-price-prediction.streamlit.app)

## 🎯 Project Philosophy

**"Start Simple, Scale Smart"**

This project guides you from complete beginner to ML practitioner through incremental learning.

## 📖 Learning Roadmap

### Level 1: Understanding Data
| Chapter | Topic | What You'll Learn |
|---------|-------|-------------------|
| 1.1 | Explore Data | Dataset structure, basic statistics |
| 1.2 | EDA | Distributions, visualizations, patterns |
| 1.3 | Sampling | Stratified sampling, Parquet format |

### Level 2: Building Your First Model
| Chapter | Topic | What You'll Learn |
|---------|-------|-------------------|
| 2.1 | Feature Selection | Correlation analysis, choosing features |
| 2.2 | Linear Regression | Model fundamentals, prediction formula |
| 2.3 | Model Evaluation | RMSE, residual analysis |
| 2.4 | Prediction Demo | Using trained models, limitations |

### Coming Soon
- **Level 3**: Multiple features, tree-based models, cross-validation
- **Level 4**: Ensemble methods, hyperparameter tuning, MLOps

## 📦 Repository Structure

```
seoul-apt-price-prediction/
├── app.py                      # Learning roadmap hub
├── pages/
│   ├── 1_1_Explore_Data.py    # Level 1 chapters
│   ├── 1_2_EDA.py
│   ├── 1_3_Sampling.py
│   ├── 2_1_Feature_Selection.py  # Level 2 chapters
│   ├── 2_2_Linear_Regression.py
│   ├── 2_3_Model_Evaluation.py
│   └── 2_4_Prediction_Demo.py
├── src/
│   ├── config.py              # Configuration settings
│   ├── io.py                  # Data loading utilities
│   ├── plots.py               # Visualization functions
│   ├── model.py               # Model utilities
│   ├── data_loader.py         # CSV data loaders
│   └── utils.py               # Helper functions
├── data/
│   ├── raw/                   # Original CSV files
│   └── sample.parquet         # 100K stratified sample
├── models/                    # Trained model files
├── train.py                   # Model training script
└── requirements.txt           # Dependencies
```

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the App
```bash
streamlit run app.py
```

### 3. Train Your Own Model
```bash
python train.py
```

## 📊 Dataset

Seoul apartment real transaction price data:
- **Original**: 1.1M+ rows
- **Sample**: 100K rows (stratified by district × year)
- **Format**: Parquet for fast I/O

## 🛠️ Tech Stack

- **Framework**: Streamlit
- **ML**: scikit-learn
- **Data**: Pandas, PyArrow
- **Visualization**: Plotly, Matplotlib

## 🙌 Credits

Built as an educational ML project.
