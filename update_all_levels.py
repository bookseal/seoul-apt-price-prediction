import os
import glob
import re
import json

# Define the base directory
BASE_DIR = "/data/ephemeral/home/workspace/seoul-apt-price-prediction"

# Notebook content template
def get_notebook_content(level_num, title, filename):
    streamlit_url = f"https://bookseal-seoul-apt-price-prediction.streamlit.app/{filename.replace('.ipynb', '')}"
    
    nb_content = {
     "cells": [
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        f"# 🎯 {title}\n",
        "\n",
        f"**[🏠 Back to Streamlit App]({streamlit_url})**\n",
        "\n",
        "This notebook accompanies the Seoul Apartment Price Prediction Roadmap.\n",
        "You can run the code below to see how the model works step-by-step."
       ]
      },
      {
       "cell_type": "code",
       "execution_count": None,
       "metadata": {},
       "outputs": [],
       "source": [
        "import pandas as pd\n",
        "import numpy as np\n",
        "import matplotlib.pyplot as plt\n",
        "\n",
        "# Load data from GitHub\n",
        "url = \"https://github.com/bookseal/seoul-apt-price-prediction/raw/main/data/sample.parquet\"\n",
        "df = pd.read_parquet(url)\n",
        "df.head()"
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "### 🚧 Under Construction\n",
        "The detailed code for this level is being polished! \n",
        "Stay tuned for updates." if level_num > 3 else "### 🚀 Implementation\n(Code logic would go here matching the level's topic)"
       ]
      }
     ],
     "metadata": {
      "kernelspec": {
       "display_name": "Python 3",
       "language": "python",
       "name": "python3"
      },
      "language_info": {
       "codemirror_mode": {
        "name": "ipython",
        "version": 3
       },
       "file_extension": ".py",
       "mimetype": "text/x-python",
       "name": "python",
       "nbconvert_exporter": "python",
       "pygments_lexer": "ipython3",
       "version": "3.8.5"
      }
     },
     "nbformat": 4,
     "nbformat_minor": 4
    }
    return nb_content

def update_streamlit_page(filepath, filename_base):
    with open(filepath, 'r') as f:
        content = f.read()
    
    # 1. Add Import
    if "from src.navigation import display_next_level_teaser" in content:
        if "display_code_link" not in content:
            content = content.replace(
                "from src.navigation import display_next_level_teaser",
                "from src.navigation import display_next_level_teaser, display_code_link"
            )
            print(f"Updated import in {filepath}")
    
    # 2. Add Function Call
    # Look for display_next_level_teaser(X)
    # The regex matches display_next_level_teaser(digit)
    match = re.search(r'display_next_level_teaser\(\d+\)', content)
    if match:
        call_str = match.group(0)
        # Check if already added
        if f'display_code_link("{filename_base}.ipynb")' not in content:
            insertion = f'{call_str}\n        \n        # Code Link\n        display_code_link("{filename_base}.ipynb")'
            content = content.replace(call_str, insertion)
            print(f"Added footer to {filepath}")
            
            with open(filepath, 'w') as f:
                f.write(content)
        else:
             print(f"Footer already present in {filepath}")
    else:
        # Special case for Level 10 which might use display_journey_complete
        if "display_journey_complete()" in content:
             if f'display_code_link("{filename_base}.ipynb")' not in content:
                content = content.replace(
                    "display_journey_complete()",
                    f'display_journey_complete()\n        \n        # Code Link\n        display_code_link("{filename_base}.ipynb")'
                )
                print(f"Added footer to Level 10 {filepath}")
                with open(filepath, 'w') as f:
                    f.write(content)
        else:
            print(f"Could not find anchor to insert footer in {filepath}")

def main():
    # 1. Process Levels 2-10 (Level 1 is already done, but notebook needs check)
    # actually user said apply to 1-10. I already did 1 and 2 manually for streamlit.
    # Level 1 Notebook needs update (Back link).
    
    # Update Level 1 Notebook Link
    nb1_path = os.path.join(BASE_DIR, "notebooks/Level_1_Heuristic.ipynb")
    if os.path.exists(nb1_path):
        with open(nb1_path, 'r') as f:
            nb1 = json.load(f)
        # Check if link exists
        src = nb1['cells'][0]['source']
        if not any("Back to Streamlit" in line for line in src):
            link = "**[🏠 Back to Streamlit App](https://bookseal-seoul-apt-price-prediction.streamlit.app/Level_1_Heuristic)**\n\n"
            nb1['cells'][0]['source'].insert(1, link)
            with open(nb1_path, 'w') as f:
                json.dump(nb1, f, indent=1)
            print("Updated Level 1 Notebook with Back Link")

    # Process 2 to 10
    pages_dir = os.path.join(BASE_DIR, "pages")
    notebooks_dir = os.path.join(BASE_DIR, "notebooks")
    
    for i in range(2, 11):
        # Find the page file
        pattern = os.path.join(pages_dir, f"{i}_Level_{i}_*.py")
        files = glob.glob(pattern)
        
        if not files:
            print(f"No file found for Level {i}")
            continue
            
        page_file = files[0]
        basename = os.path.basename(page_file).replace(".py", "") # e.g. 2_Level_2_Linear_Regression
        # We want the notebook name to be cleaner? e.g. Level_2_Linear_Regression
        # The streamlit file starts with "2_", "3_". I should probably strip the prefix for the notebook?
        # Level 1 notebook is "Level_1_Heuristic". Page is "1_Level_1_Heuristic".
        # So yes, strip the leading "N_"
        
        nb_filename_base = "_".join(basename.split("_")[1:]) # Level_2_Linear_Regression
        nb_filename = nb_filename_base + ".ipynb"
        
        # 1. Update Streamlit Page
        # Skip 2 as I did it manually (but check won't hurt)
        update_streamlit_page(page_file, nb_filename_base)
        
        # 2. Create Notebook
        nb_path = os.path.join(notebooks_dir, nb_filename)
        # Only create if doesn't exist to avoid overwriting work (except I want to ensure structure)
        if not os.path.exists(nb_path):
            title = nb_filename_base.replace("_", " ")
            content = get_notebook_content(i, title, nb_filename)
            with open(nb_path, 'w') as f:
                json.dump(content, f, indent=1)
            print(f"Created notebook {nb_filename}")
        else:
             print(f"Notebook {nb_filename} already exists")

if __name__ == "__main__":
    main()
