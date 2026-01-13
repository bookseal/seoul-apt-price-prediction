import os
import glob
import re
import json

# Define the base directory
BASE_DIR = "/data/ephemeral/home/workspace/seoul-apt-price-prediction"

def update_notebook(filepath, level_num, filename):
    with open(filepath, 'r') as f:
        nb = json.load(f)
    
    streamlit_url = f"https://bookseal-seoul-apt-price-prediction.streamlit.app/{filename.replace('.ipynb', '')}"
    new_text = f"**[📖 Want a detailed explanation? Read the Manual (Streamlit App)]({streamlit_url})**"
    
    # Update first cell
    source = nb['cells'][0]['source']
    updated = False
    
    # Remove old link if present
    source = [line for line in source if "Back to Streamlit App" not in line]
    
    # Insert new link
    # Find where to insert (after title)
    insert_idx = 1
    for i, line in enumerate(source):
        if line.strip().startswith("#"):
            insert_idx = i + 1
            
    # Add newline before and after
    source.insert(insert_idx, "\n")
    source.insert(insert_idx + 1, f"{new_text}\n")
    
    nb['cells'][0]['source'] = source
    
    with open(filepath, 'w') as f:
        json.dump(nb, f, indent=1)
    print(f"Updated notebook text: {filepath}")

def update_streamlit_page(filepath, filename_base):
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Expected pattern to remove:
    # # Code Link
    # display_code_link("...")
    
    # Regex to remove existing calls (so we can re-insert them correctly)
    # Using specific filename to be safe
    remove_pattern = f'\\s+# Code Link\\s+display_code_link\\("{filename_base}.ipynb"\\)'
    content = re.sub(remove_pattern, '', content)
    
    # Now insert BEFORE the next level teaser
    # Pattern: display_next_level_teaser(N)
    
    teaser_match = re.search(r'(\s+)(# Next level teaser\s+)?display_next_level_teaser\(\d+\)', content)
    
    if teaser_match:
        indent = teaser_match.group(1)
        # We want to insert valid python code.
        # The match includes the newline/indent before the call.
        
        insertion = f'{indent}# Code Link{indent}display_code_link("{filename_base}.ipynb"){indent}'
        
        # Insert before the match
        start_idx = teaser_match.start()
        content = content[:start_idx] + insertion + content[start_idx:]
        print(f"Reordered footer in {filepath}")
        
    else:
        # Level 10 case: display_journey_summary()
        summary_match = re.search(r'(\s+)display_journey_summary\(\)', content)
        if summary_match:
            indent = summary_match.group(1)
            insertion = f'{indent}# Code Link{indent}display_code_link("{filename_base}.ipynb"){indent}'
            start_idx = summary_match.start()
            content = content[:start_idx] + insertion + content[start_idx:]
            print(f"Reordered footer in Level 10 {filepath}")
        else:
            print(f"Could not find anchor in {filepath}")
            
    with open(filepath, 'w') as f:
        f.write(content)

def main():
    pages_dir = os.path.join(BASE_DIR, "pages")
    notebooks_dir = os.path.join(BASE_DIR, "notebooks")
    
    # Level 1 to 10
    for i in range(1, 11):
        # Find page
        pattern = os.path.join(pages_dir, f"{i}_Level_{i}_*.py")
        files = glob.glob(pattern)
        if not files: continue
        
        page_file = files[0]
        basename = os.path.basename(page_file).replace(".py", "") 
        nb_filename_base = "_".join(basename.split("_")[1:])
        nb_filename = nb_filename_base + ".ipynb"
        
        # 1. Update Streamlit
        update_streamlit_page(page_file, nb_filename_base)
        
        # 2. Update Notebook
        nb_path = os.path.join(notebooks_dir, nb_filename)
        if os.path.exists(nb_path):
            update_notebook(nb_path, i, nb_filename)

if __name__ == "__main__":
    main()
