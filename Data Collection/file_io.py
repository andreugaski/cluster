import json
import pandas as pd
import os
from config import OUTPUT_DIR

def json_to_csv(json_file, csv_file):
    """Convert a JSON file to CSV format"""
    try:
        # Read the JSON data
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
            
        # If data is empty, skip
        if not data:
            print(f"No data in {json_file}, skipping conversion")
            return False
            
        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(data)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"Successfully converted {json_file} to {csv_file}")
        return True
        
    except Exception as e:
        print(f"Error converting {json_file}: {e}")
        return False

def save_checkpoint(data, filename, current_idx):
    """Save data collected so far as a checkpoint"""
    checkpoint_file = os.path.join(OUTPUT_DIR, f"checkpoint_{filename}_{current_idx}.json")
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"Saved checkpoint to {checkpoint_file}")
