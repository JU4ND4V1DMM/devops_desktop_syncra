import polars as pl
import os
import datetime # The import is kept, just in case the timestamp is needed later.
from typing import List

def convert_csv_folder_to_json(input_folder: str, output_base_path: str):
    """
    Reads all CSV files delimited by ';' in the input folder.
    Creates an output folder named 'JSON_[date] [input_folder_name]' 
    inside output_base_path.
    Applies encoding and data type corrections.
    """
    # 1. DEFINE SCHEMA OVERRIDES (Includes all problematic columns)
    schema_fix = {
        # Strings that may contain special characters
        '2_': pl.String,   
        '20_': pl.String,  # IDs with - ('900577698-1', '-')
        '27_': pl.String,  # IDs with - ('900577698-1', '-')
        '38_': pl.String,  # Emails/text (Note: This column is also defined below as Float64, which will override this setting)
        '47_': pl.String,  # Emails/text
        '48_': pl.String,  # Text ('@')
        '49_': pl.String,  # Emails/text (Note: This column is defined twice, Polars uses the last definition)
        '52_': pl.String,  # Dates
        '57_': pl.String,  # Dates

        # Values that should be numeric (Amounts)
        '32_': pl.Float64, # Amounts
        '33_': pl.Float64, # Amounts
        '36_': pl.Float64, # Amounts
        '39_': pl.Float64, # Amounts
    }
    
    # 2. CONFIGURE OUTPUT PATH
    # Gets the name of the input folder (e.g., 'my_csv_data')
    folder_name = os.path.basename(os.path.normpath(input_folder))
    # Creates the output folder with the prefix 'JSON_[date]'
    timedate = datetime.datetime.now().strftime("%Y%m%d")
    final_output_folder = os.path.join(output_base_path, f"JSON_{timedate} {folder_name}")
    
    os.makedirs(final_output_folder, exist_ok=True)
    print(f"📁 Output folder created: {final_output_folder}")
    
    # 3. Get the list of CSV files
    csv_files: List[str] = [
        f for f in os.listdir(input_folder) 
        if f.lower().endswith('.csv')
    ]
    
    if not csv_files:
        print(f"⚠️ No CSV files found in: {input_folder}")
        return

    print(f"📦 Found {len(csv_files)} CSV files. Starting conversion...")

    # 4. Iterate over each file and process it
    for filename in csv_files:
        csv_path = os.path.join(input_folder, filename)
        json_filename = filename.replace('.csv', '.json')
        json_path = os.path.join(final_output_folder, json_filename)
        
        try:
            # EAGER READ with encoding and schema correction
            df = pl.read_csv(
                csv_path,
                separator=';',
                encoding='windows-1252', # Encoding fix
                schema_overrides=schema_fix, # Data type fix
            )
            
            # CONVERSION AND WRITING (JSON Lines for efficiency)
            df.write_ndjson(json_path)

            print(f"✅ Converted: {filename} -> {json_filename}")

        except Exception as e:
            print(f"❌ CRITICAL ERROR processing {filename}: {e}")
            
    print("✨ Conversion process finished with schema corrections applied.")

# --- Usage Example (Adjust these paths) ---
# output_base_path = "C:/Users/C.operativo/Downloads" 
# input_folder = "C:/Users/C.operativo/Documents/DatosCSV" 

# convert_csv_folder_to_json(input_folder, output_base_path)