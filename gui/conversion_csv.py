import pandas as pd
import os
from zipfile import BadZipFile

def convert_xlsx_to_csv(folder_path):

    if not os.path.exists(folder_path):
        print(f"The folder {folder_path} does not exist.")
        return
    
    for filename in os.listdir(folder_path):
        
        print(f"Processing file: {filename}")
        
        if filename.endswith(".xlsx"):

            file_path = os.path.join(folder_path, filename)
            csv_filename = filename.replace(".xlsx", ".csv")
            csv_path = os.path.join(folder_path, csv_filename)
            
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
                #df = pd.read_excel(file_path, engine='openpyxl', sheet_name='Base Castigo')
                df.to_csv(csv_path, index=False, sep=';')
                print(f"Converted: {file_path} to {csv_path}")

            except BadZipFile:
                print(f"Error reading {file_path}: File is not a valid Excel file.")
                continue

            except Exception as e:
                print(f"Error converting {file_path}: {e}")
                continue