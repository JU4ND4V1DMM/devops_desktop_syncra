import os
import shutil
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment
import polars as pl
import pandas as pd
from typing import Union

# Define the Polars DataFrame type for clarity
PolarsDataFrame = pl.DataFrame


def format_excel_file(filepath: str):
    """
    Formats the header of a single Excel file (XLSX) using openpyxl by applying bold, 
    center alignment, and freezing the first row. This function is file I/O based.

    Args:
        filepath: The path to the Excel file to format.
    """
    try:
        # Load the workbook and get the active sheet
        wb = load_workbook(filepath)
        ws = wb.active

        # Freeze the first row (starting from A2)
        ws.freeze_panes = ws['A2']

        # Apply bold and center alignment to all cells in the first row
        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal='center', vertical='center')

        # Save the changes back to the file
        wb.save(filepath)
        print(f"✨ Formatted header in file: {filepath}")
    except Exception as e:
        print(f"ERROR: Could not format Excel file at {filepath}. Reason: {e}")


def save_to_csv(data_frame: PolarsDataFrame, output_path: str, filename: str, partitions: Union[int, str], delimiter: str = ","):
    """
    Saves a Polars DataFrame directly to a single CSV file. 
    The complex PySpark cleanup and renaming logic is removed for maximum speed.

    Args:
        data_frame: The Polars DataFrame to be saved.
        output_path: The base directory where the final file will reside.
        filename: The base name for the output file.
        partitions: Retained for compatibility but ignored, as Polars writes a single file.
        delimiter: The delimiter to use in the CSV file (default is comma).
    """
    
    # 1. Prepare directory and filename
    if output_path and not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"✔️ Created output directory: {output_path}")
    
    now = datetime.now()
    file_date = now.strftime("%Y%m%d")
    
    # Polars writes to a single file, simplifying the naming convention
    final_file_path = os.path.join(output_path, f"{filename} {file_date}.csv")
    
    try:
        # 2. Polars writing logic
        data_frame.write_csv(
            file=final_file_path,
            separator=delimiter,
            include_header=True
        )
        print(f"✔️ CSV file successfully saved to: {final_file_path}")
    except Exception as e:
        print(f"ERROR: Failed to save DataFrame to CSV. Reason: {e}")


def save_to_0csv(data_frame: PolarsDataFrame, output_path: str, filename: str, partitions: Union[int, str], delimiter: str = ","):
    """
    Saves a Polars DataFrame to a file with the '.0csv' extension. 
    Optimized for speed by writing directly to a single file.

    Args:
        data_frame: The Polars DataFrame to be saved.
        output_path: The base directory where the final file will reside.
        filename: The base name for the output file.
        partitions: Retained for compatibility but ignored.
        delimiter: The delimiter to use in the CSV file (default is comma).
    """
    
    # 1. Prepare directory and filename
    if output_path and not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"✔️ Created output directory: {output_path}")
        
    now = datetime.now()
    file_date = now.strftime("%Y%m%d")
    
    # Polars writes to a single file, using the .0csv extension
    final_file_path = os.path.join(output_path, f"{filename} {file_date}.0csv")

    print(f"Saving Polars DataFrame to 0CSV: {final_file_path}")

    try:
        # 2. Polars writing logic
        data_frame.write_csv(
            file=final_file_path,
            separator=delimiter,
            include_header=True
        )
        print(f"✔️ .0csv file successfully saved to: {final_file_path}")
    except Exception as e:
        print(f"ERROR: Failed to save Polars DataFrame to .0csv. Reason: {e}")


def save_to_xlsx(data_frame: PolarsDataFrame, output_path: str, filename: str, partitions: Union[int, str]):
    """
    Saves a Polars DataFrame to a single Excel (XLSX) file via Pandas, 
    then applies custom openpyxl formatting. The partitions argument is ignored.

    Args:
        data_frame: The Polars DataFrame to be saved.
        output_path: The base directory for the output file.
        filename: The base name for the output file.
        partitions: Retained for compatibility but ignored.
    """
    
    # 1. Prepare directory and filename
    if output_path and not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"✔️ Created output directory: {output_path}")

    now = datetime.now()
    file_date = now.strftime("%Y%m%d")
    
    final_file_name = f"{filename} {file_date}.xlsx"
    final_file_path = os.path.join(output_path, final_file_name)

    print(f"Saving DataFrame as Excel to {final_file_path}...")
    
    try:
        # 2. Convert Polars to Pandas and save to Excel
        # Use 'Details' sheet name to match the original PySpark-Excel option
        data_frame.to_pandas().to_excel(
            excel_writer=final_file_path,
            sheet_name='Details', 
            index=False # Do not save the Pandas index
        )
        
        # 3. Apply custom formatting
        format_excel_file(final_file_path)
        print(f"✔ Final file saved as: {final_file_path}")

    except Exception as e:
        print(f"ERROR: Failed to save Polars DataFrame to XLSX. Reason: {e}")