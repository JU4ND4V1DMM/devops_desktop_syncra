import os
import shutil
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment

def save_to_csv(data_frame, output_path, filename, partitions, delimiter=","):
    partitions = int(partitions)
    
    now = datetime.now()
    time_file = now.strftime("%Y%m%d_%H%M")
    file_date = now.strftime("%Y%m%d")
    
    # Create a temporary folder to store initial files
    temp_folder_name = f"{filename}_{time_file}"
    temp_output_path = os.path.join(output_path, temp_folder_name)

    # Save the DataFrame into the temporary folder
    (data_frame
        .repartition(partitions)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", delimiter)
        .csv(temp_output_path)
    )

    # Remove unnecessary files
    for root, dirs, files in os.walk(temp_output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))

    # Move the CSV files from the temporary folder to the main output path
    for i, file in enumerate(os.listdir(temp_output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(temp_output_path, file)
            new_file_path = os.path.join(output_path, f"{filename} {file_date} {i}.csv")
            os.rename(old_file_path, new_file_path)

    # Delete the temporary folder
    if os.path.exists(temp_output_path):
        shutil.rmtree(temp_output_path)
        print(f"🗑️ Temporary folder deleted: {temp_output_path}")

    print(f"✔️ CSV files successfully moved to: {output_path}")

def format_excel_file(filepath):
    wb = load_workbook(filepath)
    ws = wb.active

    # Freeze the first row
    ws.freeze_panes = ws['A2']

    # Apply bold and center alignment to the first row
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal='center', vertical='center')

    wb.save(filepath)
    print(f"✨ Formatted header in file: {filepath}")

def save_to_xlsx(data_frame, output_path, filename, partitions):
    
    ## 
    now = datetime.now()
    file_date = now.strftime("%Y%m%d")
    folder_name = f"{filename} {file_date}"

    full_output_path = os.path.join(output_path, folder_name, filename)

    print(f"Saving DataFrame as Excel to {full_output_path}...")

    # Save using spark-excel package
    (data_frame
        .coalesce(partitions)
        .write
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("dataAddress", "'Details'!A1")
        .mode("overwrite")
        .save(full_output_path)
    )

    full_output_path = os.path.join(output_path, folder_name)
    
    # Remove temporary or unnecessary files
    for root, dirs, files in os.walk(full_output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))

    # Find the generated part file and rename it to .xlsx
    for file in os.listdir(full_output_path):
        if not file.endswith(".xlsx"):
            old_path = os.path.join(full_output_path, file)
            new_path = os.path.join(output_path, f"{folder_name}.xlsx")
            os.rename(old_path, new_path)
            format_excel_file(new_path)
            print(f"✔ Final file saved as: {new_path}")

    if os.path.exists(full_output_path):
        shutil.rmtree(full_output_path)
        print(f"🗑️ Deleted temporary folder: {full_output_path}")