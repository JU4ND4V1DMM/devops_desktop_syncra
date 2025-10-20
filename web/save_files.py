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
    """
    try:
        wb = load_workbook(filepath)
        ws = wb.active
        ws.freeze_panes = ws['A2']

        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal='center', vertical='center')

        wb.save(filepath)
        print(f"✨ Formatted header in file: {filepath}")
    except Exception as e:
        print(f"ERROR: Could not format Excel file at {filepath}. Reason: {e}")


def save_to_csv(data_frame, output_path: str, filename: str, partitions: Union[int, str], delimiter: str = ","):
    """
    Saves a DataFrame (Polars or PySpark) to a single CSV file.
    Tries Polars first for performance, falls back to PySpark if conversion fails.
    """
    if output_path and not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"✔️ Created output directory: {output_path}")
    
    now = datetime.now()
    file_date = now.strftime("%Y%m%d")
    final_file_path = os.path.join(output_path, f"{filename} {file_date}.csv")

    try:
        # ✅ Intentar con Polars primero
        print("💾 Intentando guardar con Polars...")
        if not isinstance(data_frame, pl.DataFrame):
            # Conversión desde PySpark o Pandas
            if hasattr(data_frame, "toPandas"):  # PySpark
                data_frame = pl.from_pandas(data_frame.toPandas())
            elif isinstance(data_frame, pd.DataFrame):
                data_frame = pl.from_pandas(data_frame)

        data_frame.write_csv(file=final_file_path, separator=delimiter, include_header=True)
        print(f"✅ CSV file successfully saved to: {final_file_path}")

    except Exception as e:
        print(f"⚠️ Polars CSV save failed ({e}), trying PySpark fallback...")
        try:
            # ⚙️ Guardar con PySpark
            output_dir = os.path.splitext(final_file_path)[0]  # carpeta sin extensión
            data_frame.write.mode("overwrite").option("header", True).csv(output_dir)
            print(f"✅ CSV saved with PySpark to: {output_dir}")
        except Exception as spark_err:
            print(f"❌ ERROR: Failed to save with both Polars and PySpark. Reason: {spark_err}")


def save_to_0csv(data_frame, output_path: str, filename: str, partitions: Union[int, str], delimiter: str = ","):
    """
    Saves a DataFrame (Polars or PySpark) to a '.0csv' file.
    Tries Polars first, falls back to PySpark if needed.
    """
    if output_path and not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"✔️ Created output directory: {output_path}")
    
    now = datetime.now()
    file_date = now.strftime("%Y%m%d")
    final_file_path = os.path.join(output_path, f"{filename} {file_date}.0csv")

    try:
        # ✅ Intentar con Polars primero
        print("💾 Intentando guardar con Polars (.0csv)...")
        if not isinstance(data_frame, pl.DataFrame):
            if hasattr(data_frame, "toPandas"):  # PySpark
                data_frame = pl.from_pandas(data_frame.toPandas())
            elif isinstance(data_frame, pd.DataFrame):
                data_frame = pl.from_pandas(data_frame)

        data_frame.write_csv(file=final_file_path, separator=delimiter, include_header=True)
        print(f"✅ .0csv file successfully saved to: {final_file_path}")

    except Exception as e:
        print(f"⚠️ Polars .0csv save failed ({e}), trying PySpark fallback...")
        try:
            output_dir = os.path.splitext(final_file_path)[0]
            data_frame.write.mode("overwrite").option("header", True).csv(output_dir)
            print(f"✅ .0csv saved with PySpark to: {output_dir}")
        except Exception as spark_err:
            print(f"❌ ERROR: Failed to save with both Polars and PySpark. Reason: {spark_err}")


def save_to_xlsx(data_frame: PolarsDataFrame, output_path: str, filename: str, partitions: Union[int, str]):
    """
    Saves a Polars DataFrame to a single Excel (XLSX) file via Pandas, 
    then applies custom openpyxl formatting.
    """
    if output_path and not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"✔️ Created output directory: {output_path}")

    now = datetime.now()
    file_date = now.strftime("%Y%m%d")
    final_file_name = f"{filename} {file_date}.xlsx"
    final_file_path = os.path.join(output_path, final_file_name)

    print(f"💾 Saving DataFrame as Excel to {final_file_path}...")
    try:
        # Convertir Polars o PySpark a Pandas
        if hasattr(data_frame, "toPandas"):  # PySpark
            df_pandas = data_frame.toPandas()
        elif isinstance(data_frame, pl.DataFrame):
            df_pandas = data_frame.to_pandas()
        else:
            df_pandas = data_frame

        df_pandas.to_excel(excel_writer=final_file_path, sheet_name='Details', index=False)
        format_excel_file(final_file_path)
        print(f"✅ Final Excel file saved as: {final_file_path}")

    except Exception as e:
        print(f"❌ ERROR: Failed to save DataFrame to XLSX. Reason: {e}")