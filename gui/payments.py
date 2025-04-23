import pandas as pd
import os
from datetime import datetime

def clean_numeric_cta(value):
    """Clean numeric account values by removing periods and splitting by comma."""
    if isinstance(value, str):
        return value.replace('.', '').split(',')[0]  # Remove periods and take the first part if there's a comma
    return value

def clean_numeric_amount(value):
    """Clean numeric amount values by splitting on the first period and comma."""
    if isinstance(value, str):
        return value.split('.')[0].split(',')[0]  # Take the first part before any period or comma
    return value

def clean_date(value):
    """Convert date strings to yyyy-mm-dd format, handling various formats and time components."""
    if isinstance(value, str):
        # Split by space and take the first part to handle date with time
        date_part = value.split(' ')[0]
        # If there is a colon in the string, split by colon and take the first part
        if ':' in date_part:
            date_part = date_part.split(':')[0]
        # Attempt to parse and convert the date to yyyy-mm-dd format
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S", "%d/%m/%y", "%d-%m-%y"):
            try:
                # Try to convert the date using the current format
                parsed_date = datetime.strptime(date_part, fmt)
                return parsed_date.strftime('%Y-%m-%d')  # Format date to yyyy-mm-dd
            except ValueError:
                continue  # Try the next format
    return value  # Return the original value if not a string or not parsable

def process_txt(file_path):
    """Process a TXT file and clean its data."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()  # Read the first line to determine the delimiter
        # Determine the delimiter used in the file
        delimiter = '\t' if '\t' in first_line else ('|' if '|' in first_line else (';' if ';' in first_line else ' '))
        df = pd.read_csv(file_path, sep=delimiter, dtype=str)  # Read the file into a DataFrame
        return clean_dataframe(df)  # Clean and return the DataFrame
    except Exception as e:
        print(f"Error processing TXT {file_path}: {e}")
        return None

def process_csv(file_path):
    """Process a CSV file and clean its data."""
    try:
        df = pd.read_csv(file_path, sep=';', dtype=str, encoding='utf-8', 
                         skipinitialspace=True, on_bad_lines='skip')  # Read CSV file
        return clean_dataframe(df)  # Clean and return the DataFrame
    except Exception as e:
        print(f"Error processing CSV {file_path}: {e}")
        return None

def clean_dataframe(df):
    """Clean the DataFrame by renaming columns and applying cleaning functions."""
    # Rename and clean various columns in the DataFrame
    if 'Cuenta' in df.columns:
        df['Cuenta'] = df['Cuenta'].apply(clean_numeric_cta)
        df = df.rename(columns={'Cuenta': 'obligacion'})
    if 'CUENTA' in df.columns:
        df['CUENTA'] = df['CUENTA'].apply(clean_numeric_cta)
        df = df.rename(columns={'CUENTA': 'obligacion'})
    if 'Número de Cliente' in df.columns:
        df['Número de Cliente'] = df['Número de Cliente'].apply(clean_numeric_cta)
        df = df.rename(columns={'Número de Cliente': 'obligacion'})
        
    if 'Pago' in df.columns:
        df['Pago'] = df['Pago'].apply(clean_numeric_amount)
        df = df.rename(columns={'Pago': 'valor'})
    if 'PAGO' in df.columns:
        df['PAGO'] = df['PAGO'].apply(clean_numeric_amount)
        df = df.rename(columns={'PAGO': 'valor'})
    if 'Fecha de Creación' in df.columns:
        df['Fecha de Creación'] = df['Fecha de Creación'].apply(clean_numeric_amount)
        df = df.rename(columns={'Fecha de Creación': 'valor'})
        
    if 'Fecha' in df.columns:
        df['Fecha'] = df['Fecha'].apply(clean_date)
        df = df.rename(columns={'Fecha': 'fecha'})
    if 'FECHA_APLICACION' in df.columns:
        df['FECHA_APLICACION'] = df['FECHA_APLICACION'].apply(clean_date)
        df = df.rename(columns={'FECHA_APLICACION': 'fecha'})
    if 'Nombre Casa de Cobro' in df.columns:
        df['Nombre Casa de Cobro'] = df['Nombre Casa de Cobro'].apply(clean_date)
        df = df.rename(columns={'Nombre Casa de Cobro': 'fecha'})    
    return df[['obligacion', 'fecha', 'valor']]  # Return only the relevant columns

def process_excel(file_path):
    """Process an Excel file and clean its data."""
    try:
        xls = pd.ExcelFile(file_path)  # Load the Excel file
        df_list = []
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet, dtype=str)  # Read each sheet into a DataFrame
            if sheet == 'PAGOS':
                if 'CUSTCODE' in df.columns:
                    df['CUSTCODE'] = df['CUSTCODE'].str.replace('.', '', regex=False)
                    df = df.rename(columns={'CUSTCODE': 'obligacion'})
                if 'FECHA' in df.columns:
                    df['FECHA'] = df['FECHA'].apply(clean_date)
                    df = df.rename(columns={'FECHA': 'fecha'})
                if 'CACHKAMT' in df.columns:
                    df['CACHKAMT'] = df['CACHKAMT'].apply(clean_numeric_amount)
                    df = df.rename(columns={'CACHKAMT': 'valor'})
                df_list.append(df)  # Add the cleaned DataFrame to the list
        return pd.concat(df_list, ignore_index=True)  # Concatenate all DataFrames into one
    except Exception as e:
        print(f"Error processing Excel {file_path}: {e}")
        return None

def unify_payments(input_folder, output_folder):
    """Unify payment data from various files into a single CSV file."""
    try:
        # List all valid files in the input folder
        files = [f for f in os.listdir(input_folder) if f.lower().endswith(('.csv', '.txt', '.xlsx', '.xls', 'XLSX'))]
        if not files:
            raise FileNotFoundError("No valid files found in the input folder.")
        
        df_list = []
        for file_name in files:
            file_path = os.path.join(input_folder, file_name)  # Build the file path
            print(f"Payments Processing: {file_name}", end=' - Registers: ')
            # Process files based on their extension
            if file_name.endswith('.txt'):
                df = process_txt(file_path)
            elif file_name.endswith(('.xlsx', '.xls', 'XLSX')):
                df = process_excel(file_path)
            elif file_name.endswith('.csv'):
                df = process_csv(file_path)
            else:
                continue
            if df is not None:
                print(len(df))  # Print the number of records processed
                df_list.append(df)  # Add the cleaned DataFrame to the list
        
        if not df_list:
            raise ValueError("No valid data processed.")
        
        # Concatenate all DataFrames, remove duplicates, and reset index
        final_df = pd.concat(df_list, ignore_index=True).drop_duplicates()
        final_df['identificacion'] = ""  # Add empty columns for future use
        final_df['asesor'] = ""
        
        # Generate output file name and path
        output_file = f'Pagos {datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv'
        output_path = os.path.join(output_folder, output_file)
        
        # Convert 'valor' to numeric, fill NaNs, and filter out non-positive values
        final_df['valor'] = pd.to_numeric(final_df['valor'], errors='coerce')
        final_df['valor'] = final_df['valor'].fillna(0).astype(int).astype(str)
        final_df = final_df[final_df['valor'].astype(int) > 0]
        
        final_df['fecha'] = pd.to_datetime(final_df['fecha'], errors='coerce')
        current_date_str = datetime.now().strftime('%Y-%m-1')
        
        filtered_df = final_df[final_df['fecha'].dt.strftime('%Y-%m-%d') >= current_date_str]
        
        current_date = datetime.now()
        current_month = current_date.month
        current_year = current_date.year
        
        filtered_df = final_df[(final_df['fecha'].dt.year > current_year) | 
                        ((final_df['fecha'].dt.year == current_year) & 
                         (final_df['fecha'].dt.month >= current_month))]
        
        # Save the final DataFrame to a CSV file
        output_folder = f"{output_folder}---- Bases para CARGUE ----/"
        filtered_df[['obligacion', 'identificacion', 'fecha', 'valor', 'asesor']].to_csv(output_folder, index=False, sep=';')
        print(f"\nData saved to {output_path} with {len(final_df)} records.")
        
    except Exception as e:
        print(f"Error during unification: {e}")