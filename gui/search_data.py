import pandas.errors as pd_errors
import pandas as pd
import os
from openpyxl import load_workbook
import time

def search_values_in_csvs_and_excels(directory, output_path, search_list, process_data):
    search_values = [v.strip() for v in search_list.split(',')]
    print(f"🔎 Searching for values: {search_values}")
    
    datetime_now = pd.Timestamp.now().strftime('%Y%m%d - %H%M')
    output_excel = os.path.join(output_path, f'Resultados Busqueda {datetime_now}.xlsx')
    
    results = {}
    time_details = []  # 👈 Lista para almacenar los detalles de tiempo
    print("📂 Searching through CSV and Excel files...")

    def read_csv_dynamic(file_path, delimiter=';'):
        try:
            return pd.read_csv(file_path, delimiter=delimiter, encoding='utf-8')
        except UnicodeDecodeError:
            print(f"⚠️ Encoding issue in {file_path}. Retrying with 'latin-1'...")
            try:
                return pd.read_csv(file_path, delimiter=delimiter, encoding='latin-1')
            except Exception as e:
                print(f"❌ Still failed with 'latin-1': {e}")
                return None
        except pd_errors.EmptyDataError:
            print(f"⚠️ Empty file: {file_path}")
            return None
        except pd_errors.ParserError as e:
            print(f"❌ Parsing error in {file_path}: {e}")
            try:
                print(f"⚠️ Trying to skip bad lines in {file_path}...")
                return pd.read_csv(file_path, delimiter=delimiter, encoding='utf-8', on_bad_lines='skip')
            except Exception as e2:
                print(f"❌ Could not recover from parse error: {e2}")
                return None
        except Exception as e:
            print(f"❌ Unexpected error in {file_path}: {e}")
            return None

    # --- Search CSVs ---
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            start_time = time.time()
            start_datetime = pd.Timestamp.now()
            
            df = read_csv_dynamic(file_path)
            
            status = 'Éxito'
            if df is None:
                print(f"⛔ Skipping unreadable file: {filename}")
                status = 'Fallo'
            else:
                df = df.astype(str)
                for value in search_values:
                    if df.isin([value]).any().any():
                        print(f"✅ Value '{value}' found in file: {filename}")
                        matching_rows = df[df.isin([value]).any(axis=1)].copy()
                        matching_rows['Source_File'] = f"{filename}"
                        matching_rows['Source_Sheet'] = 'N/A'
                        
                        if value not in results:
                            results[value] = []
                        results[value].append(matching_rows)
                    else:
                        print(f"🔍 Value '{value}' not found in file: {filename}")
            
            end_time = time.time()
            end_datetime = pd.Timestamp.now()
            duration = end_time - start_time
            print(f"⏱️ Time taken for file '{filename}': {duration:.2f} seconds")
            
            for value in search_values:
                time_details.append({
                    'Archivo': filename,
                    'Fecha_Inicio': start_datetime,
                    'Fecha_Fin': end_datetime,
                    'Segundos_Totales': duration,
                    'Estado': status,
                    'Valor_Buscado': value
                })
    
    # --- Search Excel files ---
    for filename in os.listdir(directory):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(directory, filename)
            start_time = time.time()
            start_datetime = pd.Timestamp.now()
            
            status = 'Éxito'
            try:
                xls = pd.ExcelFile(file_path)
                sheet_names = xls.sheet_names
            except Exception as e:
                print(f"❌ Could not read Excel file {filename}: {e}")
                status = 'Fallo'
                sheet_names = []

            for sheet_name in sheet_names:
                try:
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    df = df.astype(str)
                except Exception as e:
                    print(f"❌ Could not read sheet '{sheet_name}' from {filename}: {e}")
                    status = 'Fallo'
                    continue

                for value in search_values:
                    if df.isin([value]).any().any():
                        print(f"✅ Value '{value}' found in file: {filename} (Sheet: {sheet_name})")
                        matching_rows = df[df.isin([value]).any(axis=1)].copy()
                        matching_rows['Source_File'] = filename
                        matching_rows['Source_Sheet'] = sheet_name
                        
                        if value not in results:
                            results[value] = []
                        results[value].append(matching_rows)
                    else:
                        print(f"🔍 Value '{value}' not found in file: {filename} (Sheet: {sheet_name})")
            
            end_time = time.time()
            end_datetime = pd.Timestamp.now()
            duration = end_time - start_time
            print(f"⏱️ Time taken for file '{filename}': {duration:.2f} seconds")
            
            for value in search_values:
                time_details.append({
                    'Archivo': filename,
                    'Fecha_Inicio': start_datetime,
                    'Fecha_Fin': end_datetime,
                    'Segundos_Totales': duration,
                    'Estado': status,
                    'Valor_Buscado': value
                })
    
    if not results:
        print("❗ No matches found in any file. Nothing to export.")
        if not time_details: # Export time_details even if no matches are found
            return None
    
    # Create Excel file with one sheet per value and a 'TimeDetail' sheet
    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
        for value, df_list in results.items():
            combined_df = pd.concat(df_list, ignore_index=True)
            sheet_name = str(value)[:31].replace('[', '').replace(']', '').replace('*', '')
            combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Write the time_details to a separate sheet
        if time_details:
            time_df = pd.DataFrame(time_details)
            time_df.to_excel(writer, sheet_name='TimeDetail', index=False)

    print(f"📁 Results exported to: {output_excel}")
    return results