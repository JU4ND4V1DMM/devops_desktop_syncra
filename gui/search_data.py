import pandas.errors as pd_errors
import pandas as pd
import os

def search_values_in_csvs(directory, output_path, search_list, process_data):
    search_values = [v.strip() for v in search_list.split(',')]
    print(f"🔎 Searching for values: {search_values}")
    
    datetime_now = pd.Timestamp.now().strftime('%Y%m%d - %H%M')
    output_excel = os.path.join(output_path, f'Resultados Busqueda {datetime_now}.xlsx')
    
    results = {}
    print("📂 Searching through CSV files...")

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

    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            df = read_csv_dynamic(file_path)
            df = df.astype(str)
            
            if df is None:
                print(f"⛔ Skipping unreadable file: {filename}")
                continue

            for value in search_values:
                if df.isin([value]).any().any():
                    print(f"✅ Value '{value}' found in file: {filename}")
                    matching_rows = df[df.isin([value]).any(axis=1)].copy()
                    matching_rows['File'] = filename
                    if value not in results:
                        results[value] = []
                    results[value].append(matching_rows)
                else:
                    print(f"🔍 Value '{value}' not found in file: {filename}")

    if not results:
        print("❗ No matches found in any file. Nothing to export.")
        return None  # Or return {} if you want to return an empty dict

    # Create Excel file with one sheet per value
    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
        for value, df_list in results.items():
            combined_df = pd.concat(df_list, ignore_index=True)
            sheet_name = str(value)[:31]  # Max sheet name length in Excel
            combined_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"📁 Results exported to: {output_excel}")
    return results