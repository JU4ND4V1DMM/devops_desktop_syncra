import pandas as pd
import os

def search_values_in_csvs(directory, search_values, output_excel):
    """
    Busca valores en archivos CSV de un directorio y guarda los resultados en un Excel.
    :param directory: Carpeta donde buscar los CSV.
    :param search_values: Lista de valores a buscar.
    :param output_excel: Ruta del archivo Excel de salida.
    """
    results = {}
    print("Searching for values in CSV files...")

    def read_csv_dynamic(file_path, delimiter=';'):
        try:
            return pd.read_csv(file_path, delimiter=delimiter, encoding='utf-8')
        except UnicodeDecodeError:
            print(f"Error reading {file_path} with utf-8. Trying with 'latin-1'.")
            return pd.read_csv(file_path, delimiter=delimiter, encoding='latin-1')

    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            df = read_csv_dynamic(file_path)
            for value in search_values:
                if df.isin([value]).any().any():
                    print("Value found in file:", filename)
                    matching_rows = df[df.isin([value]).any(axis=1)]
                    if value not in results:
                        results[value] = []
                    results[value].append((filename, matching_rows))
                else:
                    print("Value not found in file:", filename)

    for value, files in results.items():
        print(f"Value '{value}' found in the following files:")
        for filename, matching_rows in files:
            print(f"\tFile: {filename}")
            print(matching_rows)

    with pd.ExcelWriter(output_excel) as writer:
        for value, files in results.items():
            for idx, (filename, matching_rows) in enumerate(files):
                sheet_name = f'{value}_{idx+1}' if len(files) > 1 else f'{value}'
                matching_rows.to_excel(writer, sheet_name=sheet_name, index=False)

# Ejemplo de uso:
# search_values_in_csvs(
#     directory=r"C:\Users\C.operativo\Downloads\New folder",
#     search_values=[3138192586, 3143556842],
#     output_excel=