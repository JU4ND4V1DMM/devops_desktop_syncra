import pandas as pd
from datetime import datetime
import os
import re

def transform_csv_to_excel_dashboard(input_folder, output_folder):
    """
    Reads CSV files from an input folder, transforms them based on predefined
    mappings and logic, and saves them as Excel files in an output folder.

    Args:
        input_folder (str): The path to the folder containing the source CSV files.
        output_folder (str): The path to the folder where the output Excel files will be saved.
    """
    # --- Define all mappings and configurations (can be passed as arguments too) ---
    # Define the column mapping from source names to target names
    
    output_folder = f"{output_folder}/Transformación {datetime.now().strftime('%Y-%m-%d')} DASHBOARD/"
    
    source_to_target_mapping = {
        "IDENTIFICACION": "Identificacion",
        "CUENTA": "Cuenta_Next",
        "MORA": "Edad_Mora",
        "PRODUCTO": "CRM",
        "mod_init_cta": "Saldo_Asignado",
        "SEGMENTO": "Segmento",
        "VALOR_PAGO": "Form_Moneda",
        "NOMBRE_CORREGIDO": "Nombre_Completo",
        "RANGO_DEUDA": "Rango",
        "REFERENCIA": "Referencia",
        "TELEFONOS": "Dato_Contacto",
        "MORA": "marca2",
        "DESCUENTO": "DCTO",
        "VALOR_DE_PAGAR": "DEUDA_REAL",
        "FLP": "FLP",
        "PRODUCTO": "PRODUCTO",
        "TIPO_PAGO": "TIPO_PAGO",
        "MEJOR_PERFIL": "MEJOR PERFIL",
        "DIASMORA": "DIAS DE MORA",
        "NOMBRE_CORTO": "NOMBRE CORTO",
        "TIPO_DE_BASE": "TIPO BASE",
        "OUTPUT_DATA": "SMS",
        "REQUEST_ID": "REQUEST ID"
    }

    # Define the final column order
    final_column_order = [
        "Identificacion", "Cuenta_Next", "Cuenta", "Fecha_Asignacion", "Edad_Mora", "CRM", "Saldo_Asignado",
        "Segmento", "Form_Moneda", "Nombre_Completo", "Rango", "Referencia", "Dato_Contacto",
        "Hora_Envio", "Hora_Real", "Fecha_Envio", "marca2", "DCTO", "DEUDA_REAL", "FLP",
        "PRODUCTO", "fechapromesa", "TIPO_PAGO", "MEJOR PERFIL", "DIAS DE MORA",
        "RANKING STATUS", "CANTIDAD SERVICIOS", "NOMBRE CORTO", "TIPO BASE", "SMS", "REQUEST ID"
    ]

    # Define the CRM translation mapping
    crm_translation_map = {
        'Postpago': 'BSCS',
        'Equipo': 'ASCARD',
        'Hogar': 'RR',
        'Negocios': 'SGA'
    }

    date_file = datetime.now().strftime("%Y-%m")

    # Define the columns to be filled with literal values
    literal_columns = {
        "Fecha_Asignacion": date_file,
        "fechapromesa": "Desconocida",
        "RANKING STATUS": "Dinámico",
        "CANTIDAD SERVICIOS": 0,
    }
    # --- End of configurations ---

    # Ensure the destination folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Iterate through all files in the source folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            print(f"Processing file: {filename}")

            try:
                # Read the CSV file with the semicolon delimiter
                df = pd.read_csv(file_path, sep=";", encoding='latin1', low_memory=False)
                
                # Normalize column names to uppercase and remove leading/trailing spaces
                df.columns = [col.upper().strip() for col in df.columns]

                # --- Step 1: Extract date and time from the filename ---
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
                time_real_match = re.search(r'(\d{2}-\d{2})\.csv', filename)
                time_envio_match = re.search(r'_(\d{4})\s', filename)

                fecha_envio = pd.to_datetime(date_match.group(1), format='%Y-%m-%d').date() if date_match else None
                hora_real_str = time_real_match.group(1).replace('-', ':') if time_real_match else None
                hora_envio_str = time_envio_match.group(1) if time_envio_match else None
                
                if hora_envio_str:
                    hora_envio_str = f"{hora_envio_str[:2]}:{hora_envio_str[2:]}:00"
                
                # --- Step 2: Create the new DataFrame with mapped columns ---
                new_df = pd.DataFrame()
                
                reverse_mapping = {v: k for k, v in source_to_target_mapping.items()}
                
                for target_col in final_column_order:
                    # Handle special cases and literals
                    if target_col in ['Fecha_Envio', 'Hora_Real', 'Hora_Envio']:
                        new_df[target_col] = pd.Series([None] * len(df))
                    elif target_col in literal_columns:
                        new_df[target_col] = literal_columns[target_col]
                    elif target_col == 'Cuenta_Next' and 'CUENTA' in df.columns:
                        new_df['Cuenta_Next'] = df['CUENTA']
                    elif target_col == 'Cuenta' and 'CUENTA' in df.columns:
                        new_df['Cuenta'] = df['CUENTA']
                    elif target_col == 'CRM' and 'PRODUCTO' in df.columns:
                        new_df['CRM'] = df['PRODUCTO']
                    elif target_col == 'Saldo_Asignado' and 'MOD_INIT_CTA' in df.columns:
                        new_df['Saldo_Asignado'] = df['MOD_INIT_CTA']
                    elif target_col == 'Edad_Mora' and 'MORA' in df.columns:
                        new_df['Edad_Mora'] = df['MORA']
                    elif target_col == 'Dato_Contacto' and 'TELEFONOS' in df.columns:
                        new_df['Dato_Contacto'] = df['TELEFONOS']
                    else:
                        source_col = reverse_mapping.get(target_col)
                        if source_col and source_col in df.columns:
                            new_df[target_col] = df[source_col]
                        else:
                            new_df[target_col] = None

                # --- Step 3: Apply post-processing and transformations ---
                new_df['Fecha_Envio'] = fecha_envio
                new_df['Hora_Real'] = hora_real_str
                new_df['Hora_Envio'] = hora_envio_str

                new_df['CRM'] = new_df['CRM'].map(crm_translation_map).fillna(new_df['CRM'])
                
                if 'Cuenta_Next' in new_df.columns:
                    new_df['Cuenta_Next'] = new_df['Cuenta_Next'].astype(str).str.replace('-', '', regex=False)
                if 'Cuenta' in new_df.columns:
                    new_df['Cuenta'] = new_df['Cuenta'].astype(str).str.replace('-', '', regex=False)

                # --- Step 4: Save the transformed DataFrame to a new Excel file ---
                output_filename = filename.replace(".csv", ".xlsx")
                output_filepath = os.path.join(output_folder, output_filename)
                
                new_df.to_excel(output_filepath, sheet_name='Hoja1', index=False, engine='openpyxl')
                
                print(f"Successfully processed {filename} and saved as {output_filename}")

            except Exception as e:
                print(f"Error processing file {filename}: {e}")

    print("\nAll files processed.")