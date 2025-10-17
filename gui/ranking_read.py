import os
from PyQt6.QtWidgets import QMessageBox
from datetime import datetime
import pandas as pd

def process_ranking_files(input_folder, output_file):
    """
    Processes Excel files in a folder, filters and transforms data, and saves the result to a CSV file.

    Args:
        input_folder (str): Path to the folder containing the Excel files.
        output_file (str): Path to save the resulting CSV file.
    """
    all_data = []
    all_data_detail = []
    unprocessed_files = []  # List to track files with 0 records
    
    # Define possible column names for dynamic handling
    cuenta_columns = ["raiz", "cuenta"]
    estado_columns = ["gestion", "recuperada"]
    filter_columns = ["aliado", "casa", "casacobro", "agencia"]
    servicios_column = "nservicios"
    pago_column = ["pago"]
    datepayment_column = ["fechadepago"]
    concept_column = ["concepto", "estadoactual"]

    # Iterate through all files in the input folder
    for file in os.listdir(input_folder):
        if file.endswith(".xlsx") or file.endswith(".xls"):
            file_path = os.path.join(input_folder, file)
            excel_data = pd.ExcelFile(file_path)

            # Iterate through all sheets in the Excel file
            for sheet_name in excel_data.sheet_names:
                df = excel_data.parse(sheet_name)

                # Normalize column names: strip spaces, convert to lowercase, and replace special characters
                df.columns = (
                    df.columns.str.strip()
                    .str.lower()
                    .str.replace(" ", "")
                    .str.replace(r"[^\w\s]", "", regex=True)
                )

                # Filter rows dynamically based on filter_columns
                filter_column = next((col for col in filter_columns if col in df.columns), None)
                if filter_column:
                    df = df[df[filter_column].str.contains("RECUPERA", case=False, na=False)]
                else:
                    continue

                # Handle "cuenta" columns dynamically
                cuenta_column = next((col for col in cuenta_columns if col in df.columns), None)
                if cuenta_column:
                    df["cuenta"] = (df[cuenta_column]
                                                .astype(str)
                                                .str.strip()
                                                .str.replace(r"\.0$", "", regex=True)
                                                .str.replace(".", "", regex=False) 
                                            )

                    # Count occurrences of each "cuenta" and add a "servicios" column
                    if servicios_column in df.columns:
                        df["servicios"] = df[servicios_column]
                        df["tipo"] = "fija"
                    else:
                        df["servicios"] = df.groupby("cuenta")["cuenta"].transform("count")
                        df["tipo"] = "movil"
                else:
                    continue

                # Add "pago" column dynamically
                payment_column = next((col for col in pago_column if col in df.columns), None)
                if payment_column:
                    df["pago"] = df[payment_column]
                else:
                    df["pago"] = None  # Default value if no pago column is found

                # Add "fecha" column dynamically
                date_column = next((col for col in datepayment_column if col in df.columns), None)
                if date_column:
                    df["fecha"] = df[date_column]
                else:
                    df["fecha"] = None  # Default value if no fecha column is found

                # Add "concepto" column dynamically
                concepto_column = next((col for col in concept_column if col in df.columns), None)
                if concepto_column:
                    df["concepto"] = df[concepto_column]
                else:
                    df["concepto"] = None  # Default value if no concepto column is found

                # Add "estado" column dynamically
                estado_column = next((col for col in estado_columns if col in df.columns), None)
                if estado_column:
                    df["estado"] = df[estado_column]
                else:
                    df["estado"] = df["concepto"]

                # Create a new column "llave" based on "estado" and "tipo"
                df["llave"] = (df["estado"].astype(str) + df["tipo"].astype(str)).str.upper()

                # Update "estado" based on "llave"
                df["estado"] = df["llave"].apply(
                    lambda x: "NO RECUPERADA" if x == "NOFIJA" else
                              "RECUPERADA" if x == "SIFIJA" else
                              "NO GESTIONAR" if x == "NOMOVIL" else
                              
                              "GESTIONAR" if x == "AJUSTEMOVIL" else
                              "GESTIONAR" if x == "PENDIENTEMOVIL" else
                              "NO GESTIONAR" if x == "PAGO TOTAL NO RXMOVIL" else
                              "NO GESTIONAR" if x == "PAGO TOTAL NO_RXMOVIL" else
                              "NO GESTIONAR" if x == "PAGO TOTAL SI RXMOVIL" else
                              "NO GESTIONAR" if x == "PAGO TOTAL SI_RXMOVIL" else
                              
                              "GESTIONAR" if x == "SIMOVIL" else
                              "GESTION RECAUDO" if "GESTION RECAUDO" in x else
                              "GESTION RECAUDO" if "GESTION_RECAUDO" in x else
                              "GESTION RECAUDO" if "GESTIÓN_RECAUDO" in x else
                              "GESTION RECAUDO" if "GESTIÓN RECAUDO" in x else
                              x
                )
                
                df["archivo"] = file  # 'file' es el nombre del archivo en tu ciclo
                
                # Select relevant columns if they exist
                df.columns = df.columns.str.upper()
                required_columns = ["CUENTA", "SERVICIOS", "ESTADO"]
                required_columns_detail = ["CUENTA", "SERVICIOS", "ESTADO", "PAGO", "FECHA", "CONCEPTO", "ARCHIVO"]
                
                df_detail = df[[col for col in required_columns_detail if col in df.columns]]
                df_detail["PAGO"] = df_detail["PAGO"].fillna(0).astype(int)
                df = df[[col for col in required_columns if col in df.columns]]

                # Remove duplicates
                df = df.drop_duplicates()
                df_detail = df_detail.drop_duplicates()

                # Append to the list of all data
                if len(df)>0:
                    all_data.append(df)
                    all_data_detail.append(df_detail)
                else:
                    unprocessed_files.append(file)
        else:
             unprocessed_files.append(file)
               
    # Concatenate all data and save to CSV
    if all_data:
        folder = f"---- Bases para CARGUE ----"
        output_directory = os.path.join(output_file, folder)
        os.makedirs(output_directory, exist_ok=True)  # Ensure the directory exists

        output_file_ranking = os.path.join(output_directory, f"Cargue Rankings {datetime.now().strftime('%Y-%m-%d')}.csv")

        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(output_file_ranking, sep=";", index=False, encoding="utf-8")        
        folder = f"---- Bases para CRUCE ----"
        output_directory = os.path.join(output_file, folder)
        os.makedirs(output_directory, exist_ok=True)  # Ensure the directory exists

        output_file_ranking = os.path.join(output_directory, f"Detalle Rankings {datetime.now().strftime('%Y-%m-%d')}.csv")

        final_df = pd.concat(all_data_detail, ignore_index=True)
        final_df.to_csv(output_file_ranking, sep=";", index=False, encoding="utf-8")
        print(f"Processing complete. Results saved to: {output_file_ranking}")
    else:
        print("No data found to process.")
        
    # Log unprocessed files
    if unprocessed_files:
        unprocessed_files_str = "\n".join(unprocessed_files)
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Archivos no procesados")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Warning)
        Mbox_In_Process.setText("Proceso finalizado. Los siguientes archivos no se pudieron procesar:\n\n" + unprocessed_files_str)
        Mbox_In_Process.setStandardButtons(QMessageBox.StandardButton.Ok)
        Mbox_In_Process.exec()