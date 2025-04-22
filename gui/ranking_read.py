import os
import pandas as pd

def process_ranking_files(input_folder, output_file):
    """
    Processes Excel files in a folder, filters and transforms data, and saves the result to a CSV file.

    Args:
        input_folder (str): Path to the folder containing the Excel files.
        output_file (str): Path to save the resulting CSV file.
    """
    all_data = []

    # Iterate through all files in the input folder
    for file in os.listdir(input_folder):
        if file.endswith(".xlsx") or file.endswith(".xls"):
            file_path = os.path.join(input_folder, file)
            excel_data = pd.ExcelFile(file_path)

            # Iterate through all sheets in the Excel file
            for sheet_name in excel_data.sheet_names:
                df = excel_data.parse(sheet_name)

                # Standardize column names
                df.columns = df.columns.str.strip().str.lower()

                # Filter rows where "aliado", "casa", or "casa cobro" contains "recupera"
                if "aliado" in df.columns:
                    df = df[df["aliado"].str.contains("recupera", case=False, na=False)]
                elif "casa" in df.columns:
                    df = df[df["casa"].str.contains("recupera", case=False, na=False)]
                elif "casa cobro" in df.columns:
                    df = df[df["casa cobro"].str.contains("recupera", case=False, na=False)]

                # Handle "raiz" or "cuenta" columns
                if "raiz" in df.columns:
                    df["cuenta"] = df["raiz"].astype(str).str.replace(".", "", regex=False)
                elif "cuenta" in df.columns:
                    df["cuenta"] = df["cuenta"].astype(str).str.replace(".", "", regex=False)

                # Count occurrences of each "cuenta" and add a "servicios" column
                if "n servicios" in df.columns:
                    df["servicios"] = df["n servicios"]
                else:
                    df["servicios"] = df.groupby("cuenta")["cuenta"].transform("count")

                # Add "estado" column based on "concepto" or "recuperada"
                if "concepto" in df.columns:
                    df["estado"] = df["concepto"]
                elif "recuperada" in df.columns:
                    df["estado"] = df["recuperada"]

                # Select relevant columns
                df = df[["cuenta", "servicios", "estado"]]

                # Remove duplicates
                df = df.drop_duplicates()

                # Append to the list of all data
                all_data.append(df)

    # Concatenate all data and save to CSV
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(output_file, sep=";", index=False, encoding="utf-8")
        print(f"Processing complete. Results saved to: {output_file}")
    else:
        print("No valid data found in the provided folder.")

# Example usage
input_folder = r"C:\Users\C.operativo\Downloads\RANKING"
output_file = r"C:\Users\C.operativo\Downloads\Lectura Ranking.csv"
process_ranking_files(input_folder, output_file)