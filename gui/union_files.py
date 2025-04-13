import os
from web.pyspark import get_spark_session
from pyspark.sql import DataFrame
from datetime import datetime

spark = get_spark_session()

def read_file_with_delimiter(file_path: str) -> DataFrame:
    with open(file_path, 'r') as f:
        first_line = f.readline()
        if ',' in first_line:
            delimiter = ','
        elif ';' in first_line:
            delimiter = ';'
        elif '\t' in first_line:
            delimiter = '\t'
        else:
            raise ValueError(f"No delimiter detected in file: {file_path}")
    df = spark.read.csv(file_path, sep=delimiter, header=True, inferSchema=True)
    return df, delimiter

def merge_files(input_directory: str, output_directory: str):
    file_paths = [os.path.join(input_directory, f) for f in os.listdir(input_directory) if f.endswith('.csv') or f.endswith('.txt')]
    merged_df = None
    found_delimiter = None

    for file_path in file_paths:
        try:
            df, delimiter = read_file_with_delimiter(file_path)
            found_delimiter = delimiter
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            continue
        
        if merged_df is None:
            merged_df = df
        else:
            merged_df = merged_df.unionByName(df, allowMissingColumns=True)

    if merged_df is not None:
        now = datetime.now()
        file_date = now.strftime("%d-%m-%Y %H-%M")
        output_path = os.path.join(output_directory, f"Union Archivos Carpeta {file_date}")
        
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        merged_df = merged_df.dropDuplicates()
        
        merged_df.repartition(1).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_path)

        for root, dirs, files in os.walk(output_path):
            for file in files:
                if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                    os.remove(os.path.join(root, file))

        for i, file in enumerate(os.listdir(output_path), start=1):
            if file.endswith(".csv"):
                old_file_path = os.path.join(output_path, file)
                new_file_path = os.path.join(output_path, f'Union Archivos {file_date}.csv')
                os.rename(old_file_path, new_file_path)
        
        print(f"Files combined and saved in {output_path} with delimiter '{found_delimiter}'")
    else:
        print("No files were combined.")