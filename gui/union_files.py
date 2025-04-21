import os
from web.pyspark import get_spark_session
from pyspark.sql import DataFrame
from datetime import datetime
from web.save_files import save_to_csv

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
        
        merged_df = merged_df.dropDuplicates()
        
        delimiter = ";"
        Type_Proccess = "Union Archivos"
        
        Partitions = 1
        
        save_to_csv(merged_df, output_directory, Type_Proccess, Partitions, delimiter)
        
        print(f"Files combined and saved in {output_directory} with delimiter '{found_delimiter}'")
    else:
        print("No files were combined.")