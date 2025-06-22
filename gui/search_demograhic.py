import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, concat
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv

def process_data(directory, output_directory, selected_columns, return_matches, join_column_cruce, partitions):
    
    # Initialize Spark Session
    spark = get_spark_session()
    
    original_data = spark.read.csv(directory, header=True, sep=";")
    join_column_original = original_data.columns[0]
    
    src_folder = r"D:\OneDrive - Recupera SAS\Data Claro\Claro_Data_Lake\Demográficos Unificados"
    cruce_path = copy_csv_files(src_folder, output_directory)
    unions_csv = read_and_union_csvs(cruce_path, spark)
    cruce_data = unions_csv  
    
    # Perform the join
    joined_data = original_data.join(cruce_data, original_data[join_column_original] == cruce_data[join_column_cruce], "left")
    
    result_data = joined_data
    
    # Filter based on the return_matches flag
    if return_matches:
        result_data = joined_data.filter(cruce_data[join_column_cruce].isNotNull())
        print("Return matches is True")
    else:
        result_data = joined_data.filter(cruce_data[join_column_cruce].isNull())
        print("Return matches is False")
        
    # Select the specified columns
    result_data = result_data.select(*selected_columns)
    result_data = result_data.dropDuplicates()
    
    # Save the result to the specified output directory
    Type_File = f"Demograficos Cruzados"
    delimiter = ";"
    save_to_csv(result_data, output_directory, Type_File, partitions, delimiter)
    
    print(f"Data processing complete. Results saved to: {output_directory}")

def copy_csv_files(src_folder, output_directory):
    # Nombre temporal raro para la carpeta destino
    temp_folder = os.path.join(output_directory, "temp_spark_30042000")
    os.makedirs(temp_folder, exist_ok=True)

    for root, _, files in os.walk(src_folder):
        for file in files:
            if file.lower().endswith('.csv'):
                src_file = os.path.join(root, file)
                dst_file = os.path.join(temp_folder, file)
                shutil.copy2(src_file, dst_file)
                
    return temp_folder

def read_and_union_csvs(cruce_path, spark):
    
    df = spark.read.option("header", True).option("delimiter", ";").csv(f"{cruce_path}/*.csv")
    df = df.dropDuplicates()
    return df

########################################
########################################
########################################

# Example Usage in Claro

def search_demographic_claro(filepath, output_directory, partitions, process_data_):
    
    selected_columns = ["identificacion", "dato"]  # Replace with your desired columns
    return_matches = True  # Set to False if you want non-matching records
    join_column_cruce = "identificacion"  # The column from Data para Cruce to join on

    process_data(filepath, output_directory, selected_columns, return_matches, join_column_cruce, partitions)