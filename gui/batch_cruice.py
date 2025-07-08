import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, concat
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv

def process_data(directory, output_directory, selected_columns, return_matches, join_column_original, join_column_cruce, partitions):
    
    # Initialize Spark Session
    spark = get_spark_session()
    
    # Read the original and cruz data
    original_path = os.path.join(directory, "Batch", "*.csv")
    cruce_path = os.path.join(directory, "Demográficos", "*.csv")
    
    original_data = spark.read.csv(original_path, header=True, sep=";")
    original_data = original_data.filter(col("Filtro_BATCH") == "No efectivo")  # Filter out rows where 'Filtro_BATCH' is No efectivo
    original_data = original_data.withColumn("CRUCE", concat(col("numeromarcado"), col("identificacion")))
    original_data = original_data.drop("cuenta_promesa")
    
    cruce_data = spark.read.csv(cruce_path, header=True, sep=";")
    cruce_data = cruce_data.withColumn("LLAVE", concat(col("dato"), col("identificacion")))
    cruce_data = cruce_data.drop("identificacion")
    
    list_excluded = ["60", "90", "120", "150", "180", "210", "Castigo", "Provision", "Preprovision"]
    cruce_data = cruce_data.filter(~col("Marca").isin(list_excluded))
    
    print(f"Counter Original Data: {original_data.count()}")
    print(f"Counter Cruce Data: {cruce_data.count()}")
    
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
    result_data = result_data.withColumnRenamed("cuenta", "cuenta_promesa")
    result_data = result_data.select(*selected_columns)
    result_data = result_data.dropDuplicates()  # Remove duplicates if any

    # Save the result to the specified output directory
    output_directory = os.path.join(output_directory, "---- Bases para CARGUE ----")
    Type_File = "BD Batch Claro"
    delimiter = ";"
    
    save_to_csv(result_data, output_directory, Type_File, partitions, delimiter)

########################################
########################################
########################################

# Example Usage
def cruice_batch_campaign_claro(directory, output_directory, partitions):
    
    print("Starting Cruice Batch Campaign Claro...")
    
    selected_columns = [
        "gestion", "usuario", "fechagestion", "accion",
        "perfil", "numeromarcado", "identificacion", "cuenta_promesa", "fecha_promesa",
        "valor_promesa", "numero_cuotas"
    ]  # Replace with your desired columns

    return_matches = True  # Set to False if you want non-matching records
    join_column_original = "CRUCE"  # The column from Data Original to join on
    join_column_cruce = "LLAVE"  # The column from Data para Cruce to join on

    process_data(directory, output_directory, selected_columns, return_matches, join_column_original, join_column_cruce, partitions)