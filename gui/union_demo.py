from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number
from datetime import datetime
import os

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

def Union_Files_Demo(Path, Outpath, partitions):
    
    files = [os.path.join(Path, file) for file in os.listdir(Path) if file.endswith(".csv")]

    Data_Frame = spark.read.option("header", "true").option("sep", ";").csv(files)

    Data_Frame = Data_Frame.withColumn("Key", concat(col("dato"), col("cuenta")))
    Data_Frame = Data_Frame.dropDuplicates(["key"])
    Data_Frame = Data_Frame.orderBy(["dato"], ascending = True)
    
    if "Marca" not in Data_Frame.columns:
        Data_Frame = Data_Frame.withColumn("Marca", lit("Sin asignar marca"))
        
    Data_Frame = Data_Frame.select("identificacion","cuenta","ciudad","depto","dato","tipodato","Marca")
    #Data_Frame = Data_Frame.filter(col("tipodato") == "telefono")
    #Data_Frame = Data_Frame.filter(col("tipodato") == "email")
    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Date_File = now.strftime("%Y%m%d")
    Type_File = "Demograficos_Consolidados_"

    output_path = f'{Outpath}/{Type_File}{Time_File}'
    Data_Frame.repartition(int(partitions)).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_path)

    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))

    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'Union Demo Reparto {Date_File} {i}.csv')
            os.rename(old_file_path, new_file_path)
    
    return Data_Frame