import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, regexp_replace, lit
from web.pyspark import get_spark_session

def Function_Exclusions(Path, Outpath, Partitions):
    spark = get_spark_session()
    sqlContext = SQLContext(spark)

    df = spark.read.option("header", "false").csv(Path)
    df = spark.read.csv(Path, header= True, sep=";")
    df = df.select([col(c).cast(StringType()).alias(c) for c in df.columns])

    Management_Columns = ["cuenta", "perfil_historico", "ultimo_perfil", "mejorperfil"]
    df = df.select(Management_Columns)

    character_list = ["-"]
    for character in character_list:
        df = df.withColumn("cuenta", regexp_replace(col("cuenta"), character, ""))

    df = df.select("cuenta", "ultimo_perfil", "mejorperfil", "perfil_historico")
    df = df.dropDuplicates()
    
    df = df.filter(col('ultimo_perfil') == "Reclamacion")
    df = df.filter(col('mejorperfil') == "Reclamacion")
    df = df.filter(col('perfil_historico') == "Reclamacion")
    
    df = df.withColumn("FECHA", lit(datetime.now().strftime("%Y-%m-%d")))
    df = df.withColumnRenamed("cuenta", "CUENTA")

    df = df.select("CUENTA", "FECHA")


    Save_File_Form(df, Outpath, Partitions)
    
    return df

def Save_File_Form(df, Outpath, Partitions):
    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Outpath = f"{Outpath}Reclamaciones Reporte Gestion {Time_File}"

    df.repartition(Partitions).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(Outpath)

    for root, dirs, files in os.walk(Outpath):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))

    for i, file in enumerate(os.listdir(Outpath), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(Outpath, file)
            new_file_path = os.path.join(Outpath, f'No Gestion Perfiles_{now.strftime("%Y-%m-%d")} {i}.csv')
            os.rename(old_file_path, new_file_path)

    return df