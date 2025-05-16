import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit
from web.pyspark import get_spark_session
from web.save_files import save_to_csv

def Function_Complete(Path, Outpath, Partitions):

    spark = get_spark_session()

    sqlContext = SQLContext(spark)
    now = datetime.now()

    df = spark.read.option("header", "false").csv(Path)
    df = spark.read.csv(Path, header= True, sep=",")
    df = df.select([col(c).cast(StringType()).alias(c) for c in df.columns])

    dfcolumns = ["Usuario","identificación","LLAMADAS","HORA","tiempo de inicio de sesión",
                 "ESPERA","ESPERE%","CHARLA","CHARLA TIEMPO%","DISPO","DISPOTIME%","Pausa",
                 "pausetime%","DEAD","TIEMPO MUERTO%","CLIENTE","CONNECTED","ALMUER","BANO",
                 "BKAM","BKPM","LAGGED","LOGIN","PAUACT","RETROA","REUNIO","VISIBLE","HIDDEN"]
    

    df_real_columns = df.columns
    for column_df in dfcolumns:
        if column_df in df_real_columns:
            pass
        else:
            df = df.withColumn(column_df, lit(""))

    df = df.withColumnRenamed('BKAM', 'BREAK')
    df = df.withColumnRenamed('BKPM', 'BREAKP')
    df = df.withColumnRenamed('PAUACT', 'PACTIV')
    df = df.withColumnRenamed('REUNIO', 'REU')
    df = df.withColumnRenamed('identificación', 'identificacion')
    df = df.withColumnRenamed('tiempo de inicio de sesión', 'tiempo de inicio de sesion')

    now = datetime.now()
    yesterday = now - timedelta(days=1)
    Yesterday_Date = yesterday.strftime("%d/%m/%Y")

    df = df.withColumn("Fecha", lit(f"{Yesterday_Date}"))

    Type_Proccess = f"TMO Conversion"
    delimiter = ";"
    
    save_to_csv(df, Outpath, Type_Proccess, Partitions, delimiter)

    return df