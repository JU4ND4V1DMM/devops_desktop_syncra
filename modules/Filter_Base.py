import os
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, length, split, to_date, substring
from pyspark.sql.functions import trim, format_number, expr, when, coalesce, datediff, current_date
from web.pyspark import get_spark_session

spark = get_spark_session()

sqlContext = SQLContext(spark)

def Function_Complete(Data_):
    
    Data_ = change_name_column(Data_, "nombrecompleto")

    #### Change value of DTO
    Data_ = Data_.withColumn(
        "descuento",
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A") | (col("descuento") == "Sin Descuento"), lit("0"))
        .otherwise(col("descuento")))
    
    #### Inclusion of brand (dont exist)
    Data_ = Data_.withColumn(
        "marca", 
        when(((col("marca_refinanciado") == "REFINANCIADO")) , lit("potencial a castigar"))
        .otherwise(col("marca")))
    
    #### Type of Transaction
    Data_ = Data_.withColumn(
        "tipo_pago", 
        when(((col("tipo_pago").isNull()) | (col("tipo_pago") == "")), lit("Sin Pago"))
        .otherwise(col("tipo_pago")))
    
    #### Change Brand for Apple
    Data_ = Data_.withColumn(
        "marca2",
        when(col("ciudad") == "ASIGNACION_MANUAL_APPLE", lit("Apple Manual"))
        .otherwise(col("marca")))

    #### Change value of RANKING STATUS
    Data_ = Data_.withColumn(
        "estado_ranking", 
        when(((col("estado_ranking").isNull()) | (col("estado_ranking") == "")), lit("NO APLICA FILTRO RANKING"))
        .otherwise(col("estado_ranking")))
    
    #### Filter for Colas and Ranking´s
    Data_ = Data_.filter((col("colas").isNull()) | (col("colas") == ""))
    filter_ranking = ["GESTION RECAUDO", "GESTIONAR", "NO RECUPERADA", "NO APLICA FILTRO RANKING"]
    Data_ = Data_.filter((col("estado_ranking").isin(filter_ranking)) | (col("estado_ranking").isNull()) | (col("estado_ranking") == ""))
    
    return Data_

def change_name_column (Data_, Column):

    Data_ = Data_.withColumn(Column, upper(col(Column)))

    character_list_N = ["\\ÃƒÂ‘", "\\Ã‚Â¦", "\\Ã‘", "Ñ", "ÃƒÂ‘", "Ã‚Â¦", "Ã‘"]
    
    for character in character_list_N:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, "NNNNN"))
    
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "NNNNN", "N"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "Ã‡", "A"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "ÃƒÂ", "I"))


    character_list = ["SR/SRA", "SR./SRA.", "SR/SRA.","SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)",\
                    "\\.",'#', '$', '/','<', '>', "\\*", "SEÑORES ","SEÑOR(A) ","SEÑOR ","SEÑORA ", "SENORES ",\
                    "SENOR(A) ","SENOR ","SENORA ", "¡", "!", "\\?" "¿", "_", "-", "}", "\\{", "\\+", "0 ", "1 ", "2 ", "3 ",\
                     "4 ", "5 ", "6 ", "7 ","8 ", "9 ", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "  "]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))
    
    Data_ = Data_.withColumn(Column, regexp_replace(Column, "[^A-Z& ]", ""))

    character_list = ["SEORES ","SEORA ","SEOR ","SEORA "]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))

    Data_ = Data_.withColumn(Column,regexp_replace(col(Column), r'^(A\s+| )+', ''))
        
    return Data_