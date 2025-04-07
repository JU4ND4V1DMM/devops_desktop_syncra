from datetime import datetime
import os
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, when, expr, concat, lit, row_number, collect_list, concat_ws, trim
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

def function_complete_IVR(input_folder, output_folder, Partitions, Widget_Process, Date, Search_Data, Channel):

    files = []

    for root, _, file_names in os.walk(input_folder):
        for file_name in file_names:
            if file_name.endswith('.csv') or file_name.endswith('.txt'):
                print(f"{_}")
                files.append(os.path.join(root, file_name))

    consolidated_df = None
    for file in files:
        if file.endswith('.csv'):
            df = spark.read.csv(file, header=True, inferSchema=True)
        elif file.endswith('.txt'):
            df = spark.read.option("delimiter", "\t").csv(file, header=True, inferSchema=True)

        if consolidated_df is None:
            consolidated_df = df
        else:
            consolidated_df = consolidated_df.unionByName(df, allowMissingColumns=True)

    if consolidated_df is not None:
        
        selected_columns = [
            "last_local_call_time", "status", "vendor_lead_code", 
            "source_id", "list_id", "phone_number", "title", "first_name", "last_name", "called_count", "security_phrase"
        ]

        consolidated_df = consolidated_df.select(*selected_columns)

        consolidated_df = consolidated_df.withColumn("last_local_call_time", regexp_replace(col("last_local_call_time"), "T", " "))
        consolidated_df = consolidated_df.withColumn("last_local_call_time", regexp_replace(col("last_local_call_time"), ".000-05:00", ""))
        consolidated_df = consolidated_df.withColumn("last_local_call_time", regexp_replace(col("last_local_call_time"), "2008-01-01 00:00:00", "2000-04-30 00:00:00"))

        consolidated_df = consolidated_df.withColumn(
            "status",
            when(col("status") == "AB", "Buzon de voz lleno")
            .when(col("status") == "PM", "Se inicia mensaje y cuelga")                          #Efectivo
            .when(col("status") == "AA", "Maquina contestadora")
            .when(col("status") == "ADC", "Numero fuera de servicio")
            .when(col("status") == "DROP", "No contesta")
            .when(col("status") == "NA", "No contesta")
            .when(col("status") == "NEW", "Contacto sin marcar")
            .when(col("status") == "PDROP", "Error desde el operador")
            .when(col("status") == "PU", "Cuelga llamada")                                      
            .when(col("status") == "SVYEXT", "Llamada transferida al agente")                   #Efectivo
            .when(col("status") == "XFER", "Se reprodujo mensaje completo")                     #Efectivo
            .when(col("status") == "SVYHU", "Cuelga durante una transferencia")                 #Efectivo
            .otherwise(col("status"))
        )
        
        consolidated_df = consolidated_df.withColumn(
            "status",
            when((col("status") == "Contacto sin marcar") & (col("called_count") > 0), "Contacto sin remarcar")
            .otherwise(col("status"))
        )

        consolidated_df = consolidated_df.withColumn(
            "title",
            when(col("title") == "ASCA", "Equipo")
            .when(col("title") == "RR", "Hogar")
            .when(col("title") == "SGA", "Negocios")
            .when(col("title") == "BSCS", "Postpago")
            .otherwise("error")
        )
        
        consolidated_df = consolidated_df.withColumn(
            "Campana",
            when(col("list_id") == "4251", "GMAC")
            .when(col("list_id") == "4181", "PASH")
            .when((col("list_id") == "5461") | (col("list_id") == "5491"), "PUNTORED")
            .otherwise("CLARO")
        )
        
        consolidated_df = consolidated_df.withColumn(
            "Filtro Titularidad",
            when(col("security_phrase") == "1", "Confirma Titularidad")
            .when(col("security_phrase") == "2", "Tercero")
            .when((col("security_phrase").isNull()) & (col("list_id") == "5481"), "Sin respuesta")
            .when(col("security_phrase").isNull(), "No Aplica")
            .otherwise("Respuesta Invalida")
        )

        consolidated_df = consolidated_df.withColumn(
            "first_name",
            when((col("first_name").isNull() | (trim(col("first_name")) == ""))\
                  & (col("title").isNull() | (trim(col("title")) == ""))\
                      & (col("last_name").isNull() | (trim(col("last_name")) == "")), "GMAC")
            .otherwise(col("first_name")))

        consolidated_df = consolidated_df.withColumn(
            "first_name",
            when(col("first_name").isNull() | (trim(col("first_name")) == ""), "0")
            .otherwise(col("first_name"))
        )

        Transaccional = ((col("list_id") == "4000") | (col("list_id") == "4021") | (col("list_id") == "4041") | (col("list_id") == "4061")\
                         | (col("list_id") == "4081") | (col("list_id") == "4101") | (col("list_id") == "4121")\
                            | (col("list_id") == "4141") | (col("list_id") == "4161") | (col("list_id") == "4181")\
                                | (col("list_id") == "4186") | (col("list_id") == "4191") | (col("list_id") == "4196")\
                                    | (col("list_id") == "4251") | (col("list_id") == "5421") | (col("list_id") == "5441")\
                                        | (col("list_id") == "5500") | (col("list_id") == "5510") | (col("list_id") == "5520")\
                                            | (col("list_id") == "5401") | (col("list_id") == "5461") | (col("list_id") == "5481"))
    
        consolidated_df = consolidated_df.withColumn("Canal", when(Transaccional, "Transacccional")\
                                        .otherwise(lit("IVR Intercom")))

        if Date == "All Dates":
            pass
        else:
            consolidated_df = consolidated_df.filter(col("last_local_call_time") == Date)

        if len(Search_Data) >= 2:
            Search_List = Search_Data.split(",")  # Convierte en lista
            print(Search_List)
            print(type(Search_List))

            # Crea una condición que evalúa todas las columnas del DataFrame
            condition = None
            for column in consolidated_df.columns:
                column_condition = col(column).rlike("|".join(Search_List))  # Busca cualquier coincidencia
                condition = column_condition if condition is None else condition | column_condition

            # Aplica el filtro al DataFrame
            consolidated_df = consolidated_df.filter(condition)

        if Channel == "Todo":
            pass
        elif Channel == "Transaccionales":
            consolidated_df = consolidated_df.filter(col("Canal") == "Transacccional")
        else:
            consolidated_df = consolidated_df.filter(col("Canal") == "IVR Intercom")

        consolidated_df = consolidated_df.withColumn(
            "phone_type",
            when((col("phone_number").cast("long") >= 300000000) & (col("phone_number").cast("long") <= 3599999999), "Celular")
            .otherwise("Fijo")
        )

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")
        Time_File_csv = now.strftime("%Y%m%d %H%M")
        output_folder_ = f"{output_folder}/Consolidado_IVR_{Time_File}"

        consolidated_df = Function_ADD(consolidated_df)
        
        Partitions = int(Partitions)
        consolidated_df.repartition(Partitions).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_folder_)
            
        for root, dirs, files in os.walk(output_folder_):
            for file in files:
                if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                    os.remove(os.path.join(root, file))
        
        for i, file in enumerate(os.listdir(output_folder_), start=1):
            if file.endswith(".csv"):
                old_file_path = os.path.join(output_folder_, file)
                new_file_path = os.path.join(output_folder_, f'Resultado IVR {Time_File_csv} Part- {i}.csv')
                os.rename(old_file_path, new_file_path)

    else:
        pass
    return consolidated_df

def Function_ADD(RDD):

    filter_origins = ["Postpago", "Equipo", "Hogar", "Negocios", "Portofino", "Atmos", "Seven-Seven", "Patprimo"]
    #RDD = RDD.filter(col("title").isin(filter_origins))

    RDD = RDD.withColumn(
            "effectiveness",
            when((col("status") == "Se inicia mensaje y cuelga") | (col("status") == "Llamada transferida al agente")\
                 | (col("status") == "Se reprodujo mensaje completo") | (col("status") == "Cuelga durante una transferencia"), "Efectivo")

            .otherwise("No efectivo")
        )

    RDD = RDD.withColumnRenamed("last_local_call_time", "Ultima Marcacion")
    RDD = RDD.withColumnRenamed("status", "Estado de Llamada")
    RDD = RDD.withColumnRenamed("vendor_lead_code", "Documento")
    RDD = RDD.withColumnRenamed("source_id", "Cuenta")
    RDD = RDD.withColumnRenamed("list_id", "Lista - Canal")
    RDD = RDD.withColumnRenamed("phone_number", "Linea")
    RDD = RDD.withColumnRenamed("title", "Origen")
    RDD = RDD.withColumnRenamed("first_name", "Marca")
    RDD = RDD.withColumnRenamed("phone_type", "Tipo de Linea")
    RDD = RDD.withColumnRenamed("effectiveness", "Efectividad")
    RDD = RDD.withColumnRenamed("last_name", "FLP")
    RDD = RDD.withColumnRenamed("called_count", "Marcaciones")
    RDD = RDD.withColumnRenamed("security_phrase", "Respuesta Usuario")

    RDD = RDD.withColumn(
            "Origen",
            when((col("Campana") == "PASH"), col("Cuenta"))
            .otherwise(col("Origen"))
        )
    RDD = RDD.withColumn(
            "Cuenta",
            when((col("Campana") == "PASH"), concat(col("Documento"), lit("_"), col("Marca"), lit("_"), col("Cuenta")))
            .when(col("Campana") == "CLARO", concat(col("Cuenta"), lit("-")))
            .otherwise(col("Cuenta"))
        )
    
    RDD = RDD.withColumn("Cuenta", regexp_replace(col("Cuenta"),"--", "-"))
    
    Listcolumns = ["Ultima Marcacion", "Estado de Llamada", "Documento", "Cuenta", "Lista - Canal", "Linea", 
                   "Origen", "Marca", "FLP", "Marcaciones", "Campana", "Canal", "Tipo de Linea", 
                   "Efectividad", "Respuesta Usuario", "Filtro Titularidad"]
    
    RDD = RDD.select(Listcolumns)
    RDD = RDD.select(Listcolumns)
    
    return RDD