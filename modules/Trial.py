import string
from functools import reduce
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, concat_ws, array
from pyspark.sql.functions import expr, when, row_number, collect_list, length, size, split, lower
import os
from web.pyspark import get_spark_session

spark = get_spark_session()

sqlContext = SQLContext(spark)

def Filter_EMAIL(path, outpath, Partitions):

    Data_Root = spark.read.csv(path, header= True, sep=";")
    Data_Root = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    columns_to_list = [f"{i}_" for i in range(1, 53)]
    Data_Root = Data_Root.select(columns_to_list)
    
    potencial = (col("5_") == "Y") & (col("3_") == "BSCS")
    churn = (col("5_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA"))
    provision = (col("5_") == "Y") & (col("3_") == "ASCARD")
    prepotencial = (col("6_") == "Y") & (col("3_") == "BSCS")
    prechurn = (col("6_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA"))
    preprovision = (col("6_") == "Y") & (col("3_") == "ASCARD")
    castigo = col("7_") == "Y"
    potencial_a_castigar = (col("5_") == "N") & (col("6_") == "N") & (col("7_") == "N") & (col("43_") == "Y")
    marcas = col("13_")

    Data_Root = Data_Root.withColumn("Marca", when(potencial, "Potencial")\
                                        .when(churn, "Churn")\
                                        .when(provision, "Provision")\
                                        .when(prepotencial, "Prepotencial")\
                                        .when(prechurn, "Prechurn")\
                                        .when(preprovision, "Preprovision")\
                                        .when(castigo, "Castigo")\
                                        .when(potencial_a_castigar, "Potencial a Castigar")\
                                        .otherwise(marcas))
    
    moras_numericas = (col("Marca") == "120") | (col("Marca") == "150") | (col("Marca") == "180")
    prepotencial_especial = (col("Marca") == "Prepotencial") & (col("3_") == "BSCS") & ((col("12_") == "PrePotencial Convergente Masivo_2") | (col("12_") == "PrePotencial Convergente Pyme_2"))

    Data_Root = Data_Root.withColumn("Marca", when(moras_numericas, "120 - 180")\
                                        .when(prepotencial_especial, "Prepotencial Especial")\
                                        .otherwise(col("Marca")))
    
    Data_Root = Data_Root.withColumn("Rango", \
            when((col("9_") <= 20000), lit("1 Menos a 20 mil")) \
                .when((col("9_") <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((col("9_") <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((col("9_") <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((col("9_") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((col("9_") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((col("9_") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((col("9_") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
                .when((col("9_") <= 2000000), lit("9 Entre 1 a 2 millones")) \
                .otherwise(lit("9.1 Mayor a 2 millones")))
    
    character_list = list(string.ascii_uppercase)
    Punctuation_List = ["\\*"]
    character_list = character_list + Punctuation_List
    
    Data_Root = Data_Root.withColumn("1_", upper(col("1_")))

    for character in character_list:
        Data_Root = Data_Root.withColumn("1_", regexp_replace(col("1_"), character, ""))
    
    Data_Root = Data_Root.withColumn("Monto", col("9_").cast("double"))
    Data_Root = Data_Root.withColumn("Monto", regexp_replace("Monto", "\\.", ","))
    Data_Root = Data_Root.withColumn("2_", concat(col("2_"), lit("-")))
    Data_Root = Data_Root.withColumnRenamed("1_", "Documento")
    Data_Root = Data_Root.withColumnRenamed("2_", "Cuenta")
    
    Data_Root = Data_Root.withColumnRenamed("3_", "CRM Origen")

    Data_Root = Data_Root.select("Marca", "CRM Origen", "Rango", "Documento", "Cuenta", "Monto", \
                                 "47_", "51_", "48_", "49_", "50_")
    
    #Data_Root = Data_Root.filter(col("Marca") != "Castigo")
    
    Data_Root = Email_Data(Data_Root)
    Data_Root = Filter_Email_Data(Data_Root)

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"Validacion_EMAILS_"

    output_path = f'{outpath}{Type_File}{Time_File}'
    Partitions = int(Partitions)
    Data_Root.repartition(Partitions).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(output_path)

    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))
    
    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'Emails Consolidados Part - {i}.csv')
            os.rename(old_file_path, new_file_path)
            
    return Data_Root

def Email_Data(Data_):

    columns_to_stack = ["47_", "48_", "49_", "50_", "51_"] #EMAIL and Phone X (1-4)

    all_columns_to_stack = columns_to_stack

    columns_to_drop_contact = all_columns_to_stack
    stacked_contact_data_frame = Data_.select("*", *all_columns_to_stack)

    stacked_contact_data_frame = stacked_contact_data_frame.select(
        "*",
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Email")
    )

    Data_ = stacked_contact_data_frame.drop(*columns_to_drop_contact)

    return Data_

def Filter_Email_Data(Data_):

    Data_ = Data_.withColumn(
        "Tipologia",
        when(length(split(col("Email"), "@")[0]) < 6, "ERRADO")  # Check the length of the part before the '@'
        .when(size(split(col("Email"), "@")) == 2, "CORREO UNICO")
        .when(size(split(col("Email"), "@")) >= 3, "CORREOS SIN DELIMITAR")
        .otherwise("INVALIDO")
    )

    Data_ = Data_.filter(col("Email").contains("@"))

    list_email_replace = [
        "notiene", "nousa", "nobrinda", "000@00.com.co", "nolorecuerda", "notengo", "noposee",
        "nosirve", "notien", "noutili", "nomanej", "nolegust", "nohay", "nocorreo", "noindic",
        "nohay", "@gamil", "@claro" "pendienteconfirmar", "sincorreo", "pendienteporcrearclaro", "correo.claro",
        "crearclaro", ":", "|", "porcrear", "+", "#", "@xxx", "-"
    ]

    email_set = set(list_email_replace)

    Data_ = Data_.withColumn("Email", lower(col("Email")))

    contains_any_expr = reduce(
        lambda acc, word: acc | col("Email").contains(word),
        email_set,
        lit(False)
    )

    # Actualizar la columna Tipologia
    Data_ = Data_.withColumn(
        "Tipologia",
        when(contains_any_expr, "ERRADO")
        .otherwise(col("Tipologia"))
        )
    
    Data_ = Data_.filter(col("Email") != "@")
    Data_ = Data_.filter(col("Tipologia") != "ERRADO")
    Data_ = Data_.filter(col("Tipologia") != "INVALIDO")

    character_list = ['#', '$', '/','<', '>', "\\*", "¡", "!", "\\?" "¿", "-", "}", "\\{", "\\+", " "]

    for character in character_list:
        Data_ = Data_.withColumn("Email", regexp_replace(col("Email"), character, ""))

    return Data_

File_Name = "BaseGeneral"
path = f'C:/Users/c.operativo/Downloads/Dev_/Union/{File_Name}.csv'
output_directory = 'C:/Users/c.operativo/Downloads/'
Partitions = 1

Filter_EMAIL(path, output_directory, Partitions)
