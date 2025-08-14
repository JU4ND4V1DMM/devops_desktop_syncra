import os
from PyQt6.QtWidgets import QMessageBox
from skills import list_city_mins
from functools import reduce
import string
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, expr, length, size, split, lower, when
from web.pyspark_session import get_spark_session
from web.save_files import save_to_0csv, save_to_csv
from web.temp_parquet import save_temp_log

spark = get_spark_session()

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, partitions):

    Data_Frame = First_Changes_DataFrame(path)

    Data_Email = Email_Data(Data_Frame)
    Type = "Emails"
    Data_Email = Demographic_Proccess_Emails(Data_Email, output_directory, partitions)
    Data_Email = save_temp_log(Data_Email, spark)
    Save_Data_Frame(Data_Email, output_directory, partitions, Type)

    Data_Frame = Phone_Data(Data_Frame)
    Type = "Mins"
    Data_NO = Demographic_Proccess_Mins(Data_Frame, output_directory, partitions, "NO_valido")
    Data_NO = save_temp_log(Data_NO, spark)
    Data_AC = Demographic_Proccess_Mins(Data_Frame, output_directory, partitions, "valido")

    Data_Frame = Data_AC.union(Data_NO)

    Save_Data_Frame(Data_Frame, output_directory, partitions, Type)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True,sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    potencial = (col("5_") == "Y") & (col("3_") == "BSCS")
    churn = (col("5_") == "Y") & (col("3_") == "RR")
    provision = (col("5_") == "Y") & (col("3_") == "ASCARD")
    prepotencial = (col("6_") == "Y") & (col("3_") == "BSCS")
    prepotencial_especial = (col("6_") == "Y") & (col("3_") == "BSCS") & (col("12_") == "PrePotencial Convergente Masivo_2")
    prechurn = (col("6_") == "Y") & (col("3_") == "RR")
    preprovision = (col("6_") == "Y") & (col("3_") == "ASCARD")
    castigo = col("7_") == "Y"
    potencial_a_castigar = (col("5_") == "N") & (col("6_") == "N") & (col("7_") == "N") & (col("42_") == "Y")
    marcas = col("13_")

    DF = DF.withColumn("Marca", when(potencial, "Potencial")\
            .when(churn, "Churn")\
            .when(provision, "Provision")\
            .when(prepotencial, "Prepotencial")\
            .when(prepotencial_especial, "Prepotencial Especial")\
            .when(prechurn, "Prechurn")\
            .when(preprovision, "Preprovision")\
            .when(castigo, "Castigo")\
            .when(potencial_a_castigar, "Potencial a Castigar")\
            .otherwise(marcas))

    return DF

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("1_", "identificacion")
    Data_Frame = Data_Frame.withColumnRenamed("2_", "cuenta")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions, Type):

    now = datetime.now()
    Time_File_File = now.strftime("%Y%m%d")
    Type_File = "---- Bases para CARGUE ----"
    Directory_to_Save = os.path.join(Directory_to_Save, Type_File)

    Name_File = f"Demograficos {Type}"
    delimiter = ";"
    save_to_csv(Data_Frame, Directory_to_Save, Name_File, partitions, delimiter)
    
    Data_Frame = Data_Frame.filter(col("Marca") != "Castigo")
    Name_File = f"Demograficos SIN CASTIGO {Type}"
    save_to_csv(Data_Frame, Directory_to_Save, Name_File, partitions, delimiter)

    return Data_Frame

### Dinamización de columnas de contacto
def Phone_Data(Data_):

    #Data_ = Data_.filter(col("7_") == "N")  ###Filter BRAND ###################
    #Data_ = Data_.filter((col("13_") == "30") & (col("13_") == "N"))  ###Filter BRAND ###################

    list_replace = ["VDK", "VD"]
    
    #Data_ = Data_.filter((col("28_").like("%VDK%")) | (col("28_").like("%VD%")))

    for letters in list_replace:
        Data_ = Data_.withColumn("28_", regexp_replace(col("28_"), \
            letters, "9999999999"))
          
          ##  Replace for exclusion of mins

    columns_to_stack_min = ["28_"] #MIN
    columns_to_stack_mobile = ["46_", "47_", "48_", "49_", "50_"] #Telefono X and EMAIL
    columns_to_stack_activelines = ["51_", "52_", "53_", "54_", "55_"] #ActiveLines

    all_columns_to_stack = columns_to_stack_mobile + columns_to_stack_activelines + columns_to_stack_min

    columns_to_drop_contact = all_columns_to_stack
    stacked_contact_data_frame = Data_.select("*", *all_columns_to_stack)

    stacked_contact_data_frame = stacked_contact_data_frame.select(
        "*",
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as dato")
    )

    Data_ = stacked_contact_data_frame.drop(*columns_to_drop_contact)

    return Data_

def Email_Data(Data_):

    columns_to_stack = ["46_", "47_", "48_", "49_", "50_"] #EMAIL and Phone X (1-4)

    all_columns_to_stack = columns_to_stack

    columns_to_drop_contact = all_columns_to_stack
    stacked_contact_data_frame = Data_.select("*", *all_columns_to_stack)

    stacked_contact_data_frame = stacked_contact_data_frame.select(
        "*",
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as dato")
    )

    Data_ = stacked_contact_data_frame.drop(*columns_to_drop_contact)

    return Data_

def Remove_Dots(dataframe, column):

    dataframe = dataframe.withColumn(column, regexp_replace(col(column), "[.-]", ""))
    
    return dataframe

### Proceso de filtrado de líneas
def Demographic_Proccess_Mins(Data_, Directory_to_Save, partitions, TypeProccess):

    Data_ = Data_.withColumn("ciudad", lit("BOGOTA"))
    Data_ = Data_.withColumn("depto", lit("BOGOTA"))
    Data_ = Data_.withColumn("tipodato", lit("telefono"))
    
    Data_ = Data_.select("1_", "2_", "22_", "ciudad", "depto", "dato", "tipodato", "Marca")
    
    Data_ = Data_.withColumn("1_", regexp_replace("1_", "[^0-9]", ""))
    Data_ = Data_.filter(col("1_").cast("int").isNotNull())
    Data_ = Data_.withColumn("1_", col("1_").cast("int"))
    
    character_list = list(string.ascii_uppercase)
    Punctuation_List = ["\\*", "57- ", "57-", "57 - ", "-", " "]
    character_list = character_list + Punctuation_List
    
    Data_ = Data_.withColumn("1_", upper(col("1_")))

    for character in character_list:
        Data_ = Data_.withColumn("1_", regexp_replace(col("1_"), character, ""))
        Data_ = Data_.withColumn("2_", regexp_replace(col("2_"), character, ""))
        Data_ = Data_.withColumn("dato", regexp_replace(col("dato"), character, ""))
    
    Data_ = Function_Filter(Data_, TypeProccess)
    Data_ = Data_.withColumn("cruice", concat(col("2_"), col("dato")))
    Data_ = Data_.dropDuplicates(["cruice"])

    Data_ = Remove_Dots(Data_, "1_")
    Data_ = Remove_Dots(Data_, "2_")

    Data_ = Renamed_Column(Data_)

    Data_ = Data_.select("identificacion", "cuenta", "ciudad", "depto", "dato", "tipodato", "Marca")

    return Data_

def Function_Filter(RDD, TypeProccess):

    if TypeProccess == "valido":

        Data_C = RDD.filter(col("dato") >= 3000000001)
        Data_C = Data_C.filter(col("dato") <= 3599999998)
        Data_F = RDD.filter(col("dato") >= 6010000001)
        Data_F = Data_F.filter(col("dato") <= 6089999998)
    
        RDD = Data_C.union(Data_F)
    
    else:

        RDD = list_city_mins.lines_inactives_df(RDD)

    return RDD

def Demographic_Proccess_Emails(Data_, Directory_to_Save, partitions):

    Data_ = Data_.withColumn("ciudad", lit("BOGOTA"))
    Data_ = Data_.withColumn("depto", lit("BOGOTA"))
    Data_ = Data_.withColumn("tipodato", lit("email"))
    
    Data_ = Data_.select("1_", "2_", "22_", "ciudad", "depto", "dato", "tipodato", "Marca")

    Data_ = Data_.withColumn("1_", regexp_replace("1_", "[^0-9]", ""))
    Data_ = Data_.filter(col("1_").cast("int").isNotNull())
    Data_ = Data_.withColumn("1_", col("1_").cast("int"))
    
    character_list = list(string.ascii_uppercase)
    Punctuation_List = ["\\*"]
    character_list = character_list + Punctuation_List
    
    Data_ = Data_.withColumn("1_", upper(col("1_")))

    for character in character_list:
        Data_ = Data_.withColumn("1_", regexp_replace(col("1_"), character, ""))
        Data_ = Data_.withColumn("2_", regexp_replace(col("2_"), character, ""))
    
    Data_ = Function_Filter_Email(Data_)
    Data_ = Data_.withColumn("cruice", concat(col("2_"), col("dato")))
    Data_ = Data_.dropDuplicates(["cruice"])

    Data_ = Remove_Dots(Data_, "1_")
    Data_ = Remove_Dots(Data_, "2_")

    Data_ = Renamed_Column(Data_)

    Data_ = Data_.select("identificacion", "cuenta", "ciudad", "depto", "dato", "tipodato", "Marca")

    return Data_

def Function_Filter_Email(Data_):

    Data_ = Data_.withColumn(
        "Tipologia",
        when(length(split(col("dato"), "@")[0]) < 6, "ERRADO")  # Check the length of the part before the '@'
        .when(size(split(col("dato"), "@")) == 2, "CORREO UNICO")
        .when(size(split(col("dato"), "@")) >= 3, "CORREOS SIN DELIMITAR")
        .otherwise("ERRADO")
    )

    Data_ = Data_.filter(col("dato").contains("@"))

    list_email_replace = [
        "notiene", "nousa", "nobrinda", "000@00.com.co", "nolorecuerda", "notengo", "noposee",
        "nosirve", "notien", "noutili", "nomanej", "nolegust", "nohay", "nocorreo", "noindic",
        "nohay", "@gamil", "pendienteconfirmar", "sincorr", "pendienteporcrearclaro", "correo.claro",
        "crearclaro", ":", "|", " ", "porcrear", "+", "#", "@xxx", "-", "@claro", "suministra", 
        "factelectronica", "nodispone"
    ]

    email_set = set(list_email_replace)

    Data_ = Data_.withColumn("dato", lower(col("dato")))

    contains_any_expr = reduce(
        lambda acc, word: acc | col("dato").contains(word),
        email_set,
        lit(False)
    )

    # Actualizar la columna Tipologia
    Data_ = Data_.withColumn(
        "Tipologia",
        when(contains_any_expr, "ERRADO")
        .otherwise(col("Tipologia"))
        )
    
    Data_ = Data_.filter(col("dato") != "@")
    Data_ = Data_.filter(col("Tipologia") != "ERRADO")

    return Data_