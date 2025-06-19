import modules.filter_base as filter_base
import os
import pyspark
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, concat_ws
from pyspark.sql.functions import expr, when, row_number, collect_list, sum, length
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv
 
spark = get_spark_session()

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, Partitions):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = ORDER_Process(Data_Frame, output_directory, Partitions)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    #DF = change_character_account(DF, "cuenta")
    #DF = change_character_account(DF, "cuenta2")
    
    DF = DF.withColumn(
        "TIPO_BASE", 
        when(((col("nombre_campana") == "FLP 02") | (col("nombre_campana") == "FLP 01") | (col("nombre_campana") == "FLP 03")), concat(lit("CLIENTES "), col("nombre_campana")))
        .when(col("nombre_campana") == "Clientes Corporativos", lit("CLIENTES CORPORATIVOS"))
        .otherwise(lit("CLIENTES INVENTARIO")))
    
    DF = change_name_column(DF, "nombrecompleto")

    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def change_character_account (Data_, Column):

    character_list = ["-"]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    return Data_

### Limpieza de nombres
def change_name_column (Data_, Column):

    character_list = ["SR/SRA", "SR./SRA.", "SR/SRA.","SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)",\
                    "\\.",'#', '$', '/','<', '>', "\\*", "SEÑORES ","SEÑOR(A) ","SEÑOR ","  "]
    
    Data_ = Data_.withColumn(Column, upper(col(Column)))

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    
    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "Cuenta_Next")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "Identificacion")
    Data_Frame = Data_Frame.withColumnRenamed("cuenta2", "Cuenta")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "Fecha_Asignacion")
    Data_Frame = Data_Frame.withColumnRenamed("marca", "Edad_Mora")
    Data_Frame = Data_Frame.withColumnRenamed("marca2", "Marca 2")
    Data_Frame = Data_Frame.withColumnRenamed("origen", "CRM")
    Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "Saldo_Asignado")
    Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "Nombre_Completo")
    Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia")
    Data_Frame = Data_Frame.withColumnRenamed("descuento", "DCTO")
    Data_Frame = Data_Frame.withColumnRenamed("tipo_pago", "TIPO_PAGO")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions):

    Type_File = f"Reordenacion Demograficos Claro"
    delimiter = ";"
    
    save_to_csv(Data_Frame, Directory_to_Save, Type_File, Partitions)

    return Data_Frame

### Dinamización de columnas de celulares
def Phone_Data(Data_):

    Data_ = filter_base.Function_Complete(Data_)

    columns_to_stack_celular = [f"celular{i}" for i in range(1, 11)]
    columns_to_stack_fijo = [f"fijo{i}" for i in range(1, 4)]
    columns_to_stack_min_ = ["min"]
    all_columns_to_stack = columns_to_stack_celular + columns_to_stack_fijo + columns_to_stack_min_
    columns_to_drop_contact = all_columns_to_stack
    Data_ = Data_.withColumn("min ", col("min"))
    Stacked_Data_Frame = Data_.select("*", *all_columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop_contact)
    Stacked_Data_Frame = Data_.select("*")

    return Stacked_Data_Frame

### Desdinamización de líneas
def Phone_Data_Div(Data_Frame):

    for i in range(1, 14):
        Data_Frame = Data_Frame.withColumn(f"phone{i}", when(col("Filtro") == i, col("Dato_Contacto")))

    consolidated_data = Data_Frame.groupBy("cuenta").agg(*[collect_list(f"phone{i}").alias(f"phone_list{i}") for i in range(1, 14)])
    pivoted_data = consolidated_data.select("cuenta", *[concat_ws(",", col(f"phone_list{i}")).alias(f"phone{i}") for i in range(1, 14)])
    
    Data_Frame = Data_Frame.select("identificacion","Cruce_Cuentas", "cuenta", "cuenta2", "marca", "marca2", "origen", "Mod_init_cta", \
                                   "nombrecompleto", "referencia", "descuento", "Filtro", "TIPO_BASE")

    Data_Frame = Data_Frame.join(pivoted_data, "cuenta", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)
    
    return Data_Frame

def cruice(RDD, Value_Compare):

    for position in range(1, 14):
        
        condition = (col(f"phone{position}") == "") & (col(f"phone{position}").isNotNull())
        RDD = RDD.withColumn(f"phone{position}", when(condition, Value_Compare).otherwise(col(f"phone{position}")))

    return RDD

### Proceso de reordenación
def ORDER_Process(Data_, Directory_to_Save, Partitions):

    Data_C = Data_.filter(col("Dato_Contacto") >= 3000000009)
    Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
    Data_F = Data_.filter(col("Dato_Contacto") >= 6010000000)
    Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
    
    Data_ = Data_C.union(Data_F)

    VALUE = "000000000"
    
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))
    Data_ = Data_.withColumn("Cruce_DOCS", concat(col("cuenta"), lit("-"), col("identificacion")))
    
    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    windowSpec = Window.partitionBy("Cruce_DOCS").orderBy("cuenta","Dato_Contacto")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))

    Data_ = Phone_Data_Div(Data_)

    Data_ = Data_.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Price_Col = "Mod_init_cta"     

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    list_columns = ["identificacion", "cuenta", "cuenta2", "phone1", "phone2", "phone3", "phone4", "phone5", "phone6", \
                         "phone7", "phone8", "phone9", "phone10", "phone11", "phone12", "phone13", "marca", "marca2", "origen", \
                         f"{Price_Col}", "nombrecompleto", "referencia", "descuento", "TIPO_BASE"]
    
    #############################
    Data_ = Data_.filter(col("marca") != "castigo")
    #############################

    Data_ = Data_.select(list_columns)

    Data_ = cruice(Data_, VALUE)

    for length_column in range(1, 14):

        condition_null = (col(f"phone{length_column}") == "000000000")
        condition_mobile = (col(f"phone{length_column}") <= 3599999999)
        Data_ = Data_.withColumn(f"New_Filter{length_column}", when(condition_null, 3)\
                                 .when(condition_mobile, 1).otherwise(2))

    Data_ = Data_.withColumn("order_filter", concat("New_Filter1", "New_Filter2", "New_Filter3", "New_Filter4", "New_Filter5", "New_Filter6",\
                     "New_Filter7", "New_Filter8", "New_Filter9", "New_Filter10", "New_Filter11", "New_Filter12", "New_Filter13"))

    Data_ = Data_.withColumn("order_filter", (col("order_filter") * 1))
    Data_ = Data_.orderBy(["order_filter"], ascending = True)

    for position in range(1, 14):

        condition = (col(f"phone{position}") == VALUE)
        Data_ = Data_.withColumn(f"phone{position}", when(condition, "").otherwise(col(f"phone{position}")))
        Data_ = Data_
    
    Data_ = Data_.select(list_columns)

    Movil_Filter = (
    (col("phone1").between(3000000009, 3599999999)) |
    (col("phone2").between(3000000009, 3599999999)) |
    (col("phone3").between(3000000009, 3599999999)) |
    (col("phone4").between(3000000009, 3599999999)) |
    (col("phone5").between(3000000009, 3599999999)) |
    (col("phone6").between(3000000009, 3599999999)) |
    (col("phone7").between(3000000009, 3599999999)) |
    (col("phone8").between(3000000009, 3599999999)) |
    (col("phone9").between(3000000009, 3599999999)) |
    (col("phone10").between(3000000009, 3599999999)) |
    (col("phone11").between(3000000009, 3599999999)) |
    (col("phone12").between(3000000009, 3599999999)) |
    (col("phone13").between(3000000009, 3599999999))
)


    Data_ = Data_.withColumn("MOVIL", when(Movil_Filter, "SI TIENE").otherwise("NO TIENE"))


    Data_ = Data_.withColumn("No.Movil", expr(
    "IF(phone1 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone2 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone3 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone4 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone5 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone6 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone7 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone8 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone9 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone10 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone11 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone12 BETWEEN 3000000009 AND 3599999999, 1, 0) + \
     IF(phone13 BETWEEN 3000000009 AND 3599999999, 1, 0)"))


    FIJO_Movil_Filter = (
    (col("phone1").between(6000000009, 6599999999)) |
    (col("phone2").between(6000000009, 6599999999)) |
    (col("phone3").between(6000000009, 6599999999)) |
    (col("phone4").between(6000000009, 6599999999)) |
    (col("phone5").between(6000000009, 6599999999)) |
    (col("phone6").between(6000000009, 6599999999)) |
    (col("phone7").between(6000000009, 6599999999)) |
    (col("phone8").between(6000000009, 6599999999)) |
    (col("phone9").between(6000000009, 6599999999)) |
    (col("phone10").between(6000000009, 6599999999)) |
    (col("phone11").between(6000000009, 6599999999)) |
    (col("phone12").between(6000000009, 6599999999)) |
    (col("phone13").between(6000000009, 6599999999))
)

    Data_ = Data_.withColumn("FIJO", when(FIJO_Movil_Filter, "SI TIENE").otherwise("NO TIENE"))

    Data_ = Data_.withColumn("No.Fijo", expr(
    "IF(phone1 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone2 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone3 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone4 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone5 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone6 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone7 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone8 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone9 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone10 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone11 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone12 BETWEEN 6000000009 AND 6599999999, 1, 0) + \
     IF(phone13 BETWEEN 6000000009 AND 6599999999, 1, 0)"))

    Save_Data_Frame(Data_, Directory_to_Save, Partitions)

    return Data_