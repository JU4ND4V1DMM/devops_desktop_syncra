import os
import utils.Active_Lines
from web.pyspark import get_spark_session
from datetime import datetime
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, regexp_replace, when, date_format, current_date, to_date, date_format, split, length, upper
from web.save_files import save_to_0csv, save_to_csv

class Charge_DB(QtWidgets.QMainWindow):

    def __init__(self, row_count, file_path, folder_path, process_data):
        
        super().__init__()
        
        self.spinBox_Partitions = None
        self.partitions = None

        self.file_path = file_path
        self.folder_path = folder_path
        self.process_data = process_data
        self.digit_partitions()
        self.exec_process()

    def digit_partitions(self):

        partitions_CAM = self.process_data.spinBox_Partitions.value()
        print(partitions_CAM)
        self.partitions = partitions_CAM

    def exec_process(self):
        
        self.digit_partitions()
        self.data_to_process = []
        self.process_data.pushButton_CAM.clicked.connect(self.generate_DB)
        self.process_data.pushButton_Partitions_BD.clicked.connect(self.Partitions_Data_Base)
        self.process_data.pushButton_MINS.clicked.connect(self.mins_from_bd)

    def generate_DB(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        self.BD_Control_Next()
        self.DB_Create()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de creación ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def Partitions_Data_Base(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        self.partition_DATA()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de partición ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def mins_from_bd(self):

        path =  self.file_path
        output_directory = self.folder_path
        partitions = self.partitions

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        utils.Active_Lines.Function_Complete(path, output_directory, partitions)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de valdiación de líneas ejecutado exitosamente.")
        Mbox_In_Process.exec()
    
    def Update_BD_ControlNext(self, Data_Root):
        
        Data_Root = Data_Root.withColumn("[AccountAccountCode?]", regexp_replace(col("[AccountAccountCode?]"), "-", ""))
        Data_Root = Data_Root.withColumn("[AccountAccountCode2?]", col("[AccountAccountCode?]"))
        
        Data_Root = Data_Root.withColumn("Numero de Cliente", regexp_replace("Numero de Cliente", "[^0-9]", ""))
        Data_Root = Data_Root.withColumn("Numero de Cliente", when(col("Numero de Cliente").isNull(), lit("0")).otherwise(col("Numero de Cliente")))
        Data_Root = Data_Root.withColumn("Numero de Cliente", col("Numero de Cliente").cast("int"))
        Data_Root = Data_Root.withColumn("[Documento?]", col("Numero de Cliente"))
        
        Data_Root = Data_Root.withColumn("Precio Subscripcion", lit(""))
        
        Data_Root = Data_Root.withColumn("Fecha de Aceleracion", date_format(to_date(col("Fecha de Aceleracion"), "d/MM/yyyy"), "yyyy-MM-dd"))
        Data_Root = Data_Root.withColumn("Fecha de Vencimiento", date_format(to_date(col("Fecha de Vencimiento"), "d/MM/yyyy"), "yyyy-MM-dd"))

        Data_Root = Data_Root.withColumn("Fecha Final ", date_format(to_date(split(col("Fecha Final "), " ")[0], "d/M/yyyy"), "yyyy-MM-dd"))
        Data_Root = Data_Root.withColumn("Fecha de Asignacion", date_format(to_date(split(col("Fecha de Asignacion"), " ")[0], "d/M/yyyy"), "yyyy-MM-dd"))
        
        return Data_Root
    
    def change_name_column (self, Data_, Column):

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

    def BD_Control_Next(self):

        spark = get_spark_session()

        sqlContext = SQLContext(spark)
        
        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        list_origins = ["ASCARD", "RR", "BSCS", "SGA"]

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")

        Data_Root = spark.read.csv(file, header= True, sep=";")
        Data_Root = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

        columns_to_list = [f"{i}_" for i in range(1, 53)]
        Data_Root = Data_Root.select(columns_to_list)
        Data_Root = Data_Root.filter(col("3_").isin(list_origins))

        ##############
        #Data_Root = Data_Root.filter(col("7_") != "Y")      #Filter Brand CASTIGO
        ##############

        Data_Root = Data_Root.withColumn("Telefono 1", lit(""))
        Data_Root = Data_Root.withColumn("Telefono 2", lit(""))
        Data_Root = Data_Root.withColumn("Telefono 3", lit(""))
        Data_Root = Data_Root.withColumn("Telefono 4", lit(""))
        Data_Root = Data_Root.withColumn("Valor Scoring", lit(""))
        Data_Root = Data_Root.withColumn("[AccountAccountCode2?]", col("2_"))
        Data_Root = Data_Root.withColumn("44_", lit(""))

        columns_to_list = ["1_", "2_", "3_", "4_", "5_", "6_", "7_", "8_", "9_", "10_", "11_", "12_", \
                           "13_", "14_", "15_", "16_", "17_", "18_", "51_", "Telefono 1", "Telefono 2", "Telefono 3", \
                           "Telefono 4", "Valor Scoring", "19_", "20_", "21_", "22_", "23_", "24_", "25_", \
                           "26_", "27_", "28_", "29_", "30_", "31_", "32_", "33_", "34_", "35_", "36_", "37_", \
                           "38_", "39_", "40_", "41_", "42_", "43_", "44_", "[AccountAccountCode2?]"]
        
        Data_Root = Data_Root.select(columns_to_list)
        Data_Root = Data_Root.dropDuplicates(["2_"])
        Data_Root = Data_Root.orderBy(col("3_"))

        Data_Root = Data_Root.withColumn("24_2", col("24_"))
        Data_Root = self.change_name_column(Data_Root, "24_2")
        Data_Root = Data_Root.withColumn("24_", when(length(col("24_2")) < 7, col("24_")).otherwise(col("24_2")))
        
        Data_Root = Data_Root.select(columns_to_list)
        
        Data_Root = Data_Root.withColumnRenamed("1_", "Numero de Cliente")
        Data_Root = Data_Root.withColumnRenamed("2_", "[AccountAccountCode?]")
        Data_Root = Data_Root.withColumnRenamed("3_", "CRM Origen")
        Data_Root = Data_Root.withColumnRenamed("4_", "Edad de Deuda")
        Data_Root = Data_Root.withColumnRenamed("5_", "[PotencialMark?]")
        Data_Root = Data_Root.withColumnRenamed("6_", "[PrePotencialMark?]")
        Data_Root = Data_Root.withColumnRenamed("7_", "[WriteOffMark?]")
        Data_Root = Data_Root.withColumnRenamed("8_", "Monto inicial")
        Data_Root = Data_Root.withColumnRenamed("9_", "[ModInitCta?]")
        Data_Root = Data_Root.withColumnRenamed("10_", "[DeudaRealCuenta?]")
        Data_Root = Data_Root.withColumnRenamed("11_", "[BillCycleName?]")
        Data_Root = Data_Root.withColumnRenamed("12_", "Nombre Campana")
        Data_Root = Data_Root.withColumnRenamed("13_", "[DebtAgeInicial?]")
        Data_Root = Data_Root.withColumnRenamed("14_", "Nombre Casa de Cobro")
        Data_Root = Data_Root.withColumnRenamed("15_", "Fecha de Asignacion")
        Data_Root = Data_Root.withColumnRenamed("16_", "Deuda Gestionable")
        Data_Root = Data_Root.withColumnRenamed("17_", "Direccion Completa")
        Data_Root = Data_Root.withColumnRenamed("18_", "Fecha Final ")
        Data_Root = Data_Root.withColumnRenamed("51_", "Email")
        Data_Root = Data_Root.withColumnRenamed("19_", "Segmento")
        Data_Root = Data_Root.withColumnRenamed("20_", "[Documento?]")
        Data_Root = Data_Root.withColumnRenamed("21_", "[AccStsName?]")
        Data_Root = Data_Root.withColumnRenamed("22_", "Ciudad")
        Data_Root = Data_Root.withColumnRenamed("23_", "[InboxName?]")
        Data_Root = Data_Root.withColumnRenamed("24_", "Nombre del Cliente")
        Data_Root = Data_Root.withColumnRenamed("25_", "Id de Ejecucion")
        Data_Root = Data_Root.withColumnRenamed("26_", "Fecha de Vencimiento")
        Data_Root = Data_Root.withColumnRenamed("27_", "Numero Referencia de Pago")
        Data_Root = Data_Root.withColumnRenamed("28_", "MIN")
        Data_Root = Data_Root.withColumnRenamed("29_", "Plan")
        Data_Root = Data_Root.withColumnRenamed("30_", "Cuotas Aceleradas")
        Data_Root = Data_Root.withColumnRenamed("31_", "Fecha de Aceleracion")
        Data_Root = Data_Root.withColumnRenamed("32_", "Valor Acelerado")
        Data_Root = Data_Root.withColumnRenamed("33_", "Intereses Contingentes")
        Data_Root = Data_Root.withColumnRenamed("34_", "Intereses Corrientes Facturados")
        Data_Root = Data_Root.withColumnRenamed("35_", "Intereses por mora facturados")
        Data_Root = Data_Root.withColumnRenamed("36_", "Cuotas Facturadas")
        Data_Root = Data_Root.withColumnRenamed("37_", "Iva Intereses Contigentes Facturado")
        Data_Root = Data_Root.withColumnRenamed("38_", "Iva Intereses Corrientes Facturados")
        Data_Root = Data_Root.withColumnRenamed("39_", "Iva Intereses por Mora Facturado")
        Data_Root = Data_Root.withColumnRenamed("40_", "Precio Subscripcion")
        Data_Root = Data_Root.withColumnRenamed("41_", "Codigo de proceso")
        Data_Root = Data_Root.withColumnRenamed("42_", "[CustomerTypeId?]")
        Data_Root = Data_Root.withColumnRenamed("43_", "[RefinanciedMark?]")
        Data_Root = Data_Root.withColumnRenamed("44_", "[Discount?]")

        Data_Error = Data_Root

        Data_Root = Data_Root.filter(col("[CustomerTypeId?]") >= 0)
        Data_Root = Data_Root.filter(col("[CustomerTypeId?]") <= 100)
        name = "Cargue" 
        origin = "Multiorigen"
        self.Save_File(Data_Root, root, partitions, name, origin, Time_File)

        Data_Brands = Data_Root.filter(col("[WriteOffMark?]") != "Y")
        name = "Multimarca_Cargue"
        origin = "Multiorigen"
        self.Save_File(Data_Brands, root, partitions, name, origin, Time_File)
        
        Data_Brands_Update = self.Update_BD_ControlNext(Data_Brands)
        name = "Multimarca_Cargue_Actualizacion"
        origin = "Multiorigen"
        self.Save_File(Data_Brands_Update, root, partitions, name, origin, Time_File)

        Data_Error = Data_Error.filter((col("[CustomerTypeId?]").isNull()) & (col("[CustomerTypeId?]").cast("double").isNull()))
        name = "Errores"
        origin = "Multiorigen"
        self.Save_File(Data_Error, root, partitions, name, origin, Time_File)

        return Data_Root

    def DB_Create(self):
        
        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        list_origins = ["ASCARD", "RR", "BSCS", "SGA"]

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")

        origin_list = list_origins
        RDD_Data = self.Function_Complete(file)
        RDD_Data = self.Renamed_column(RDD_Data)

        origin = "Multiorigen"
        brand = "Multimarca"
        RDD_Data_MULTIBRAND = RDD_Data.filter(col("CRM_Origen").isin(list_origins))
        RDD_Data_MULTIBRAND = RDD_Data_MULTIBRAND.filter(col("Marca_Asignada") != "Castigo")

        self.Save_File(RDD_Data_MULTIBRAND, root, partitions, brand, origin, Time_File)

        origin = "Multiorigen"
        brand = "castigo"
        RDD_Data_CAST = RDD_Data.filter(col("CRM_Origen").isin(list_origins))
        RDD_Data_CAST = RDD_Data_CAST.filter(col("Marca_Asignada") == "Castigo")

        self.Save_File(RDD_Data_CAST, root, partitions, brand, origin, Time_File)

        origin = "ASCARD - RR - SGA"
        brand = "castigo"
        list_origins = ["ASCARD", "RR", "SGA"]
        RDD_Data_CAST_AR = RDD_Data_CAST.filter(col("CRM_Origen").isin(list_origins))

        self.Save_File(RDD_Data_CAST_AR, root, partitions, brand, origin, Time_File)

        origin = "BSCS"
        brand = "castigo"
        list_origins = ["BSCS"]
        RDD_Data_CAST_SB = RDD_Data_CAST.filter(col("CRM_Origen").isin(list_origins))

        self.Save_File(RDD_Data_CAST_SB, root, partitions, brand, origin, Time_File)

    def Function_Complete(self, path):

        spark = get_spark_session()

        sqlContext = SQLContext(spark)

        Data_Root = spark.read.csv(path, header= True, sep=";")
        Data_Root = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

        #ActiveLines
        Data_Root = Data_Root.withColumn("52_", concat(col("53_"), lit(","), col("54_"), lit(", "), col("55_"), lit(","), col("56_")))

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

        Data_Root = Data_Root.dropDuplicates(["2_"])

        Data_Root = Data_Root.withColumn("53_", when(potencial, "Potencial")\
                                            .when(churn, "Churn")\
                                            .when(provision, "Provision")\
                                            .when(prepotencial, "Prepotencial")\
                                            .when(prechurn, "Prechurn")\
                                            .when(preprovision, "Preprovision")\
                                            .when(castigo, "Castigo")\
                                            .when(potencial_a_castigar, "Potencial a Castigar")\
                                            .otherwise(marcas))
        
        moras_numericas = (col("53_") == "120") | (col("53_") == "150") | (col("53_") == "180")
        prepotencial_especial = (col("53_") == "Prepotencial") & (col("3_") == "BSCS") & ((col("12_") == "PrePotencial Convergente Masivo_2") | (col("12_") == "PrePotencial Convergente Pyme_2"))

        Data_Root = Data_Root.withColumn("53_", when(moras_numericas, "120 - 180")\
                                            .when(prepotencial_especial, "Prepotencial Especial")\
                                            .otherwise(col("53_")))

        Data_Root = Data_Root.withColumn("54_", regexp_replace(col("2_"), "[.-]", ""))

        Data_Root = Data_Root.withColumn("55_", col("9_").cast("double"))

        Data_Root = Data_Root.withColumn("55_", regexp_replace("55_", "\\.", ","))
        
        Segment = ((col("42_") == "81") | (col("42_") == "84") | (col("42_") == "87"))
        Data_Root = Data_Root.withColumn("56_",
                          when(Segment, "Personas")
                          .otherwise("Negocios"))

        Data_Root = Data_Root.withColumn("57_", \
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
        
        Data_Root = Data_Root.orderBy(col("3_"))
        
        return Data_Root
        
    def Save_File(self, Data_Frame, Directory_to_Save, Partitions, Brand_Filter, Origin_Filter, Time_File):

        if Brand_Filter == "castigo":
            Type_File = f"---- Bases para CRUCE ----"
            extension = "0csv"
            Name_File = f"Castigo {Origin_Filter}"
        
        elif Brand_Filter == "Cargue" or Brand_Filter == "Errores" or Brand_Filter == "Multimarca_Cargue" or Brand_Filter == "Multimarca_Cargue_Actualizacion":
            Type_File = f"---- Bases para CARGUE ----"
            extension = "csv"

            if Brand_Filter == "Errores":
                Type_File = f"Base_de_CARGUE_{Time_File}/Errores"
                Name_File = "de Errores (NO RELACIONADA EN CARGUE)"
                extension = "0csv"

            elif Brand_Filter == "Multimarca_Cargue":
                Type_File = f"Base_de_CARGUE_{Time_File}/Cargue sin Castigo"
                Name_File = "Cargue UNIF sin Castigo"
            
            elif Brand_Filter == "Multimarca_Cargue_Actualizacion":
                Type_File = f"Base_de_CARGUE_{Time_File}/Cargue sin Castigo Actualizacion"
                Name_File = "Cargue UNIF Actualizacion sin Castigo"

            else:
                Name_File = "Cargue UNIF"

        else: 
            Type_File = f"---- Bases para CRUCE ----"
            extension = "0csv"
            Name_File = "Multimarca"
            
        delimiter = ";"
        output_path = f'{Directory_to_Save}{Type_File}'
        Name_File = f'BD Cruce {Name_File}'
        
        if extension == "csv":
            save_to_csv(Data_Frame, output_path, Name_File, Partitions, delimiter)
        else:
            save_to_0csv(Data_Frame, output_path, Name_File, Partitions, delimiter)

    def Renamed_column(self, Data_Root):

        Data_Root = Data_Root.withColumnRenamed("1_", "Documento")
        Data_Root = Data_Root.withColumnRenamed("2_", "Cuenta")
        Data_Root = Data_Root.withColumnRenamed("3_", "CRM_Origen")
        Data_Root = Data_Root.withColumnRenamed("4_", "Edad de Deuda")
        Data_Root = Data_Root.withColumnRenamed("5_", "Potencial_Mark")
        Data_Root = Data_Root.withColumnRenamed("6_", "PrePotencial_Mark")
        Data_Root = Data_Root.withColumnRenamed("7_", "Write_Off_Mark")
        Data_Root = Data_Root.withColumnRenamed("8_", "Monto inicial")
        Data_Root = Data_Root.withColumnRenamed("9_", "Mod_Init_Cta")
        Data_Root = Data_Root.withColumnRenamed("10_", "Deuda_Real_Cuenta")
        Data_Root = Data_Root.withColumnRenamed("11_", "Bill_CycleName")
        Data_Root = Data_Root.withColumnRenamed("12_", "Nombre Campana")
        Data_Root = Data_Root.withColumnRenamed("13_", "Debt_Age_Inicial")
        Data_Root = Data_Root.withColumnRenamed("14_", "Nombre_Casa_de_Cobro")
        Data_Root = Data_Root.withColumnRenamed("15_", "Fecha_de_Asignacion")
        Data_Root = Data_Root.withColumnRenamed("16_", "Deuda_Gestionable")
        Data_Root = Data_Root.withColumnRenamed("17_", "Direccion_Completa")
        Data_Root = Data_Root.withColumnRenamed("18_", "Fecha_Final")
        Data_Root = Data_Root.withColumnRenamed("19_", "Segmento")
        Data_Root = Data_Root.withColumnRenamed("20_", "Documento_Limpio")
        Data_Root = Data_Root.withColumnRenamed("21_", "Acc_Sts_Name")
        Data_Root = Data_Root.withColumnRenamed("22_", "Ciudad")
        Data_Root = Data_Root.withColumnRenamed("23_", "Inbox_Name")
        Data_Root = Data_Root.withColumnRenamed("24_", "Nombre_del_Cliente")
        Data_Root = Data_Root.withColumnRenamed("25_", "Id_de_Ejecucion")
        Data_Root = Data_Root.withColumnRenamed("26_", "Fecha_de_Vencimiento")
        Data_Root = Data_Root.withColumnRenamed("27_", "Numero_Referencia_de_Pago")
        Data_Root = Data_Root.withColumnRenamed("28_", "MIN")
        Data_Root = Data_Root.withColumnRenamed("29_", "Plan")
        Data_Root = Data_Root.withColumnRenamed("30_", "Cuotas_Aceleradas")
        Data_Root = Data_Root.withColumnRenamed("31_", "Fecha_de_Aceleracion")
        Data_Root = Data_Root.withColumnRenamed("32_", "Valor_Acelerado")
        Data_Root = Data_Root.withColumnRenamed("33_", "Intereses_Contingentes")
        Data_Root = Data_Root.withColumnRenamed("34_", "Intereses_Corrientes_Facturados")
        Data_Root = Data_Root.withColumnRenamed("35_", "Intereses_por_mora_facturados")
        Data_Root = Data_Root.withColumnRenamed("36_", "Cuotas_Facturadas")
        Data_Root = Data_Root.withColumnRenamed("37_", "Iva_Intereses_Contigentes_Facturado")
        Data_Root = Data_Root.withColumnRenamed("38_", "Iva Intereses Corrientes_Facturados")
        Data_Root = Data_Root.withColumnRenamed("39_", "Iva_Intereses_por_Mora_Facturado")
        Data_Root = Data_Root.withColumnRenamed("40_", "Precio_Subscripcion")
        Data_Root = Data_Root.withColumnRenamed("41_", "Codigo_de_proceso")
        Data_Root = Data_Root.withColumnRenamed("42_", "Customer_Type_Id")
        Data_Root = Data_Root.withColumnRenamed("43_", "Refinancied_Mark")
        Data_Root = Data_Root.withColumnRenamed("44_", "Discount")
        Data_Root = Data_Root.withColumnRenamed("45_", "Permanencia")
        Data_Root = Data_Root.withColumnRenamed("46_", "Deuda_sin_Permanencia")
        Data_Root = Data_Root.withColumnRenamed("47_", "Telefono_1")
        Data_Root = Data_Root.withColumnRenamed("48_", "Telefono_2")
        Data_Root = Data_Root.withColumnRenamed("49_", "Telefono_3")
        Data_Root = Data_Root.withColumnRenamed("50_", "Telefono_4")
        Data_Root = Data_Root.withColumnRenamed("51_", "Email")
        Data_Root = Data_Root.withColumnRenamed("52_", "Active_Lines")
        Data_Root = Data_Root.withColumnRenamed("53_", "Marca_Asignada")
        Data_Root = Data_Root.withColumnRenamed("54_", "Cuenta_Next")
        Data_Root = Data_Root.withColumnRenamed("55_", "Valor_Deuda")
        Data_Root = Data_Root.withColumnRenamed("57_", "Rango_Deuda")
        Data_Root = Data_Root.withColumn("Multiproducto", lit(""))
        Data_Root = Data_Root.withColumn("Fecha_Ingreso", date_format(current_date(), "dd/MM/yyyy"))

        return Data_Root
    
    def partition_DATA(self):

        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        try:
            with open(file, 'r', encoding='utf-8') as origin_file:
                rows = origin_file.readlines()

                rows_por_particion = len(rows) // partitions

                for i in range(partitions):
                    begining = i * rows_por_particion
                    end = (i + 1) * rows_por_particion if i < partitions - 1 else len(rows)

                    if i > 0:
                        extension_file = "0csv"
                    else:
                        extension_file = "csv"

                    nombre_particion = os.path.join(root, f"Particion {i+1}.{extension_file}")

                    with open(nombre_particion, 'w', encoding='utf-8') as file_output:
                        file_output.writelines(rows[begining:end])

                if end < len(rows):
                    nombre_particion = os.path.join(root, f"File_Part_{partitions+1}.csv")
                    with open(nombre_particion, 'w', encoding='utf-8') as file_output:
                        file_output.writelines(rows[end:])

        except:
            pass

    
    
