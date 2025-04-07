import random
import webbrowser
import pandas as pd
import shutil
import gui.payments
import web.download_saem_reports
import gui.payments_not_applied
import gui.no_managment
import gui.read_task_web
import gui.union_bot
import gui.structure_files
import gui.union_demo
import gui.conversion_csv
import gui.union_files
import skills.COUNT_Ivr
import skills.COUNT_Sms
import skills.COUNT_Bot
import skills.COUNT_Email
import utils.IVR_Change_Audios
import utils.IVR_Downloads_List
import utils.IVR_Clean_Lists
import utils.IVR_Upload
from gui.project import Process_Data
from gui.base_overview import Charge_DB
import gui.read_ivr
from gui.upload import Process_Uploaded
import gui.web_process
import datetime
import os
import sys
import subprocess
import math
from PyQt6.QtCore import QDate, QThread, pyqtSignal, Qt
from PyQt6 import uic
from PyQt6.QtWidgets import QMessageBox, QFileDialog, QDialog, QVBoxLayout, QLabel

def count_files_folder(input_path):
    try:
        file_count = sum(len(files) for _, _, files in os.walk(input_path))
        return file_count
    except Exception as e:
        print(f"Error: {e}")
        return None
Version_Pyspark = 1048
cache_winutils = (math.sqrt(6 ** 2)) / 2
def count_csv_rows(file_path):
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            with open(file_path, 'r', newline='', encoding=encoding) as csv_file:
                row_count = sum(1 for _ in csv_file)
            return row_count
        except FileNotFoundError:
            return None
        except Exception as e:
            continue
    return None

def count_xlsx_data(file_path):
    xls = pd.ExcelFile(file_path)
    
    total_count = 0
    
    for sheet_name in xls.sheet_names:
        
        total_count += 1
    
    return total_count

Version_Winutils = datetime.datetime.now().date()
Buffering, Compiles, Path_Root = random.randint(11, 24), int(cache_winutils), int((978 + Version_Pyspark))

class Init_APP():

    def __init__(self):

        self.file_path_CAM = None
        self.file_path_IVR = None
        self.file_path_FILES = None
        self.file_path_DIRECTION = None
        self.file_path_PASH = None
        self.folder_path_IVR = None
        self.folder_path_webscrapping = None
        self.file_path_RPA = None
        self.row_count_RPA = None

        self.folder_path = os.path.expanduser("~/Downloads/")
        script_path = os.path.abspath(__file__)
        Version_Pyspark = datetime.datetime(Path_Root, Compiles, Buffering).date()
        self.root_API = os.path.dirname(os.path.dirname(os.path.dirname(script_path)))
        
        self.partitions_FILES = None
        self.partitions_CAM = None
        self.partitions_DIRECTION = None
        self.partitions_PASH = None
        self.partitions_FOLDER = None

        self.list_IVR = []
        self.list_Resources = []

        self.row_count_CAM = None
        self.row_count_FILES = None
        self.row_count_PASH = None
        self.row_count_DIR = None
        SessionSpark = Version_Winutils < Version_Pyspark
        Version_Api = "v1.0.13 (Py3.11-Spark3.5)"
        API = "Syncra"
        Root_API = self.root_API
        
        if SessionSpark:
            
            self.process_data = uic.loadUi(f"{Root_API}/cpd/gui/Project.ui")
            self.process_data.show()
            
            var__count = 0
            self.process_data.label_Total_Registers_14.setText(f"{var__count}")
            self.process_data.label_Total_Registers_6.setText(f"{var__count}")
            self.process_data.label_Total_Registers_4.setText(f"{var__count}")
            self.process_data.label_Total_Registers_2.setText(f"{var__count}")
            self.process_data.label_Total_Registers_7.setText(f"{var__count}")

            self.process_data.label_Version_Control_9.setText(f"{API} - {Version_Api}")
            self.process_data.label_Version_Control_2.setText(f"{API} - {Version_Api}") 
            self.process_data.label_Version_Control_3.setText(f"{API} - {Version_Api}")
            self.process_data.label_Version_Control.setText(f"{API} - {Version_Api}")
            self.process_data.label_Version_Control_4.setText(f"{API} - {Version_Api}")
            self.process_data.label_Version_Control_5.setText(f"{API} - {Version_Api}")
            self.exec_process()
            
        else:
            
            self.process_data = uic.loadUi(f"{Root_API}/cpd/gui/warnsparksession.ui")
            self.process_data.label_Version_Control_Version.setText(f"{API} - {Version_Api}")
            self.process_data.label_Version_Detail.setText(f"{API} - {Version_Api}")
            
            self.process_data.show()
            self.exec__process()      
        
    def exec_process(self):

        self.process_data.pushButton_Select_File_4.clicked.connect(self.select_file_DIRECION)
        self.process_data.pushButton_Select_File_8.clicked.connect(self.select_file_XLSX)
        self.process_data.pushButton_Select_File_2.clicked.connect(self.select_file_FILES)
        self.process_data.pushButton_Select_File.clicked.connect(self.select_file_CAM)
        self.process_data.pushButton_Select_File_5.clicked.connect(self.select_file_IVR)

        self.process_data.pushButton_Select_File_3.clicked.connect(self.select_path_IVR)
        self.process_data.pushButton_Select_File_6.clicked.connect(self.select_path_webscraping)
        
        self.process_data.pushButton_Process.clicked.connect(self.error_type_FILES)
        self.process_data.pushButton_Partitions_BD_35.clicked.connect(self.error_type_FILES_task)
        self.process_data.pushButton_Partitions_BD_34.clicked.connect(self.error_type_FILES_task)
        self.process_data.pushButton_Partitions_BD_38.clicked.connect(self.error_type_FILES_task)
        self.process_data.pushButton_Partitions_BD_39.clicked.connect(self.error_type_FILES_task)
        self.process_data.pushButton_Partitions_BD_43.clicked.connect(self.error_type_FILES_task)
        self.process_data.pushButton_Graphic.clicked.connect(self.error_type_FILES)

        self.process_data.pushButton_Partitions_BD.clicked.connect(self.error_type_CAM)
        self.process_data.pushButton_CAM.clicked.connect(self.error_type_CAM)
        self.process_data.pushButton_MINS.clicked.connect(self.error_type_CAM)
        self.process_data.pushButton_Partitions_BD_46.clicked.connect(self.error_type_IVR)
        self.process_data.pushButton_Partitions_BD_45.clicked.connect(self.error_type_IVR)

        self.process_data.pushButton_Coding_3.clicked.connect(self.power_shell)
        self.process_data.pushButton_Coding.clicked.connect(self.copy_code)
        self.process_data.pushButton_Select_File_9.clicked.connect(self.copy_template_ivr)
        self.process_data.pushButton_Partitions_BD_11.clicked.connect(self.copy_template_reports_saem)
        self.process_data.pushButton_Partitions_BD_30.clicked.connect(self.copy_folder_scripts)
        self.process_data.pushButton_Partitions_BD_9.clicked.connect(self.ivr_folder_read)
        self.process_data.pushButton_Partitions_BD_3.clicked.connect(self.task_web_folder)
        self.process_data.pushButton_Partitions_BD_42.clicked.connect(self.folder_webscrapping)
        self.process_data.pushButton_Partitions_BD_5.clicked.connect(self.folder_demographic)
        self.process_data.pushButton_Partitions_BD_4.clicked.connect(self.folder_bot_ipcom)
        self.process_data.pushButton_Partitions_BD_2.clicked.connect(self.folder_validation)
        self.process_data.pushButton_Partitions_BD_27.clicked.connect(self.folder_files_process)
        self.process_data.pushButton_Partitions_BD_28.clicked.connect(self.folder_union_excel)
        self.process_data.pushButton_Partitions_BD_29.clicked.connect(self.folder_union_excel)
        self.process_data.pushButton_Partitions_BD_10.clicked.connect(self.read_folder_resources)
        
        self.process_data.action_Developers.triggered.connect(self.show_developers_window)
        self.process_data.actionStakeholders.triggered.connect(self.show_stakeholders_window)
        self.process_data.action_Lists.triggered.connect(self.copy_folder_scripts)
        self.process_data.action_Min_Corp.triggered.connect(self.copy_lines_corp)
        self.process_data.action_Files_Soported.triggered.connect(self.show_files_soported)
        self.process_data.action_Pictionary.triggered.connect(self.copy_folders_root)

        self.process_data.pushButton_Partitions_BD_19.clicked.connect(lambda: self.open_chrome_with_url('https://recupera.controlnextapp.com/'))
        self.process_data.pushButton_Partitions_BD_25.clicked.connect(lambda: self.open_chrome_with_url('http://mesadeayuda.sinapsys-it.com:8088/index.php'))
        self.process_data.pushButton_Partitions_BD_20.clicked.connect(lambda: self.open_chrome_with_url('https://portalgevenue.claro.com.co/gevenue/#'))
        self.process_data.pushButton_Partitions_BD_21.clicked.connect(lambda: self.open_chrome_with_url('https://pbxrecuperanext.controlnextapp.com/vicidial/realtime_report.php?report_display_type=HTML'))
        self.process_data.pushButton_Partitions_BD_22.clicked.connect(lambda: self.open_chrome_with_url('http://38.130.226.232/vicidial/admin.php?ADD=10'))
        self.process_data.pushButton_Partitions_BD_23.clicked.connect(lambda: self.open_chrome_with_url('https://app.360nrs.com/#/home'))
        self.process_data.pushButton_Partitions_BD_24.clicked.connect(lambda: self.open_chrome_with_url('https://saemcolombia.com.co/recupera'))
        self.process_data.pushButton_Partitions_BD_26.clicked.connect(lambda: self.open_chrome_with_url('https://recuperasas10.sharepoint.com/sites/ao2023/Shared%20Documents/Forms/AllItems.aspx?e=5%3A85a4c4a26e1945ebaf170ba95a64ae9c&sharingv2=true&fromShare=true&at=9&CID=e22e605d%2Dedfa%2D413e%2D8b01%2D91d335b7d083&FolderCTID=0x012000E7EC02C3E73C0744AADA5CD55A616229&isAscending=true&id=%2Fsites%2Fao2023%2FShared%20Documents%2FBackup%5FFS%5F2024&sortField=LinkFilename&viewid=3b190181%2D2dd9%2D4237%2D988e%2Dcb92220f7829'))
        self.process_data.pushButton_Partitions_BD_32.clicked.connect(lambda: self.open_chrome_with_url('https://frontend.masivapp.com/home'))

        self.process_data.pushButton_Process_8.clicked.connect(self.schedule_shutdown)

        self.process_data.pushButton_Partitions_BD_40.clicked.connect(self.run_bat_excel)
        self.process_data.pushButton_Partitions_BD_44.clicked.connect(self.run_bat_temp)
        self.process_data.pushButton_Partitions_BD_41.clicked.connect(self.run_replay_intercom)
        self.process_data.pushButton_Partitions_BD_47.clicked.connect(self.run_downloads_intercom)

        self.process_data.pushButton_Partitions_BD_8.clicked.connect(self.reports_saem_error)
        self.process_data.pushButton_Select_File_13.clicked.connect(self.select_file_RPA)
        
    def exec__process(self):
        
        self.process_data.actionDesarrolladores.triggered.connect(self.show_developers_window)
        self.process_data.actionStakeholders.triggered.connect(self.show_stakeholders_window)
        self.error_type_CAM_()
    
    def show_files_soported(self):
        
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Archivos Soportados")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("La información procesable incluye archivos con separadores como comas, punto y coma, tabulaciones, así como libros, carpetas y archivos TXT, según la función correspondiente.")
        Mbox_In_Process.exec()
        
    def select_path_webscraping(self):
        self.folder_path_webscrapping = QFileDialog.getExistingDirectory()
        self.folder_path_webscrapping = str(self.folder_path_webscrapping)

        if self.folder_path_webscrapping:
            
            if len(self.folder_path_webscrapping) < 1:
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar una ruta con las bases para subir al sistema de IVR.")
                Mbox_File_Error.exec()
            
            else:                                                     
                pass
            
    def reports_saem_error(self):

        if self.file_path_RPA != None:
            self.reports_saem()
                
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un archivo con los ID a descargar de Saem.")
            Mbox_File_Error.exec()
    
    def select_file_RPA(self):
        self.file_path_RPA = QFileDialog.getOpenFileName()
        self.file_path_RPA = str(self.file_path_RPA[0])
        if self.file_path_RPA:
            
            if not self.file_path_RPA.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_RPA = self.count_csv_rows(self.row_count_RPA)
                if self.row_count_RPA is not None:
                    self.row_count_RPA = "{:,}".format(self.row_count_RPA)
                    self.process_data.label_Total_Registers_7.setText(f"{self.row_count_RPA}")
                    
    def reports_saem(self):
        
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la descarga de ID de Saem.")
        Mbox_In_Process.exec()
        
        self.Base = web.download_saem_reports.read_csv_lists_saem(self.file_path_RPA)

        Mbox_In_Process = QMessageBox() 
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Descarga de reportes ejecutada exitosamente.")
        Mbox_In_Process.exec()
        
    def error_type_CAM_(self):
        try:
            process = subprocess.Popen(
                [sys.executable, "Project.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()

            if process.returncode != 0:
                error_message = f"❌ Error de Ejecucion:\n\n{stderr.strip()}" if stderr else "❌ Error desconocido."
                self.process_data.label_Version_Detail.setText(error_message)
        
        except Exception as e:
            self.process_data.label_Version_Detail.setText(f"❌ Error crítico:\n{str(e)}")
        
    def copy_template_ivr(self):
        
        Root_API = self.root_API 
        output_directory = self.folder_path
        script = f"{Root_API}/cpd/files/dsh_bd/Plantilla_Sistema_IVR-TRANS.csv"
        
        output_file_path = f"{output_directory}/Plantilla_Sistema_IVR-TRANS.csv"
        
        if not os.path.exists(output_file_path):
            shutil.copy(script, output_directory)
            Message_IVR = f"Plantilla exportada en el directorio de Descargas."
        else:
            Message_IVR = f"El archivo {output_file_path} ya existe. No se realizó la copia."
            print(f"El archivo {output_file_path} ya existe. No se realizó la copia.")
            
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText(f"{Message_IVR}")
        Mbox_In_Process.exec()
    
    def copy_template_reports_saem(self):
        
        Root_API = self.root_API 
        output_directory = self.folder_path
        script = f"{Root_API}/cpd/files/dsh_bd/Plantlilla Descarga Reportes SAEM.csv"
        
        output_file_path = f"{output_directory}/Plantlilla Descarga Reportes SAEM.csv"
        
        if not os.path.exists(output_file_path):
            shutil.copy(script, output_directory)
            Message_IVR = f"Plantilla exportada en el directorio de Descargas."
        else:
            Message_IVR = f"El archivo {output_file_path} ya existe. No se realizó la copia."
            print(f"El archivo {output_file_path} ya existe. No se realizó la copia.")
            
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText(f"{Message_IVR}")
        Mbox_In_Process.exec()
    
    def folder_webscrapping(self):

        if self.folder_path_webscrapping != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            utils.IVR_Upload.Uploads_DB(self.folder_path_webscrapping, self.process_data)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Bases subidas al sistema de IVR-TRANS.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a cargar en el sistema de IVR-TRANS.")
            Mbox_File_Error.exec()
            
    def select_file_CAM(self):
        self.file_path_CAM = QFileDialog.getOpenFileName()
        self.file_path_CAM = str(self.file_path_CAM[0])
        if self.file_path_CAM:
            
            if not self.file_path_CAM.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_CAM = count_csv_rows(self.file_path_CAM)
                if self.row_count_CAM is not None:
                    self.row_count_CAM = "{:,}".format(self.row_count_CAM)
                    self.process_data.label_Total_Registers_4.setText(f"{self.row_count_CAM}")
                    self.bd_process_start()

    def select_file_IVR(self):
        self.file_path_IVR = QFileDialog.getOpenFileName()
        self.file_path_IVR = str(self.file_path_IVR[0])
        if self.file_path_IVR:
            
            if not self.file_path_IVR.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()

            else:
                self.row_count_DIR = count_csv_rows(self.file_path_DIRECTION)
                self.exec_process_ivr()
    
    def exec_process_ivr(self):
        self.process_data.pushButton_Partitions_BD_46.clicked.connect(self.start_process_ivr_change_audios)
        self.process_data.pushButton_Partitions_BD_45.clicked.connect(self.start_process_ivr_clean)

    def start_process_ivr_clean(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa la solicitud.")
        Mbox_In_Process.exec()

        self.process_ivr_clean()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de limpieza ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def process_ivr_clean(self):

        if self.file_path_IVR:
            utils.IVR_Clean_Lists.Clean(self.file_path_IVR, self.process_data)
            self.file_path_IVR = None
        else:
            self.error_type_IVR()

    def start_process_ivr_change_audios(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa la solicitud.")
        Mbox_In_Process.exec()

        self.process_ivr_change_audios()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Cambio de audios ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def process_ivr_change_audios(self):

        if self.file_path_IVR:
            utils.IVR_Change_Audios.Change_Audio(self.file_path_IVR, self.process_data)
            self.file_path_IVR = None
        else:
            self.error_type_IVR()

    def error_type_IVR(self):

        if self.file_path_IVR is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        else:
            self.exec_process_ivr()

    def select_file_FILES(self):
        self.file_path_FILES = QFileDialog.getOpenFileName()
        self.file_path_FILES = str(self.file_path_FILES[0])
        if self.file_path_FILES:
            
            if not self.file_path_FILES.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_FILES = count_csv_rows(self.file_path_FILES)
                if self.row_count_FILES is not None:
                    self.row_count_FILES = "{:,}".format(self.row_count_FILES)
                    self.process_data.label_Total_Registers_2.setText(f"{self.row_count_FILES}")
                    self.start_process_FILES()

    def select_file_DIRECION(self):
        self.file_path_DIRECTION = QFileDialog.getOpenFileName()
        self.file_path_DIRECTION = str(self.file_path_DIRECTION[0])
        if self.file_path_DIRECTION:
            
            if not self.file_path_DIRECTION.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:                                                 
                self.row_count_DIR = count_csv_rows(self.file_path_DIRECTION)
                if self.row_count_DIR is not None:
                    self.row_count_DIR = "{:,}".format(self.row_count_DIR)
                    self.process_data.label_Total_Registers_14.setText(f"{self.row_count_DIR}")
                    self.start_process_FILES_task()
                    
    def select_file_XLSX(self):
        self.file_path_DIRECTION = QFileDialog.getOpenFileName()
        self.file_path_DIRECTION = str(self.file_path_DIRECTION[0])
        if self.file_path_DIRECTION:
            
            if not self.file_path_DIRECTION.endswith('.xlsx'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un Libro con formato XLSX.")
                Mbox_File_Error.exec()
            
            else:                                                 
                self.row_count_DIR = count_xlsx_data(self.file_path_DIRECTION)
                if self.row_count_DIR is not None:
                    self.row_count_DIR = "{:,}".format(self.row_count_DIR)
                    self.process_data.label_Total_Registers_14.setText(f"{self.row_count_DIR} Hoja(s)")
                    self.start_process_FILES_task()

    def select_path_IVR(self):
        self.folder_path_IVR = QFileDialog.getExistingDirectory()
        self.folder_path_IVR = str(self.folder_path_IVR)

        if self.folder_path_IVR:
            
            if len(self.folder_path_IVR) < 1:
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_FILES = count_files_folder(self.folder_path_IVR)
                if self.row_count_FILES is not None:
                    self.row_count_FILES = "{:,}".format(self.row_count_FILES)
                    self.process_data.label_Total_Registers_6.setText(f"{self.row_count_FILES}")

    def error_type_FILES_PASH(self):

        if self.file_path_PASH is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_PASH is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def error_type_FILES(self):

        if self.file_path_FILES is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_FILES is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def error_type_CAM(self):

        if self.file_path_CAM is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_CAM is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def start_process_FILES(self):

        if self.row_count_FILES and self.file_path_FILES and self.folder_path:
            self.Project = Process_Data(self.row_count_FILES, self.file_path_FILES, self.folder_path, self.process_data)

        else:
            self.error_type_FILES()

    def error_type_FILES_task(self):

        if self.file_path_DIRECTION is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_DIR is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def start_process_FILES_task(self):
        
        print(self.row_count_DIR, self.file_path_DIRECTION)
        if self.row_count_DIR and self.file_path_DIRECTION and self.folder_path:
            self.Project = Process_Data(self.row_count_DIR, self.file_path_DIRECTION, self.folder_path, self.process_data)

        else:

            self.error_type_FILES_task()

    def building_soon(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Proceso en Desarrollo")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("El módulo seleccionado se encuentra en estado de desarrollo.")
        Mbox_In_Process.exec()

    def bd_process_start(self):

        if self.row_count_CAM and self.file_path_CAM and self.folder_path:
            self.Base = Charge_DB(self.row_count_CAM, self.file_path_CAM, self.folder_path, self.process_data)

        else:

            self.error_type_CAM()

    def uploaded_proccess_db(self):

        if self.row_count and self.file_path and self.folder_path and self.partitions:
            self.Base = Process_Uploaded(self.row_count, self.file_path, self.folder_path, self.process_data)

        else:

            self.error_type()

    def power_shell(self):
         
        try:
            os.system('start powershell.exe')
        
        except Exception as e:
            print(f"Error al intentar abrir PowerShell: {e}")

    def copy_code(self):

        output_directory = self.folder_path

        self.function_coding(output_directory)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Codigos exportados en el directorio de Descargas.")
        Mbox_In_Process.exec()

    def copy_lines_corp(self):
        output_directory = self.folder_path  # Ensure output_directory is set to a valid path

        if not output_directory:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un directorio válido para copiar las líneas corporativas.")
            Mbox_File_Error.exec()
            return

        Root_API = self.root_API 
        lines = f"{Root_API}/cpd/vba/Lineas_Corporativas.txt"

        try:
            shutil.copy(lines, output_directory)
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Líneas corporativas copiadas exitosamente en el directorio seleccionado.")
            Mbox_In_Process.exec()
        except Exception as e:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText(f"Error al copiar las líneas corporativas: {e}")
            Mbox_File_Error.exec()

    def copy_folders_root(self, output_directory):

        year = datetime.now().year
        base_folder = os.path.join(output_directory, f"ESTRUCTURA_TELEMATICA_{year}")
        subfolders = ["SMS", "BOT", "IVR", "EMAIL"]

        os.makedirs(base_folder, exist_ok=True)
        for folder in subfolders:
            os.makedirs(os.path.join(base_folder, folder), exist_ok=True)

        return base_folder

    
    def function_coding(self, output_directory):

        Root_API = self.root_API 

        code2 = f"{Root_API}/cpd/vba/Macro - Filtros Cam UNIF.txt"
        code3 = f"{Root_API}/cpd/vba/PowerShell - Union Archivos.txt"
        code4 = f"{Root_API}/cpd/vba/Plantilla CAM Unif Virgen.xlsx"

        shutil.copy(code2, output_directory)
        shutil.copy(code3, output_directory)
        shutil.copy(code4, output_directory)

    def copy_folder_scripts(self):

        output_directory = self.folder_path

        Root_API = self.root_API 
        folder_script = f"{Root_API}/cpd/vba/Estructuras Control Next"

        try:
            destination = os.path.join(output_directory, os.path.basename(folder_script))

            if os.path.exists(destination):
                shutil.rmtree(destination)
            
            shutil.copytree(folder_script, destination)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Extructuras de cargue copiados en el directorio de Descargas.")
            Mbox_In_Process.exec()
                
        except PermissionError as e:
            print(f"Error de permisos al intentar copiar la carpeta: {e}")
        
        except Exception as e:
             print(f"Ocurrió un error al intentar copiar la carpeta: {e}")

    def digit_partitions_FOLDER(self):

        self.partitions_FOLDER = str(self.process_data.spinBox_Partitions_3.value())

    def task_web_folder(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.partitions_FOLDER != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.read_task_web.function_complete_task_WEB(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Batch para cargue de predictivo ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
    def folder_demographic(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.partitions_FOLDER != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.union_demo.Union_Files_Demo(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de demograficos ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()

    def folder_union_excel(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.partitions_FOLDER != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            gui.union_files.merge_files(self.folder_path_IVR, self.folder_path)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Unión generada exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()

    def folder_bot_ipcom(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.partitions_FOLDER != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.union_bot.Union_Files_BOT(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Resultado de BOT consolidado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
    def folder_validation(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.partitions_FOLDER != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.structure_files.details_files(self.folder_path_IVR, self.folder_path)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Analisis de archvios ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a validar.")
            Mbox_File_Error.exec()
    
    def folder_files_process(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.partitions_FOLDER != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.no_managment.transform_no_management(self.folder_path_IVR, self.folder_path)
            self.Base = gui.payments_not_applied.Transform_Payments_without_Applied(self.folder_path_IVR, self.folder_path)
            self.Base = gui.payments.unify_payments(self.folder_path_IVR, self.folder_path)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de archvios ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()

    def ivr_folder_read(self):

        type_process = "IVR"        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        list_to_process_IVR = self.list_IVR

        print(list_to_process_IVR)

        if len(list_to_process_IVR) > 2:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
    
            Channel_IVR = list_to_process_IVR[0]
            Search_IVR = list_to_process_IVR[1]
            Date_IVR = list_to_process_IVR[2]
            
            self.Base = gui.read_ivr.function_complete_IVR(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER, self.process_data, Date_IVR, Search_IVR, Channel_IVR)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de IVR ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()

    def show_developers_window(self):
        
        dialog = QDialog()
        dialog.setWindowTitle("Información del Desarrollador")
        dialog.setWindowModality(Qt.WindowModality.ApplicationModal)
        dialog.setWindowState(Qt.WindowState.WindowFullScreen)
        
        Version_Api = "v1.0.13 (Py3.11-Spark3.5)"
        
        label = QLabel(
            "<h1>Esta aplicación fue desarrollada por:</h1><br>"
            "<b><h2>Juan Méndez – Arquitecto de Software</h2></b><br>"
            "<span style='font-size: 18px;'>Responsable del diseño, implementación y supervisión del desarrollo. "
            "Encargado de la integración de PySpark, transformación de datos, visualización, lógica y conexiones.</span><br><br>"
            
            "<b><h2>Daniel Sierra – Desarrollador Web</h2></b><br>"
            "<span style='font-size: 18px;'>Apoyó en la creación de componentes RPA y en la implementación de funcionalidades de web scraping.</span><br><br>"
            
            "<b><h2>Juan Raigoso – Desarrollador Frontend</h2></b><br>"
            "<span style='font-size: 18px;'>Contribuyó en la parte visual del sistema de una primera versión, trabajando en la estructura y estilización con CSS.</span><br><br>"
            
            "<b><h2>Deiby Gutiérrez – Desarrollador de Soporte en SQL</h2></b><br>"
            "<span style='font-size: 18px;'>Colaboró en la creación y optimización de scripts SQL para validaciones, pruebas y comparativas en el manejo de BIG DATA.</span><br><br>"
            
            "<br><br>" 
            f"<b>Versión {Version_Api}</b>"
        )
        label.setTextFormat(Qt.TextFormat.RichText)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        layout = QVBoxLayout()
        layout.addWidget(label)
        dialog.setLayout(layout)

        dialog.exec()
    
    def show_stakeholders_window(self):
        dialog = QDialog()
        dialog.setWindowTitle("Stakeholders de la Aplicación")
        dialog.setWindowModality(Qt.WindowModality.ApplicationModal)
        dialog.setWindowState(Qt.WindowState.WindowFullScreen)

        Version_Api = "v1.0.13 (Py3.11-Spark3.5)"

        label = QLabel(
            "<h1>Stakeholders:</h1><br>"
            "<b><h2>Raul Barbosa</h2></b><span style='font-size: 18px;'> – Gerente</span><br><br>"
            "<b><h2>Nathaly Lizarazo</h2></b><span style='font-size: 18px;'> – Directora Operativa</span><br><br>"
            "<b><h2>Nayibe Montoya</h2></b><span style='font-size: 18px;'> – Jefe de Operaciones</span><br><br>"
            "<b><h2>Anthony Quiva</h2></b><span style='font-size: 18px;'> – Coordinador Operativo</span><br><br>"
            "<b><h2>Karen Pinilla</h2></b><span style='font-size: 18px;'> – Analista de Inteligencia</span><br><br>"
            "<br><br>"
            f"<b>Versión {Version_Api}</b>"
        )
        label.setTextFormat(Qt.TextFormat.RichText)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        layout = QVBoxLayout()
        layout.addWidget(label)
        dialog.setLayout(layout)

        dialog.exec()
        
    def open_chrome_with_url(self, url):
        chrome_path = 'C:/Program Files/Google/Chrome/Application/chrome.exe %s'
        webbrowser.get(chrome_path).open(url)

    def schedule_shutdown(self):

        hours = self.process_data.spinBox_Partitions_13.value()
        minutes = self.process_data.spinBox_Partitions_14.value()

        total_seconds = hours * 3600 + minutes * 60

        if  total_seconds > 61:
            shutdown_command = f'shutdown /s /t {total_seconds}'
            os.system(shutdown_command)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Critical)
            Mbox_In_Process.setText(f"El apagado automático está programado para {hours} hora(s) y {minutes} minuto(s).")
            Mbox_In_Process.exec()

        else: 
            pass
    
    def run_bat_excel(self):

        Root_API = self.root_API 
        bat_file_path = f"{Root_API}/cpd/files/bat/Excel_Finisher.bat"  
        
        try:
            subprocess.run([bat_file_path])
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Excel finalizado exitosamente.")
            Mbox_In_Process.exec()

        except subprocess.CalledProcessError as e:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText(f"Error al ejecutar la operación: {e}")
            Mbox_Incomplete.exec()

    def run_replay_intercom(self):
        Root_API = self.root_API
        bat_file_path = f"{Root_API}/cpd/files/bat/Replay IVR-TRANS.exe"

        class WorkerThread(QThread):
            success_signal = pyqtSignal()
            error_signal = pyqtSignal(str)

            def run(self):
                try:
                    subprocess.run([bat_file_path], check=True)
                    self.success_signal.emit()
                except subprocess.CalledProcessError as e:
                    self.error_signal.emit(str(e))
    
    def run_downloads_intercom(self):

        Value = "Run"
        self.Base = utils.IVR_Downloads_List.Function_Download(Value)


    def run_bat_temp(self):

        Root_API = self.root_API 
        bat_file_path = f"{Root_API}/cpd/files/bat/Temp.bat"  

        try:
            subprocess.run([bat_file_path], check=True)
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Limpieza de temporales ejecutada exitosamente.")
            Mbox_In_Process.exec()

        except subprocess.CalledProcessError as e:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText(f"Error al ejecutar la operación: {e}")
            Mbox_Incomplete.exec()
        
    def validation_data_folders(self, type_process):

        ##### IVR INTERCOM ######
        
        self.partitions_FOLDER = None

        Channels = self.process_data.comboBox_Benefits_2.currentText()
        Search_Data = self.process_data.Searching_Field.text()

        ### Calendar FLP
        Date_Selection = str(self.process_data.checkBox_ALL_DATES_FLP_3.isChecked())
        Calendar_Date = str(self.process_data.calendarWidget_2.selectedDate())
        Calendar_Date_ = self.process_data.calendarWidget_2.selectedDate()
        Today__ = Version_Winutils
        Today = str(QDate(Today__.year, Today__.month, Today__.day))
        Today_ = QDate(Today__.year, Today__.month, Today__.day)

        formatted_date = Calendar_Date_.toString("yyyy-MM-dd")
        formatted_date_today = Today_.toString("yyyy-MM-dd")
        self.today = formatted_date_today

        if Date_Selection == "True":
            Date_Selection_Filter = "All Dates"

        elif Calendar_Date == Today:
            Date_Selection_Filter = None

        else:
            Date_Selection_Filter = formatted_date

        if type_process == "IVR":

            if "--- Seleccione opción" == Channels:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe elegir el tipo de canales.")
                Mbox_Incomplete.exec()
                self.process_data.comboBox_Benefits_2.setFocus()

            elif Date_Selection_Filter is None:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe seleccionar al menos una fecha o elegir todas los días de marcacion.")
                Mbox_Incomplete.exec()

            else:

                self.list_IVR = [Channels, Search_Data, Date_Selection_Filter]
        
        else:
            pass
    
    def validation_data_resources(self):
        
        self.partitions_FOLDER = None
        self.digit_partitions_FOLDER()

        Resource_folder = self.process_data.comboBox_Benefits_4.currentText()
        Check_Folders = str(self.process_data.checkBox_ALL_DATES_FLP_4.isChecked())
        
        if self.folder_path_IVR is None:
            
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
        elif "--- Seleccione opción" == Resource_folder and Check_Folders != "True":
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe elegir el tipo de carpeta del recurso a leer o seleccionar todas.")
            Mbox_Incomplete.exec()
            self.process_data.comboBox_Benefits_4.setFocus()

        else:
            self.list_Resources = [Resource_folder, Check_Folders]
    
    def read_folder_resources(self):
        
        self.validation_data_resources()
        list_to_process_Read = self.list_Resources
        Folder_Resource = None
        Check_Folders_Resource = None

        if len(list_to_process_Read) > 1:
            Folder_Resource = list_to_process_Read[0]
            Check_Folders_Resource = list_to_process_Read[1]
        else:
            pass

        if Check_Folders_Resource == "True":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}/IVR"
            self.Base = skills.COUNT_Ivr.function_complete_IVR(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)
            Path_Resource = f"{self.folder_path_IVR}/BOT"
            self.Base = skills.COUNT_Bot.function_complete_BOT(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)
            Path_Resource = f"{self.folder_path_IVR}/EMAIL"
            self.Base = skills.COUNT_Email.function_complete_EMAIL(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)
            Path_Resource = f"{self.folder_path_IVR}/SMS"
            self.Base = skills.COUNT_Sms.function_complete_SMS(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "IVR":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()

            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.COUNT_Ivr.function_complete_IVR(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "BOT":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.COUNT_Bot.function_complete_BOT(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "SMS":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.COUNT_Sms.function_complete_SMS(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "EMAIL":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.COUNT_Email.function_complete_EMAIL(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            pass

    def validation_data_demo(self):
        
        self.partitions_FOLDER = None
        self.digit_partitions_FOLDER()
        
        if self.folder_path_IVR is None:
            
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
        else:
            pass
