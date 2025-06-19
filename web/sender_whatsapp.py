import random
from openpyxl import Workbook, load_workbook
from urllib.parse import quote_plus
import os
from selenium.common.exceptions import TimeoutException
from PyQt6.QtWidgets import QMessageBox
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib.parse
import time
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.common import exceptions as selexceptions
from selenium.webdriver.common.keys import Keys
import re
import csv
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from web.pyspark_session import get_spark_session
import re
from pyspark.sql.functions import col, lit, concat

# Initialize Spark session
spark = get_spark_session()

def generate_message_column(df, template):
    columns = [c.upper() for c in df.columns]
    parts = re.split(r"(\([^)]+\))", template)  # Split into text and variables like (NOMBRE)
    elements = []

    for part in parts:
        if part.startswith("(") and part.endswith(")"):
            col_name = part[1:-1].upper()
            if col_name not in columns:
                print(f"⚠️ Column '{col_name}' does not exist.")
                return None
            elements.append(col(col_name))
        else:
            if part.strip() != "":
                elements.append(lit(part))

    # Concatenate all parts into a single message column
    df = df.withColumn("MESSAGE", concat(*elements))
    return df

def read_file(selected_file, template):
    # Load CSV as DataFrame using PySpark
    df = spark.read.option("header", True).option("delimiter", ";").csv(selected_file)

    # Convert column names to uppercase
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.upper())
    
    df = generate_message_column(df, template)

    first_message = df.select("MESSAGE").first()["MESSAGE"]
    
    print("First message to be sent: ", first_message)

    response = True
    
    print("Response from user: ", response)
    
    return df, response

def send_messages(selected_file, output_file, template, process_data):
    
    df, response = read_file(selected_file, template)
    
    if response == False:
        
        Message = "El archivo NO ha sido enviado."
                
        return Message

    try:
        # Configure the browser once
        chrome_options = Options()
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

        driver.get("https://web.whatsapp.com")
        print("🔒 Waiting for you to log in to WhatsApp Web...")
        WebDriverWait(driver, 80).until(
            EC.presence_of_element_located((By.ID, "pane-side"))
        )
        print("✅ Logged in successfully.")

        numbers = df.select("CELULAR").rdd.flatMap(lambda x: x).collect()
        messages = df.select("MESSAGE").rdd.flatMap(lambda x: x).collect()
        
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        date_for_file = datetime.now().strftime("%Y-%m-%d")
        status = None

    except Exception as e:
        Message = f"❌ Error durante WhatsApp Web setup o preparacion de data: {e}"
        driver.quit()
        return Message
    
    for number, message in zip(numbers, messages):
        encoded_message = quote_plus(message)
        url = f"https://web.whatsapp.com/send?phone={number}&text={encoded_message}"
        print(f"📨 Sending to {number} -> {message}")
        driver.get(url)

        try:
            
            try:
                random_wait = random.uniform(2, 5)
                button = WebDriverWait(driver, random_wait).until(EC.presence_of_element_located((By.XPATH, '//*[@id="app"]/div/span[2]/div/div/div/div/div/div/div[2]/div/button')))
                button.click()
                print("Clicked the button before logging in.")
            except TimeoutException:
                print("The button was not found; proceeding without clicking.")
            
            # Click on "Continue to chat"
            button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "action-button"))
            )
            button.click()

            # Click on "use WhatsApp Web"
            web = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.LINK_TEXT, "use WhatsApp Web"))
            )
            web.click()

            # Wait for text box and send message
            message_box = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((
                    By.XPATH, '//*[@id="main"]//footer//div[@contenteditable="true"]'
                ))
            )
            time.sleep(1)
            message_box.send_keys(Keys.ENTER)
            time.sleep(2)

            status = "Enviado"

        except Exception as e:
            status = "No enviado"
            print(f"⚠ Error with number {number}: {e}")

        save_to_excel(output_file, number, message, status)

    driver.quit()
    Message = "El archivo ha sido tratado exitosamente y su reporte esta en las Descargas."
    
    return Message

def save_to_excel(output_folder, number, message, status):
    # Crear el nombre del archivo con la fecha
    date_for_file = datetime.now().strftime("%Y-%m-%d")
    file_path = os.path.join(output_folder, f"Reporte RPA WhatsApp {date_for_file}.xlsx")

    # Intentar cargar el libro si ya existe, o crear uno nuevo
    if os.path.exists(file_path):
        workbook = load_workbook(file_path)
        sheet = workbook.active
    else:
        workbook = Workbook()
        sheet = workbook.active
        # Escribir encabezados solo si el archivo es nuevo
        sheet.append(["Número", "Mensaje", "Fecha Hora", "Estado"])

    # Agregar fila con datos
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet.append([number, message, current_datetime, status])

    # Guardar el archivo
    workbook.save(file_path)