import shutil
import os
from config import main_window
def clean_hive_tmp_dirs():
    paths = [r"C:\tmp\hive", r"D:\tmp\hive"]
    for path in paths:
        if os.path.exists(path) and os.path.isdir(path):
            for filename in os.listdir(path):
                file_path = os.path.join(path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f"Error eliminando {file_path}: {e}")


def running_process( ):

    app_process = main_window() # Create the main window
    
clean_hive_tmp_dirs()
running_process()
