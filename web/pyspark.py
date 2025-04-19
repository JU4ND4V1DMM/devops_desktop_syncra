from pyspark.sql import SparkSession
import os
import shutil
import string

def get_disk_with_most_free_space():
    
    best_drive = None
    max_free = 0
    
    for drive_letter in string.ascii_uppercase:
        drive = f"{drive_letter}:/"
        if os.path.exists(drive):
            try:
                total, used, free = shutil.disk_usage(drive)
                if free > max_free:
                    max_free = free
                    best_drive = drive
            except PermissionError:
                continue
            
    return best_drive

def get_spark_session():
    
    print("Initializing Spark Session...")

    spark = SparkSession.builder \
        .appName("GlobalSparkApp") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()

    best_disk = get_disk_with_most_free_space()
    temp_dir = os.path.join(best_disk, "SparkTemp")
    
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    spark_2 = SparkSession \
            .builder.appName("GlobalSparkApp_Config") \
            .config("spark.local.dir", temp_dir) \
            .config("spark.driver.memory", "16g") \
            .config("spark.executor.memory", "16g") \
            .config("spark.driver.maxResultSize", "4g") \
            .config("spark.sql.shuffle.partitions", "50") \
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -Djava.security.manager=allow") \
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -Djava.security.manager=allow") \
            .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
            .getOrCreate()
        
    return spark_2