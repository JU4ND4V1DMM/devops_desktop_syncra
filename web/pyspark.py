from pyspark.sql import SparkSession
import os

def get_spark_session():
    
    print("Initializing Spark Session...")
    
    spark = SparkSession.builder \
        .appName("GlobalSparkApp") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()
    
    temp_dir = "C:/SparkTemp"  # Path secure to store temporary files
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