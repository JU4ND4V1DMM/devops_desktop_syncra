from pyspark.sql import SparkSession

def get_spark_session():
    
    print("Initializing Spark Session...")
    
    spark = SparkSession.builder \
        .appName("GlobalSparkApp") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()
        
    spark_2 = SparkSession \
            .builder.appName("BD_CN") \
            .config("spark.local.dir", "C:/tmp/hive") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.driver.memory", '16g')\
            .config("spark.executor.memory", '16g')\
            .getOrCreate()
    
    spark_2.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")
        
    return spark_2