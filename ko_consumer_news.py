
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import json_tuple, from_json, col
from os.path import abspath

from kafka import KafkaConsumer
from constants import *


# set consumers of Kafka
group_id = 'TFM_news'
consumer = KafkaConsumer(TOPIC_NAME_UE,
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest', 
                         group_id=group_id)

# set Spark Haive
warehouse_location = abspath('TFM-warehouse')
conf = SparkConf()
conf.setMaster("local[1]")
conf.setAppName("Consumer_Hive")
sc = SparkContext.getOrCreate(conf=conf) 
print(sc.version) 
spark = SparkSession.builder \
    .appName("Consumer_hive") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS tfm ")
print(spark.catalog.listDatabases())

print(spark.catalog.listTables())

spark.sql("DROP TABLE IF EXISTS tfm.news")
spark.sql("""
    CREATE TABLE IF NOT EXISTS tfm.news (
          iso_code String,
          title String,
          description String,
          lang_tranlation String, 
          title_translated String, 
          description_translated String,
          pubDate Date)
    USING PARQUET
""")

print(spark.catalog.listTables("tfm"))
df = spark.sql("SELECT count(*) FROM tfm.news")
print(df.show())




#Consumir mensajes de Kafka y escribir en el CSV
try:
    while True:
        messages = consumer.poll(timeout_ms=1000)
        if messages is None:
                continue
        if not messages:
            print("No messages received.")
            continue
        
        for topic_partition, records in messages.items():
            print(topic_partition)
            for msg in records:  # Iterar sobre los mensajes de cada partición
                row = msg.value.decode('utf-8')
                print(f"Datos recibidos: {row}")


except KeyboardInterrupt:
        consumer.close()
        pass
except Exception as e:
        consumer.close()
        print('ERROR - Reading ISO Codes: ',e)
finally:
    consumer.close()
    print("Consumidor cerrado. Datos finales escritos en el CSV")


"""
try:
    # Initialize SparkConf and SparkContext
    conf = SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("Consumer_Hive")
    sc = SparkContext.getOrCreate(conf=conf) 
    sc.setLogLevel("ERROR")

    warehouse_location = abspath('TFM-warehouse')

    print(sc.version)
    spark = SparkSession.builder \
        .config('spark.jars.packages', 
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0, org.apache.kafka:kafka-clients:3.9.0') \
        .config('spark.ui.port', '0') \
        .config('spark.sql.warehouse.dir', warehouse_location) \
        .appName("Consumer_news") \
        .enableHiveSupport() \
        .master('yarn') \
        .getOrCreate()

       
    news_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
        .option("subscribe", TOPIC_NAME_UE) \
        .option("startingOffsets", "earliest") \
        .load()
    
    news_stream.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value").printSchema()

    # Write the stream to the console in append mode
    
    query = news_stream \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()
    
    # Wait for termination
    try:
        query.awaitTermination()

    except KeyboardInterrupt:
        query.stop()
        spark.stop()
        sc.stop()


except Exception as e:

    print(e)
    spark.stop()
    sc.stop()



"""