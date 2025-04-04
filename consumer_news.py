from kafka import KafkaConsumer
from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import ast
from pyspark.sql.functions import to_timestamp
from constants import *

# Init spark session
spark = SparkSession.builder.appName("AppendCSV").getOrCreate()
# Define schema for the incoming data
schema_news = StructType([
    StructField("iso_code", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("lang_tranlation", StringType(), True),
    StructField("title_translated", StringType(), False),
    StructField("description_translated", StringType(), True),
    StructField("pubDate", StringType(), False)
])

# set consumers of Kafka
group_id = 'TFM_news'
consumer = KafkaConsumer(TOPIC_NAME,
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest', 
                         group_id=group_id)

#Consumir mensajes de Kafka y escribir en el CSV
try:
    while True: 
        messages = consumer.poll(timeout_ms=1000)
        if messages is None:
                continue
        if not messages:
            print("No messages received.")
            continue
        news_list = []

        for topic_partition, records in messages.items():
            print(topic_partition)
            for msg in records:  # Iterar sobre los mensajes de cada partición
                row = msg.value.decode('utf-8')
                news_list.append(ast.literal_eval(row))
                print(f"Datos recibidos: {row}")
            # create df of new list news
            df = spark.createDataFrame(news_list, schema=schema_news)
            
            # Select distinc iso codes to add each county news to its csv in hdfs
            iso_code = df.select(['iso_code']).distinct().toPandas()['iso_code'].tolist()
            for code in iso_code:
                # filtrar df i afegir noticies al path corresponent del pais
                df_news_country = df.filter(df.iso_code == code)
                # Cast string data to timestamp
                if code == 'us':
                    df_news_country = df_news_country.withColumn("pubDate", to_timestamp(df["pubDate"], "yyyy-MM-dd'T'HH:mm:ss'Z'"))
                else:
                    df_news_country = df_news_country.withColumn("pubDate", to_timestamp(df["pubDate"], "yyyy-MM-dd HH:mm:ss"))
                
                df_news_country.show()
                print(f"Apend to '{code}' conuntry parquet : {df_news_country.count()} news")
                df_news_country.write.mode("append").parquet("/TFM/news/news_"+code)


except KeyboardInterrupt:
        consumer.close()
        pass
except Exception as e:
        consumer.close()
        print('ERROR - Consumer: ',e)
finally:
    consumer.close()
    print("Consumidor cerrado. Datos finales escritos en hdfs.")