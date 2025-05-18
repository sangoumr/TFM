##
# @package consumer_news.py
# Consume news from kafka topic, precese them and save to hdfs. Classify news with Pretrained model
# to visualize in Power BI summary concerns, using Pipline, and save results on local CSV.
#
import ast
import time
from kafka import KafkaConsumer
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
from TFM.source.classification_news import classify_news
from TFM.source.constants import (
    TOPIC_NAME,
    MAX_TIME_WAITING,
    PATH_HDFS_NEWS
    )

##
# @brief Consumes the news that has been sent to the Kafka topic by the producer.
# @details Transforms the date to timestamt and saves them by country code in parquet format.
# Wait for 5 minutes new msg, and if no msg then exit.
#
# @param my_pspark (SparkSession): Spark sesion.
#
# @return none.
def ini_consumer(my_spark)-> None:
    """ Consumes the news that has been sent to the Kafka topic by the producer. 
        Transforms the date to timestamt and saves them by country code in parquet format.
        Classify news with Pretrained model to visualize in Power BI summary concerns, using
        Pipline, and save results on local CSV.
        Wait for 5 minutes for new msg, and if no msg then exit.
    
    Args:
        my_spark (SparkSession): Spark sesion.

    Returns:
        None
    """

    # Define schema for the incoming data.
    schema_news = StructType([
        StructField("iso_code", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("lang_tranlation", StringType(), True),
        StructField("title_translated", StringType(), False),
        StructField("description_translated", StringType(), True),
        StructField("pubDate", StringType(), False)
    ])

    # Set consumers of Kafka.
    group_id = 'TFM_news'
    consumer = KafkaConsumer(TOPIC_NAME,
                            bootstrap_servers=['localhost:9092'],
                            auto_offset_reset='earliest',
                            group_id=group_id)

    # Consume messages from Kafka and write in parquet folder by country (iso_code) also classify
    # news.
    try:      

        last_msg_time = time.time()
        while True:
            messages = consumer.poll(timeout_ms=10000)
            current_time = time.time()
            elapsed_time = current_time - last_msg_time

            if messages is None:
                continue
            if not messages:
                print("No messages received.")
                if elapsed_time >= MAX_TIME_WAITING:
                    print("No messages received for 5 minutes. Exiting...")
                    consumer.close()
                    print("Closed consumer. Final data written to HDFS.")
                    break
                continue

            news_list = []

            for topic_partition, records in messages.items():
                print(topic_partition)
                # Iterate over the messages in each partition.
                for msg in records:
                    row = msg.value.decode('utf-8')
                    news_list.append(ast.literal_eval(row))
                    #print(f"Datos recibidos: {row}")
                # Create df of the new news list.
                df = my_spark.createDataFrame(news_list, schema=schema_news)
                
                # Classification daily news to visualize in Power BI summary concerns, using
                # Pipline, and save results on local CSV.
                classify_news(df, "title_translated", "description_translated")

                # Select distinct iso codes to add each county news to its parquet hdfs folder.
                iso_code = df.select(['iso_code']).distinct().toPandas()['iso_code'].tolist()
                for code in iso_code:
                    # Filter df and add news to the corresponding path of the country.
                    df_news_cntry = df.filter(df.iso_code == code)
                    # Cast string data to timestamp.
                    if code == 'us':
                        df_news_cntry = df_news_cntry \
                            .withColumn("pubDate", F.to_timestamp(df["pubDate"],
                                                               "yyyy-MM-dd'T'HH:mm:ss'Z'"))
                    else:
                        df_news_cntry = df_news_cntry \
                            .withColumn("pubDate", F.to_timestamp(df["pubDate"],
                                                               "yyyy-MM-dd HH:mm:ss"))

                    df_news_cntry.show()
                    print(f"## Append to {code} conuntry parquet: {df_news_cntry.count()} news.\n")
                    df_news_cntry.write.mode("append").parquet(PATH_HDFS_NEWS+"/news_"+code)


    except KeyboardInterrupt:
        consumer.close()
        print("Consumer closed. KeyboardInterrupt")
        pass
    except Exception as e:
        consumer.close()
        print('ERROR - Consumer: ',e)
    finally:
        consumer.close()
        print("Closed consumer. Final data written to HDFS.")

