##
# @package main.py
# Download all last news (only yesterday news because API are free) from countries defined and send
# to producer. When finish, starts consumer and pre-process them, after some time till none
# received any msg finish consumer and start natural language to process last news non processed.
# Finally get and update prediction with last news processed, from last appointment president from
# each country to today, and save results.
#
import time
import os
import pprint
import subprocess
from datetime import datetime
import sparknlp
from pyspark.sql import SparkSession
from TFM.source.nlp_news import nlp_proces_news
from TFM.source.consumer_news import ini_consumer
from TFM.source.producer_news import ini_producer
from TFM.source.get_news import get_dayli_news
from TFM.source.predict import predict_last_news
from TFM.source.constants import (
    NEWS_WAIT_TO_DOWNLOAD,
    FILE_NEWS_CLEANED,
    PATH_HDFS_NEWS_CLEANED,
    PATH_HDFS_NEWS
    )

if __name__ == '__main__':

    ## Set initial path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    try:
        ## Create my_spark and npl sessions.
        npl_spark = sparknlp.start()
        my_spark = SparkSession.builder.getOrCreate()

        producer = ini_producer()
        while True:

            ## Get latest news from both API.
            print('\n## Downloading news form both API at: ', datetime.now() )
            summary = get_dayli_news(producer)
            print('\n## Country: [Total news download, Total news translated]')
            pprint.pprint(summary)

            ## Start consumer.
            print('\n## Start consumer at: ', datetime.now())
            ini_consumer(my_spark)

            ## Process last daily news.
            print('\n## Start NLP process at: ', datetime.now() )
            ## Get country folders created in consumer for daily news download from topic.
            list_dirs_cntry =  subprocess.check_output("hdfs dfs -ls "+PATH_HDFS_NEWS+" | awk '{print $NF}'",
                                                    shell=True).decode("utf-8").splitlines()[1:]
            nlp_proces_news(my_spark,
                            list_dirs_countries=list_dirs_cntry,
                            col_desc='description_translated',
                            hdfs_file_cleaned=PATH_HDFS_NEWS_CLEANED,
                            local_file=FILE_NEWS_CLEANED)

            ## Predict last news cleaned and save results to show in Power Bi visualization.
            print('\n## Start prediction at: ', datetime.now() )
            predict_last_news(my_spark)

            ## Sleep till next day in second.
            print(f"## Now: {datetime.now()} -> Start to sleep till next day.")
            time.sleep(NEWS_WAIT_TO_DOWNLOAD)

    except KeyboardInterrupt:
        my_spark.stop()
        npl_spark.stop()
        producer.close()
        print("Keyborard Interrupt. Closing services...")
        pass
    except Exception as e:
        my_spark.stop()
        npl_spark.stop()
        producer.close()
        print("ERROR: Services closed: ",e)
    finally:
        my_spark.stop()
        npl_spark.stop()
        producer.close()
        print("Closing services. Datos enviados al broker.")
