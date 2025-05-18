##
# @package load_old_news_hdfs.py
# Load old news into Hadoop's distributed system files, in parquet format with following schema,
# limiting daily news to be loaded to 10, this setting can be changed in the constant file:
# OLD_NEWS_DAY. Also call function classify_news to get summary concerns.
#
# schema_old_news = StructType([
#    StructField("iso_code", StringType(), True),
#    StructField("title", StringType(), True),
#    StructField("description", StringType(), True),
#    StructField("pubDate", StringType(), True)
#    ])
#
import subprocess
import pyspark.sql.functions as F
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from TFM.source.constants import OLD_NEWS_DAY, PATH_HDFS_OLD_NEWS
from TFM.source.classification_news import classify_news

##
# @brief Load old news from local to HDFS.
# @details Load old news into Hadoop's distributed system files, in parquet format with the
# following scheme, limiting daily news to be loaded to 10, this setting can be changed in
# the constant file: OLD_NEWS_DAY. Also call function classify_news to get summary concerns.
#
# @param my_spark (SparkSession): Spark sesion.
def load_old_news_filtered(my_spark)->None:
    """ Load old New York Times news from local to hdfs as parquet in the old news directory,
        with next schema. Load old news into Hadoop's distributed system files, in parquet format with the
        following scheme, limiting daily news to be loaded to 10, this setting can be changed in
        the constant file: OLD_NEWS_DAY. Also call function classify_news to get summary concerns.

            schema_old_news = StructType([
                StructField("iso_code", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("pubDate", StringType(), True)
            ])

     Args:
        my_spark (SparkSession): Spark sesion.        
    """

    try:

        # Get list of files name in local directory
        list_file_news = subprocess.check_output("ls /home/roser/TFM/data/old_news | awk '{print $NF}'",
                                                shell=True).decode("utf-8").splitlines()

        schema_old_news = StructType([
            StructField("iso_code", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("pubDate", StringType(), True)
        ])

        for file in list_file_news:
            print('#File: #',file)

            df = my_spark.read.option("header", "true").schema(schema_old_news) \
                .csv("file:///home/roser/TFM/data/old_news/"+file)

            # Cast to timestamp and to date.
            df = df.withColumn("pubDate", to_timestamp(col("pubDate"), "yyyy-MM-dd'T'HH:mm:ssZ"))
            df = df.withColumn("pubDate", to_date(col("pubDate")))  # Just leave the date.

            # Get distinc dates
            disctinc_dates = df.select("pubDate").distinct().rdd.flatMap(lambda x: x).collect()
            df_final = None

            for date in disctinc_dates:
                # Filter by day.
                df_day = df.filter(F.to_date(col("pubDate")) == F.to_date(F.lit(date)))
                # Get per day only 10 news (OLD_NEWS_DAY).
                df_top50_per_day = df_day.limit(OLD_NEWS_DAY)
                # Union df results filtered by day.
                if df_final is None:
                    df_final = df_top50_per_day
                else:
                    df_final = df_final.union(df_top50_per_day)

            print("## Total news: ",df_final.count())
            df_final.write.mode("append").parquet(PATH_HDFS_OLD_NEWS)

            classify_news(df_final, "title", "description")

        print('##Old news have been load from local to hdfs.')

    except Exception as e:
        print('ERROR - loading old news in hdfs: ',e)
