##
# @package get_old_news_us.py
# Download old news from API New York Times save on local and hdfs an proces them using nlp.
#
# save news into local csv year_month_news_us_old.csv using function get_old_news()
# After that load to hdfs selectin only some news of every day, (defined in OLD_NEWS_DAY),
# using function: load_old_news_filtered()
# Then Process old news for training models, using function: nlp_proces_news()
import os
import csv
import time
import requests
import sparknlp
from pyspark.sql import SparkSession
from TFM.source.nlp_news import nlp_proces_news
from TFM.source.load_old_news_hdfs import load_old_news_filtered
from TFM.source.constants import (
    HEADERS_OLD_NEWS,
    PATH_OLD_NEWS,
    PATH_HDFS_OLD_NEWS_CLEANED,
    FILE_OLD_NEWS_CLEANED,
    PATH_HDFS_OLD_NEWS,
    DWL_OLD_NEWS_FROM,
    DWL_OLD_NEWS_TO
)
##
# @brief Download old news from API New York Times n save them into csv year_month_news_us_old.csv.
#
# @param year (str): year to download news
# @param month (str): month to download news
#
# @return None.
def get_old_news(year: str, month: str)->None:
    """ Download old news from API New York Times and 
        save them into csv year_month_news_us_old.csv.

    Args:
        year (str): year to download news
        month (str): month to download news
    """
    # Make dir news if not exist
    os.makedirs(PATH_OLD_NEWS, exist_ok=True)

    # GET data from NYTimes - US OLD news
    try:

        url = "https://api.nytimes.com/svc/archive/v1/"+year+"/"+month+".json?api-key=XmnrnMroyE7SReUoLfrGH1rGqvhedlUi"

        response_us = requests.get(url)
        response_us.raise_for_status()
        print('Request status code NYTimes: {'+year+'_'+month+'}', response_us.status_code)
        response_json_us = response_us.json()

        filename = PATH_OLD_NEWS+year+'_'+month+'_news_us_old.csv'
        file_exists = os.path.isfile(filename)

        # Open the file in append ('a') mode and add the fields iso county code, title,
        # description, and publication date.
        with open(filename, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            if not file_exists:
                writer.writerow(HEADERS_OLD_NEWS)

            for news in response_json_us['response']['docs']:

                title = news['headline']['main']
                description = news['lead_paragraph'] if news['lead_paragraph'] is not None else ''

                news_line = ['us', title, description, news['pub_date']]
                writer.writerow(news_line)

    except requests.exceptions.HTTPError as error:
        print('ERROR - Request NY Times API fails: ', error)


try:
    # Set initial path.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Download full years month to month.
    for yr in range(DWL_OLD_NEWS_FROM,DWL_OLD_NEWS_TO):
        for mnth in range(1,13):
            get_old_news(str(yr), str(mnth))
            time.sleep(10)

    # Download 4 first month of 2025.
    for mnth in range(3,5):
        get_old_news('2025', str(mnth))

    # Create my_spark and npl sessions
    npl_spark = sparknlp.start()
    my_spark = SparkSession.builder.getOrCreate()

    # Load to hdfs selecting only some news by day defined in (OLD_NEWS_DAY)
    load_old_news_filtered(my_spark)

    # Preprocess old news for training models by year.
    for _ in range(DWL_OLD_NEWS_FROM,DWL_OLD_NEWS_TO):
        nlp_proces_news(my_spark,
                        list_dirs_countries=[PATH_HDFS_OLD_NEWS],
                        col_desc='description',
                        hdfs_file_cleaned=PATH_HDFS_OLD_NEWS_CLEANED,
                        local_file=FILE_OLD_NEWS_CLEANED)


    my_spark.stop()
    npl_spark.stop()

except Exception as e:
    my_spark.stop()
    npl_spark.stop()
    print('ERROR - Downloading OLD News: ',e)
