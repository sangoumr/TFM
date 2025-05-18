##
# @package constants.py
# Constants used in the project, can be modified by the user to adapt the execution of the project
# to their requirements..

## Key for downloading current news from the United States. https://newsapi.org
NEWS_API_KEY =  "8635a40f4d36463daee878ac23c22bde"
## Key for downloading current news for the member countries of the European Union,
# https://newsdata.io
NEWS_DATA_IO_KEY = "pub_74893bb2e9fab265b3470e63f3729577d19b3"
## Key for downloading old US news from the New York Times (downloaded since the 1970s),
# https://api.nytimes.com
NEWS_NY_TIME_KEY = "XmnrnMroyE7SReUoLfrGH1rGqvhedlUi"
## Timeout to start downloading current news again.
NEWS_WAIT_TO_DOWNLOAD = 86400
## General path to save local data
LOCAL_PATH = "/home/roser/TFM/data/"
## Path where save last news downloaded from API.
PATH_NEWS = LOCAL_PATH+"news/"
## Local path where find files of country presidents.
PATH_PRESIDENTS = LOCAL_PATH+"president/"
## Local file where save last news cleaned and processed with NPL and ready to be used in
# prediction model.
FILE_NEWS_CLEANED = LOCAL_PATH+"cleaned/news_clean.csv"
## Local file where save classified news using DISTILBERT MODEL, to classify news.
FILE_CONCERNS_SUMMARY = LOCAL_PATH+"cleaned/concerns_summary.csv"
## Local file where save old news cleaned and processed with NPL used to train models.
FILE_OLD_NEWS_CLEANED = LOCAL_PATH+"cleaned/old_news_clean.csv"
## Metrics of best model trained, saved in csv to show in Power Bi visualization.
FILE_METRICS_BEST_MODEL =  LOCAL_PATH+"cleaned/metrics.csv"
## File that merges the former presidents of all countries from 1970 to the last elections and
# predictions to this day.
FILE_PRESIDENTS_N_PREDICTION = LOCAL_PATH+"cleaned/predictions.csv"
## Probability predictions today.
FILE_PROBABILITY_PREDICTION = LOCAL_PATH+"cleaned/probability_predictions.csv"
## File that has Concerns words, extracted from best model, the vocabulary and its coefficients.
FILE_CONCERNS_WORDS = LOCAL_PATH+"cleaned/concerns_words.csv"
## File that merges the former presidents of all countries from 1970 to the last elections, to use
# to prepare data sets to train and test model and make predictions. To arrange electoral period
# from last elections to next elections.
FILE_PRESI = LOCAL_PATH+"president/presidents_combined_final.csv"
## Local path where save old news month to mont downloaded from API.
PATH_OLD_NEWS = LOCAL_PATH+"old_news/"
## File with the list of countries of America with the iso code of the country and the language
# used, to download the news with the NewsAPI API.
PATH_US_ISO = LOCAL_PATH+"pais-us.csv"
## File with the list of countries of the European community with the iso code of the country and
# the leagues used, to download the news with the NewsData API.
PATH_UE_ISO = LOCAL_PATH+"paisos-ue.csv"
## Path to save in parquet format dayly news recived from consumer.
PATH_HDFS_NEWS = "/TFM/news"
## Path to save in parquet format OLD news.
PATH_HDFS_OLD_NEWS = "/TFM/old_news_us"
## Path to save in parquet format daily news received from consumer cleaned and processed by NLP.
PATH_HDFS_NEWS_CLEANED = "/TFM/cleaned/news"
## Path to save in parquet format old news cleaned and processed by NLP.
PATH_HDFS_OLD_NEWS_CLEANED = "/TFM/cleaned/old_news"
## Path in HDFS to save the best model trained to use later in predictions.
PATH_HDFS_BEST_MODEL = "/TFM/best_model"
## Iso code of language to translate all news received from API.
LANGUAGE_TRANSLATION = "en"
## ISo Codes of countries to do not translate to english language.
ISO_CODES_NO_TRANSLATION = ["gb","us"]
## News csv columns downloaded daily.
HEADERS = ["iso_code", "title", "description", "lang_tranlation",
           "title_translated", "description_translated", "pubDate"]
## OLD News csv columns downloaded.
HEADERS_OLD_NEWS = ["iso_code", "title", "description", "pubDate"]
## Number of news to download every day for US
NEWS_US = "20"
## Number of news to download every day for UE country
NEWS_UE_COUNTRIES = "10"
## Kafka server
BOOTSTRAP_SERVER= ['localhost:9092'] 
## Topic name to use in producer and consumer
TOPIC_NAME = 'last_news'
## Topics list used
TOPICS_LIST = [TOPIC_NAME]
## Limit the characters in the news description to be translated because the Google Translate API
# gives an error.
CHAR_LIMIT_TRANSLATE = 2500
## Named Entity Recognition to exclude
NER_EXCLUDE = ".*-(PER|ORG|LOC)"
## Exclude temporal references and web domains
EXCLUDE_WORDS = (
    "^(www|http|https|telnet|mailto|ftps|"
    "monday|tuesday|wednesday|thursday|friday|saturday|sunday|"
    "january|february|march|april|may|june|july|august|september|"
    "october|november|december|"
    "year|month|week|day|today|yesterday|tomorrow|"
    "now|soon|later|before|after|early|late|"
    "morning|afternoon|evening|night|midnight|noon|"
    "recently|previously|currently|already|ago|"
    "second|seconds|minute|minutes|hour|hours|"
    "weekend|holiday|season|spring|summer|autumn|fall|winter|"
    "decade|decades|century|centuries|millennium|millennia|"
    "always|never|sometimes|once|still|eventually|immediately)"
)
## Path of downloaded trained BERT MODEL, to find NER in the text, because it takes more than 300mb
BERT_MODEL = "file:///home/roser/models_npl/berttest"
## Path of downloaded trained DISTILBERT MODEL, to classify news, because it takes more than 300mb
DISTILBERT_MODEL = "file:///home/roser/models_npl/distilbert"
## Limit for loading old news per day that will be uploaded to hdfs, (first 50th published).
OLD_NEWS_DAY = 10
## Download news from the New York Times API from the start year
DWL_OLD_NEWS_FROM = 1970
## Download news from API New York Times till year
DWL_OLD_NEWS_TO = 2026
## Max time waiting for msg in consumer till close (5 minutes).
MAX_TIME_WAITING = 300
## Days diff from elections to appointment of the President set 0 if has day of elections in
# presidents file, in our case have day of appointment, for this reason don't use the news
# received in this days to train the model.
DAYS_DIF_FROM_ELECTIONS = 76
