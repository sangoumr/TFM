##
# @package constants.py
# Constants used in the project, can be modified by the user to adapt the execution of the project to their requirements..
# 

## Key for downloading current news from the United States. https://newsapi.org
NEWS_API_KEY =  "8635a40f4d36463daee878ac23c22bde"
## Key for downloading current news for the member countries of the European Union. https://newsdata.io
NEWS_DATA_IO_KEY = "pub_74893bb2e9fab265b3470e63f3729577d19b3"
## Key for downloading old US news from the New York Times (downloaded since the 1970s). https://api.nytimes.com
NEWS_NY_TIME_KEY = "XmnrnMroyE7SReUoLfrGH1rGqvhedlUi"
## Timeout to start downloading current news again.
NEWS_WAIT_TO_DOWNLOAD = 86400
LOCAL_PATH = "/home/roser/TFM/data/"
PATH_NEWS = LOCAL_PATH+"news/"
FILE_NEWS_CLEANED = LOCAL_PATH+"cleaned/news_clean.csv"
FILE_OLD_NEWS_CLEANED = LOCAL_PATH+"cleaned/old_news_clean.csv"
FILE_METRICS_BEST_MODEL =  LOCAL_PATH+"cleaned/metrics.csv"
FILE_PRESIDENTS_N_PREDICTION = LOCAL_PATH+"cleaned/predictions.csv"
FILE_PRESI = LOCAL_PATH+"president/presidents_combined_final.csv"
PATH_OLD_NEWS = LOCAL_PATH+"old_news/"
PATH_US_ISO = LOCAL_PATH+"pais-us.csv"
PATH_UE_ISO = LOCAL_PATH+"paisos-ue.csv"
PATH_HDFS_OLD_NEWS = "/TFM/old_news_us"
PATH_HDFS_NEWS_CLEANED = "/TFM/cleaned/news"
PATH_HDFS_OLD_NEWS_CLEANED = "/TFM/cleaned/old_news"
PATH_HDFS_BEST_MODEL = "/TFM/best_model"
LANGUAGE_TRANSLATION = "en"
ISO_CODES_NO_TRANSLATION = ["gb","us"]
HEADERS = ["iso_code", "title", "description", "lang_tranlation", 
           "title_translated", "description_translated", "pubDate"] # Cols del csv de news
HEADERS_OLD_NEWS = ["iso_code", "title", "description", "pubDate"]
NEWS_US = "20" # Number of news to download every day for US
NEWS_UE_COUNTRIES = "10" # Number of news to download every day for UE country
BOOTSTRAP_SERVER= ['localhost:9092'] # Kafka
TOPIC_NAME = 'last_news'
TOPICS_LIST = [TOPIC_NAME]
# Limit the characters in the news description to be translated because the Google Translate API gives an error
CHAR_LIMIT_TRANSLATE = 2500
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
LIMIT_WORD_COUNT_DAY = 20
## Path of downloaded trained BERT MODEL, to find NER in the text, because it takes more than 300mb
BERT_MODEL = "file:///home/roser/models_npl/berttest"
## Limit for loading old news per day that will be uploaded to hdfs, (first 50th published).
OLD_NEWS_DAY = 10
## Download news from the New York Times API from the start year
DWL_OLD_NEWS_FROM = 1970
## Download news from API New York Times till year
DWL_OLD_NEWS_TO = 2026
## Max time waiting for msg in consumer till close (5 minutes).
MAX_TIME_WAITING = 300

