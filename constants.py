NEWS_API_KEY =  "8635a40f4d36463daee878ac23c22bde"
NEWS_DATA_IO_KEY = "pub_74893bb2e9fab265b3470e63f3729577d19b3"
NEWS_NY_TIME_KEY = "XmnrnMroyE7SReUoLfrGH1rGqvhedlUi"
NEWS_WAIT_TO_DOWNLOAD = 43200
PATH_NEWS = "data/news/"
PATH_OLD_NEWS = "data/old_news/"
PATH_US_ISO = "data/pais-us.csv"
PATH_UE_ISO = "data/paisos-ue.csv"
LANGUAGE_TRANSLATION = "en"
ISO_CODES_NO_TRANSLATION = ["gb","us"]
HEADERS = ["iso_code", "title", "description", "lang_tranlation", "title_translated", "description_translated", "pubDate"] # Cols del csv de news
HEADERS_OLD_NEWS = ["iso_code", "title", "description", "pubDate"]
NEWS_US = "20" # Number of news to download every day for US
NEWS_UE_COUNTRIES = "10" # Number of news to download every day for UE country
BOOTSTRAP_SERVER= ['localhost:9092'] # Kafka
TOPIC_NAME = 'last_news' 
TOPICS_LIST = [TOPIC_NAME]
CHAR_LIMIT_TRANSLATE = 2500
NER_EXCLUDE = ".*-(PER|ORG|LOC)"
EXCLUDE_WORDS= "^(www|http|https|telnet|mailto|ftps|monday|tuesday|wednesday|thursday|friday|saturday|sunday|" \
    "january|february|march|april|may|june|july|august|september|october|november|december|year|week|day)"
LIMIT_WORD_COUNT_DAY = 20

