##
# @package get_news.py
# Get ISO CODES, download news from both API for all countries in iso codes list, save them into
# each county file as csv, and call producer to send news into broker.
import os
import csv
import requests
import numpy as np
from TFM.source.translate import google_translate
from TFM.source.producer_news import send_producer, flush_producer
from TFM.source.constants import (
    PATH_NEWS, PATH_US_ISO, PATH_UE_ISO, NEWS_DATA_IO_KEY, 
    NEWS_UE_COUNTRIES, LANGUAGE_TRANSLATION, ISO_CODES_NO_TRANSLATION,
    HEADERS, TOPIC_NAME, NEWS_API_KEY, NEWS_US
    )

##
# @brief Download last news from API.
# @details Get ISO CODES, download news from both API for all countries in iso codes list,
# save them into each county file as csv, and call producer to send news into broker.
#
# @param producer Kafka producer to send news to topic.
#
# @return Count of news downloaded by country and translated into English.
def get_dayli_news(producer)->dict:
    """ Get ISO CODES, download news from both API for all countries in iso codes list,
        save them into each county file as csv, and call producer to send news into broker.
    
    Args:
        producer: Kafka producer to send news to topic.

    Returns:
        dict: Count of news downloaded by country and translated into English.
    """
    # Make dir news if not exist
    os.makedirs(PATH_NEWS, exist_ok=True)

    #GET ISO Country CODES and language iso codes: US and EU countries
    try:
        dict_iso_codes = dict()
        # Open CSV from countries USA
        with open(PATH_US_ISO, newline='', encoding='utf-8') as csvfile:
            # Skip the column names
            csvfile.readline()
            lector = csv.reader(csvfile)
            paisos = np.array(list(lector))

        # Get ISO Code
        iso_code = paisos[:,2]
        iso_code_us = list(map(str.lower, iso_code))
        dict_iso_codes['us'] = iso_code_us
        #print('Iso codes for US: ', iso_code_us)

        # Open CSV from countries in EU
        with open(PATH_UE_ISO, newline='', encoding='utf-8') as csvfile:
            # Skip the column names
            csvfile.readline()
            lector = csv.reader(csvfile) 
            paisos = np.array(list(lector))

        # Get ISO Codes Counties and Languages
        iso_codes = paisos[:,-2:]
        iso_codes_eu = np.char.lower(iso_codes)
        #print('Iso codes for EU: ',iso_codes_eu)
        np.random.shuffle(iso_codes_eu)
        dict_iso_codes['ue'] = iso_codes_eu

    except KeyboardInterrupt:
        producer.close()
        pass
    except Exception as e:
        print('ERROR - Reading ISO Codes: ',e)

    # Count of downloaded and translated news.
    recompte = {}

    try:

        # GET data from NEWSAPI - UE countries
        for iso_code in dict_iso_codes['ue']:

            try:
                country_code = str(iso_code[0])
                language_codes = iso_code[1]

                recompte[country_code] = [0,0]

                # Select first language if country has more than one, for request news in a language
                # of country.
                if len(language_codes) == 2:

                    language_request = language_codes
                else:
                    list_cod = language_codes.split(',')
                    language_request = list_cod[0]

                url = "https://newsdata.io/api/1/latest?apikey="+NEWS_DATA_IO_KEY+"&country="\
                    +country_code+"&language="+language_request+"&size="+NEWS_UE_COUNTRIES

                response = requests.get(url)
                response.raise_for_status()
                #print('Request status code NewsData.io: ', response.status_code)

                response_json_ue = response.json()

                # Check if file country exist
                filename = PATH_NEWS+'news_'+country_code+'.csv'
                file_exists = os.path.isfile(filename)
                list_news = []
                recompte[country_code][0] += len(response_json_ue['results'])

                # open the file in append mode ('a') to add data without overwriting.
                with open(filename, mode="a", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)

                    if not file_exists:
                        writer.writerow(HEADERS)
                    for news in response_json_ue['results']:

                        title = news['title']
                        description = news['description'] if news['description'] is not None else ''

                        # Get translation.
                        title_translated = google_translate(title, [language_request],
                                                            LANGUAGE_TRANSLATION)
                        desc_translated = google_translate(description, [language_request],
                                                           LANGUAGE_TRANSLATION)

                        if None not in (title_translated, desc_translated) and country_code not in ISO_CODES_NO_TRANSLATION:
                            # Count news translated.
                            recompte[country_code][1] += 1

                        news_line = [country_code, news['title'], news['description'],
                                     LANGUAGE_TRANSLATION, title_translated,
                                     desc_translated, news['pubDate']]

                        writer.writerow(news_line)
                        list_news.append(news_line)

                        send_producer(producer, TOPIC_NAME, news_line)

                    flush_producer(producer, list_news)

            # If the request fails (404) then print the error.
            except requests.exceptions.HTTPError as error:
                print('ERROR - Request NewsData.io API fails: ', error, '\n', url)


        # GET data from NEWSAPI - US
        for iso_code in dict_iso_codes['us']:
            recompte[iso_code] = [0,0]
            try:
                url = "https://newsapi.org/v2/top-headlines?country="+iso_code+ \
                        "&apiKey="+NEWS_API_KEY+"&pageSize="+NEWS_US

                response_us = requests.get(url)
                response_us.raise_for_status()
                #print('Request status code NewsAPI: ', response_us.status_code)
                response_json_us = response_us.json()

                # Check if exist
                filename = PATH_NEWS+'news_'+iso_code+'.csv'
                file_exists = os.path.isfile(filename)
                list_news_us = []
                recompte[iso_code][0] = len(response_json_us['articles'])

                # Open the file in append mode ('a') to add data without overwriting.
                with open(filename, mode="a", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)

                    if not file_exists:
                        writer.writerow(HEADERS)

                    for news in response_json_us['articles']:

                        title = news['title']
                        description = news['description'] if news['description'] is not None else ''

                        news_line = [iso_code, title, description, LANGUAGE_TRANSLATION,
                                    title, description, news['publishedAt']]
                        writer.writerow(news_line)
                        list_news_us.append(news_line)

                        send_producer(producer, TOPIC_NAME, news_line)

                    flush_producer(producer, list_news_us)

            # If the request fails (404) then print the error.
            except requests.exceptions.HTTPError as error:
                print('ERROR - Request NewsAPI fails: ', error)

        return recompte

    except KeyboardInterrupt:
        producer.close()
        pass
    except Exception as e:
        print('ERROR - Downloading News: ', e)
