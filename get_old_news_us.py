import requests
import csv
import numpy as np
import os
from constants import *
from preproces_text import clean_text 
import time

def get_old_news(year: str, month: str)->None:
    """ Download old news from API New York Times and 
        save them into csv year_month_news_us_old.csv.
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
        
        # Obrim el fitxer en mode append ('a') i afegim els camps iso county code, title, description, i data de publicació
        with open(filename, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
                                
            if not file_exists:
                writer.writerow(HEADERS_OLD_NEWS)
                    
            for news in response_json_us['response']['docs']:

                title = clean_text(news['headline']['main'])
                description = clean_text(news['lead_paragraph']) if news['lead_paragraph'] is not None else ''

                news_line = ['us', title, description, news['pub_date']]
                writer.writerow(news_line)
                        
    
    except requests.exceptions.HTTPError as error:
            print('ERROR - Request NY Times API fails: ', error)


try:
    # Set initial path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Download full years
    for year in range(1970,2025):
        for month in range(1,13):
            get_old_news(str(year), str(month))
            time.sleep(10)
    
    # Download 3 first month  of 2025
    for month in range(1,4):
        get_old_news('2025', str(month))            


except Exception as e:
     print('ERROR - Downloading OLD News: ',e)
    