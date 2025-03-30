from constants import *
from get_news import *
import time
from datetime import datetime
import subprocess
from producer_news import ini_producer
import os
import pprint

if __name__ == '__main__':
    
    # Set initial path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    try:
        producer = ini_producer()
        while True:
            print('Downloading news at: ', datetime.now() )
            summary = get_dayli_news(producer)
            print('## Country: [Total news download, Total news translated]')
            pprint.pprint(summary)
            time.sleep(NEWS_WAIT_TO_DOWNLOAD) # Sleep till next day in second.

    except Exception as e:
        print(e)



#    subprocess.call('!hdfs dfs -ls /', shell = True)