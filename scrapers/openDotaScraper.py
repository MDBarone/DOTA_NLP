#Ideas:
# Ward placement comparison. Find most dewarded wards.


### Scraping
import requests
import json
import pandas as pd
from textblob import TextBlob

### SELENIUM ISSUE: Remember you need to install a driver and set the path.
# import os
# from dotenv import load_dotenv
# from bs4 import BeautifulSoup
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException
# driver = webdriver.Firefox()
# driver.get('https://www.lazada.sg/#')

# load_dotenv()
# API_KEY = os.getenv('OPENDOTA_API_KEY')
# link = 'https://api.opendota.com/api/matches/271145478?api_key=%s'
# r = requests.get(link % API_KEY)


# from datetime import datetime, timedelta
# time = datetime.today()-timedelta(days=-1)
# epochTime = time.strftime('%s')

playerLink = 'https://api.opendota.com/api/players/46313030'
link = 'https://api.opendota.com/api/explorer?sql=SELECT%20%2A%20FROM%20matches%20WHERE%20start_time>1606798800%20LIMIT%2050'
link = 'https://api.opendota.com/api/explorer?sql=SELECT%20%2A%20FROM%20player_ratings%20WHERE%20solo_competitive_rank<1500'

## Setup for cron jobs and luigi
## Get recent match data based on mmr
## Insert to match table using sqlalchemy orm
## Check if player id is new
## get player data if new id
## display data using dash
## "ward efficiency" analysis
## Host on heroku or ec2 instance
## Build analytics pipeline (NLP neural net using spark on tensorflow, served to the dash)

r = requests.get(link)
payload = r.json()
payload
payload.keys()

for row in payload['rows']:
    df = pd.DataFrame(row['chat'])
    if df.shape[0] == 0 or df.loc[df['type'] == 'chat'].shape[0] < 20:
        continue
    else:
        chatIndexes = df.loc[df['type'] == 'chat'].index
        b = TextBlob("\t".join(df.loc[chatIndexes,'key']))
        language = b.detect_language()
        df['language'] = language
        # Translate to english
        if language != 'en':
            b = b.translate(from_lang=language,to='en')
            try:
                df.loc[chatIndexes,'key'] = b.raw.split("\t")
            except:
                continue

        if b.sentiment_assessments.polarity < -0.3 and language == 'en':
            print("found")
            break

b.sentiment_assessments
# data['match_seq_num']


### DB STUFF
# Move to dbm.py later
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


conn = sqlite3.connect('../dotabase.db')
engine = create_engine('sqlite:///../localhost/dotabase.db')
c = conn.cursor()

# Replace sql query and df parameters
def write_from_df_with_sqlite3(df):
    for index, row in df.iterrows():
        c.execute(
        '''
            INSERT INTO lazada_product VALUES
              (CURRENT_TIMESTAMP,?,?,?,?,?)
        ''',
            (row['id'], row['link'],row['product_title'],row['product_price'],
            row['category'])
        )
