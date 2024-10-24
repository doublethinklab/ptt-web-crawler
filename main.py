from PttWebCrawler.crawler import PttWebCrawler
from dotenv import load_dotenv
import os
import json
import pymongo

load_dotenv()

c = PttWebCrawler(as_lib=True)
c.parse_articles(0, 10, 'HatePolitics')

client = pymongo.MongoClient(os.getenv('MONGODB_URI'))
dtl_data = client['dtl_data']
ptt_data = dtl_data['ptt_data']

for filename in os.listdir('.'):
    if filename.endswith(".json"):
        with open(filename) as f:
            data_file = json.load(f)
            data = data_file.get('articles', [])
            if data:
                ptt_data.insert_many(data)
        os.remove(filename)