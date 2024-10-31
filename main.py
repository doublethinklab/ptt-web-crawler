from json import JSONDecodeError
from pymongo import UpdateOne
from PttWebCrawler.crawler import PttWebCrawler
from dotenv import load_dotenv
import os
import json
import pymongo


load_dotenv()

client = pymongo.MongoClient(os.getenv('MONGODB_URI'))
dtl_data = client['dtl_data']
ptt_data = dtl_data['ptt_data']
def to_mongo(data):
    bulk_operations = []
    if data:
        for article in data:
            bulk_operations.append(UpdateOne(
                {'article_id': article.get('article_id')},
                {'$set': article},
                upsert=True
            ))
        ptt_data.bulk_write(bulk_operations)

c = PttWebCrawler(as_lib=True)
to_mongo(c.parse_articles(0, 10, 'joke', save_locally=True))
to_mongo(c.parse_articles(0, 10, 'Military', save_locally=True))
to_mongo(c.parse_articles(0, 50, 'HatePolitics', save_locally=True))  # 1 index = 20 articles
to_mongo(c.parse_articles(0, 50, 'Gossiping', save_locally=True))
to_mongo(c.parse_articles(0, 25, 'Womentalk', save_locally=True))
