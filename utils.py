import os
import pymongo
from pymongo import UpdateOne
from dotenv import load_dotenv

load_dotenv()

class BatchSaver:
    def __init__(self, max_size=50):
        self.max_size = max_size
        self.data = []

    def add(self, item: dict):
        self.data.append(item)
        if len(self.data) >= self.max_size:
            self.save_to_db()
            self.data.clear()

    def save_to_db(self):
        to_mongo(self.data)

    def flush(self):
        if self.data:
            self.save_to_db()
            self.data.clear()


def to_mongo(data):
    try:
        bulk_operations = []
        if data:
            if ptt_data is None:
                raise RuntimeError('MONGODB_URI is not configured; cannot write crawler results to MongoDB.')
            for article in data:
                bulk_operations.append(UpdateOne(
                    {
                        'board': article.get('board'),
                        'article_id': article.get('article_id')
                    },
                    {'$set': article},
                    upsert=True
                ))
            ptt_data.bulk_write(bulk_operations)
            print(f"{len(bulk_operations)} articles inserted")
    except Exception as e:
        print(f'failed to write articles to MongoDB: {e}')


mongo_uri = os.getenv('MONGODB_URI')
if mongo_uri:
    client = pymongo.MongoClient(mongo_uri)
    dtl_data = client['dtl_data']
    ptt_data = dtl_data['ptt_data']
else:
    client = None
    dtl_data = None
    ptt_data = None
