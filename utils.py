import os
import pymongo
from pymongo import UpdateOne
from dotenv import load_dotenv

load_dotenv()


class BatchSaver:
    def __init__(self, max_size=200):
        self.max_size = max_size
        self.data = []
        self.saved_count = 0
        self.failed_count = 0

    def add(self, item: dict):
        self.data.append(item)
        if len(self.data) >= self.max_size:
            self.save_to_db()

    def save_to_db(self):
        if not self.data:
            return
        try:
            self.saved_count += to_mongo(self.data)
        except Exception as e:
            self.failed_count += len(self.data)
            print(f'failed to write articles to MongoDB: {e}')
        finally:
            self.data.clear()

    def flush(self):
        self.save_to_db()


def to_mongo(data):
    """Upsert articles and return how many documents the write actually covered."""
    if not data:
        return 0
    if ptt_data is None:
        raise RuntimeError('MONGODB_URI is not configured; cannot write crawler results to MongoDB.')

    bulk_operations = []
    for article in data:
        bulk_operations.append(UpdateOne(
            {
                'board': article.get('board'),
                'article_id': article.get('article_id')
            },
            {'$set': article},
            upsert=True
        ))
    result = ptt_data.bulk_write(bulk_operations)
    written = result.upserted_count + result.matched_count
    print(f"{written} articles written")
    return written


def find_existing_article_ids(board, article_ids):
    """Return the subset of article_ids already stored for this board.

    Used by the intraday scan so it only spends requests on articles that have
    never been fetched before.
    """
    article_ids = list(article_ids)
    if not article_ids:
        return set()
    if ptt_data is None:
        raise RuntimeError('MONGODB_URI is not configured; cannot look up existing articles.')

    existing = set()
    chunk_size = 500
    for start in range(0, len(article_ids), chunk_size):
        chunk = article_ids[start:start + chunk_size]
        cursor = ptt_data.find(
            {'board': board, 'article_id': {'$in': chunk}},
            {'article_id': 1, '_id': 0}
        )
        existing.update(doc['article_id'] for doc in cursor)
    return existing


mongo_uri = os.getenv('MONGODB_URI')
client = None
dtl_data = None
ptt_data = None
if mongo_uri:
    try:
        client = pymongo.MongoClient(mongo_uri)
        dtl_data = client['dtl_data']
        ptt_data = dtl_data['ptt_data']
    except Exception as e:
        # Keep ptt_data None so writes raise loudly and the run reports itself
        # as incomplete, instead of taking the whole process down at import.
        print(f'failed to initialise MongoDB client: {e}')
