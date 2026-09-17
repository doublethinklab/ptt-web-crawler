from datetime import timedelta

from PttWebCrawler.crawler import PttWebCrawler
from dotenv import load_dotenv

load_dotenv()

DAILY_BOARDS = ['joke', 'Military', 'WomenTalk', 'HatePolitics', 'Gossiping']
SCAN_BOARDS = ['HatePolitics', 'Gossiping']

c = PttWebCrawler(as_lib=True)

# Full backfill of the previous day, matching `run.sh daily`
yesterday = c.today() - timedelta(days=1)
for board in DAILY_BOARDS:
    c.parse_articles_by_date(board, target_date=yesterday, days=1, save_locally=False)

# Intraday top-up, matching `run.sh scan`
for board in SCAN_BOARDS:
    c.scan_new_articles(board, save_locally=False)
