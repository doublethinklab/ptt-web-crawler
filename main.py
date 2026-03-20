from PttWebCrawler.crawler import PttWebCrawler
from dotenv import load_dotenv

load_dotenv()

c = PttWebCrawler(as_lib=True)
for board in ['joke', 'Military', 'Womentalk', 'HatePolitics', 'Gossiping']:
    c.parse_articles_by_date(board, days=1, save_locally=False)
