from PttWebCrawler.crawler import PttWebCrawler
from dotenv import load_dotenv

load_dotenv()

c = PttWebCrawler(as_lib=True)
c.parse_articles(0, 10, 'joke', save_locally=False)
c.parse_articles(0, 10, 'Military', save_locally=False)
c.parse_articles(0, 250, 'Womentalk', save_locally=False)
c.parse_articles(0, 500, 'HatePolitics', save_locally=False)  # 1 index = 20 articles
c.parse_articles(0, 500, 'Gossiping', save_locally=False)
