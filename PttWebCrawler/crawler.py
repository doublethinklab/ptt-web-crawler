# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import os
import re
import sys
import json
import requests
import argparse
import time
import codecs
from bs4 import BeautifulSoup
from six import u
from datetime import datetime, timedelta
from utils import BatchSaver

__version__ = '1.0'

# if python 2, disable verify flag in requests.get()
VERIFY = True
if sys.version_info[0] < 3:
    VERIFY = False
    requests.packages.urllib3.disable_warnings()


def extract_author_id(s):
    match = re.search(r'^(.*?)\s*\(.*\)', s)
    if match:
        return match.group(1).strip()  # 去除前後的空白
    else:
        return None


class PttWebCrawler(object):
    PTT_URL = 'https://www.ptt.cc'
    DEFAULT_HEADERS = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/122.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }

    """docstring for PttWebCrawler"""

    def __init__(self, cmdline=None, as_lib=False):
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        self.session.cookies.update({'over18': '1'})

        parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description='''
            A crawler for the web version of PTT, the largest online community in Taiwan.
            Input: board name and page indices (or articla ID)
            Output: BOARD_NAME-START_INDEX-END_INDEX.json (or BOARD_NAME-ID.json)
        ''')
        parser.add_argument('-b', metavar='BOARD_NAME', help='Board name', required=True)
        group = parser.add_mutually_exclusive_group(required=False)
        group.add_argument('-i', metavar=('START_INDEX', 'END_INDEX'), type=int, nargs=2, help="Start and end index")
        group.add_argument('-a', metavar='ARTICLE_ID', help="Article ID")
        group.add_argument('--mode', choices=['all', 'daily'], help='Crawl mode')
        parser.add_argument('--date', help='Target date in YYYY-MM-DD')
        parser.add_argument('--days', type=int, default=1, help='Number of days to crawl backward from --date')
        parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + __version__)

        if not as_lib:
            if cmdline:
                args = parser.parse_args(cmdline)
            else:
                args = parser.parse_args()
            board = args.b
            if args.i:
                start = args.i[0]
                if args.i[1] == -1:
                    end = self.getLastPage(board)
                else:
                    end = args.i[1]
                self.parse_articles(start, end, board)
            elif args.a:
                article_id = args.a
                self.parse_article(article_id, board)
            elif args.mode:
                target_date = self.parse_date_arg(args.date) if args.date else datetime.today().date()
                if args.days < 1:
                    parser.error('--days must be greater than or equal to 1')
                if args.mode == 'all':
                    self.parse_all_articles(board)
                else:
                    self.parse_articles_by_date(board, target_date=target_date, days=args.days)
            else:
                parser.error('one of -i, -a, or --mode is required')

    def parse_articles(self, start, end, board, path='data', timeout=3, save_locally=False):
        today = datetime.today().strftime('%Y%m%d')
        filename = f"{board}-{start}-{end}-{today}.json"
        filename = os.path.join(path, filename)
        all_data = BatchSaver()
        local_data = []
        for i in range(end - start + 1):
            index = start + i
            print('Processing index:', str(index))
            try:
                resp = self.session.get(
                    url=f"{self.PTT_URL}/bbs/{board}/index{index}.html",
                    verify=VERIFY, timeout=timeout
                )
                if resp.status_code != 200:
                    print('invalid url:', resp.url)
                    continue
                soup = BeautifulSoup(resp.text, 'lxml')
                divs = soup.find_all("div", "r-ent")
                for div in divs:
                    try:
                        anchor = div.find('a')
                        if not anchor:
                            continue
                        # ex. link would be <a href="/bbs/PublicServan/M.1127742013.A.240.html">Re: [問題] 職等</a>
                        href = anchor['href']
                        link = self.PTT_URL + href
                        article_id = re.sub('\.html', '', href.split('/')[-1])
                        article = self.parse(link, article_id, board, timeout=timeout, session=self.session)
                        all_data.add(article)
                        if save_locally:
                            local_data.append(article)
                    except Exception as exc:
                        print(f'failed to parse article on {board} index {index}: {exc}')
            except requests.exceptions.RequestException as exc:
                print(f'failed to fetch board page {board} index {index}: {exc}')
            time.sleep(0.1)

        all_data.flush()
        if save_locally:
            self.store(filename, local_data)
        return all_data

    def parse_articles_by_date(self, board, target_date=None, days=1, path='data', timeout=3, save_locally=False):
        if target_date is None:
            target_date = datetime.today().date()
        if days < 1:
            raise ValueError('days must be greater than or equal to 1')

        start_date = target_date - timedelta(days=days - 1)
        if days == 1:
            filename = os.path.join(path, f"{board}-{target_date.strftime('%Y%m%d')}.json")
        else:
            filename = os.path.join(
                path,
                f"{board}-{start_date.strftime('%Y%m%d')}-{target_date.strftime('%Y%m%d')}.json"
            )
        return self._crawl_by_date_range(
            board=board,
            start_date=start_date,
            end_date=target_date,
            filename=filename,
            timeout=timeout,
            save_locally=save_locally,
        )

    def parse_all_articles(self, board, path='data', timeout=3, save_locally=False):
        filename = os.path.join(path, f"{board}-all.json")
        return self._crawl_by_date_range(
            board=board,
            start_date=None,
            end_date=None,
            filename=filename,
            timeout=timeout,
            save_locally=save_locally,
        )

    def parse_article(self, article_id, board, path='data'):
        today = datetime.today().strftime('%Y%m%d')
        link = f"{self.PTT_URL}/bbs/{board}/{article_id}.html"
        filename = f'{board}-{article_id}-{today}.json'
        filename = os.path.join(path, filename)
        self.store(filename, self.parse(link, article_id, board), 'w')
        return filename

    @staticmethod
    def parse(link, article_id, board, timeout=3, session=None):
        print(f'Processing article of {board}:', article_id)
        http = session or requests.Session()
        if session is None:
            http.headers.update(PttWebCrawler.DEFAULT_HEADERS)
            http.cookies.update({'over18': '1'})

        resp = http.get(url=link, verify=VERIFY, timeout=timeout)
        if resp.status_code != 200:
            raise ValueError(f'invalid url: {resp.url}')
        soup = BeautifulSoup(resp.text, 'lxml')
        main_content = soup.find(id="main-content")
        if main_content is None:
            raise ValueError(f'main-content not found for {resp.url}')
        metas = main_content.select('div.article-metaline')
        author = ''
        title = ''
        date = ''
        if metas:
            author = extract_author_id(metas[0].select('span.article-meta-value')[0].string) if \
            metas[0].select('span.article-meta-value')[0] else author
            title = metas[1].select('span.article-meta-value')[0].string if metas[1].select('span.article-meta-value')[
                0] else title
            date = metas[2].select('span.article-meta-value')[0].string if metas[2].select('span.article-meta-value')[
                0] else date

            # remove meta nodes
            for meta in metas:
                meta.extract()
            for meta in main_content.select('div.article-metaline-right'):
                meta.extract()

        # remove and keep push nodes
        pushes = main_content.find_all('div', class_='push')
        for push in pushes:
            push.extract()

        try:
            ip = main_content.find(string=re.compile(u'※ 發信站:'))
            ip = re.search('[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*', ip).group()
        except:
            ip = "None"

        # 移除 '※ 發信站:' (starts with u'\u203b'), '◆ From:' (starts with u'\u25c6'), 空行及多餘空白
        # 保留英數字, 中文及中文標點, 網址, 部分特殊符號
        filtered = [v for v in main_content.stripped_strings if v[0] not in [u'※', u'◆'] and v[:2] not in [u'--']]
        expr = re.compile(
            u(r'[^\u4e00-\u9fa5\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\s\w:/-_.?~%()]'))
        for i in range(len(filtered)):
            filtered[i] = re.sub(expr, '', filtered[i])

        filtered = [_f for _f in filtered if _f]  # remove empty strings
        filtered = [x for x in filtered if article_id not in x]  # remove last line containing the url of the article
        content = ' '.join(filtered)
        content = re.sub(r'(\s)+', ' ', content)
        # print 'content', content

        # push messages
        p, b, n = 0, 0, 0
        messages = []
        for push in pushes:
            if not push.find('span', 'push-tag'):
                continue
            push_tag = push.find('span', 'push-tag').string.strip(' \t\n\r')
            push_userid = push.find('span', 'push-userid').string.strip(' \t\n\r')
            # if find is None: find().strings -> list -> ' '.join; else the current way
            push_content = push.find('span', 'push-content').strings
            push_content = ' '.join(push_content)[1:].strip(' \t\n\r')  # remove ':'
            push_ipdatetime = push.find('span', 'push-ipdatetime').string.strip(' \t\n\r')
            messages.append({'push_tag': push_tag, 'push_userid': push_userid, 'push_content': push_content,
                             'push_ipdatetime': push_ipdatetime})
            if push_tag == u'推':
                p += 1
            elif push_tag == u'噓':
                b += 1
            else:
                n += 1

        # count: 推噓文相抵後的數量; all: 推文總數
        message_count = {'all': p + b + n, 'count': p - b, 'push': p, 'boo': b, "neutral": n}

        # print 'msgs', messages
        # print 'mscounts', message_count

        publish_time_utc8 = None
        date_ = ''
        time_ = ''
        if date:
            publish_time_utc8 = datetime.strptime(date, '%a %b %d %H:%M:%S %Y')
            publish_time_utc = publish_time_utc8 - timedelta(hours=8)
            date_ = publish_time_utc.strftime('%Y-%m-%d')
            time_ = publish_time_utc.strftime('%H:%M:%S')

        # json data
        data = {
            'url': link,
            'board': board,
            'article_id': article_id,
            'article_title': title,
            'author': author,
            'datetime_utc8': publish_time_utc8.strftime('%Y-%m-%d %H:%M:%S') if publish_time_utc8 else '',
            'date': date_,
            'time': time_,
            'content': content,
            'ip': ip,
            'message_count': message_count,
            'messages': messages
        }
        return data

    def _crawl_by_date_range(self, board, start_date, end_date, filename, timeout=3, save_locally=False):
        latest_page = self.getLastPage(board, timeout=timeout)
        batch_saver = BatchSaver()
        local_data = []
        should_stop = False

        for page_index in range(latest_page, 0, -1):
            print('Processing index:', str(page_index))
            try:
                resp = self.session.get(
                    url=self._build_index_url(board, page_index, latest_page),
                    verify=VERIFY,
                    timeout=timeout
                )
                if resp.status_code != 200:
                    print('invalid url:', resp.url)
                    continue
                soup = BeautifulSoup(resp.text, 'lxml')
                divs = soup.find_all("div", "r-ent")
                if not divs:
                    continue

                for div in divs:
                    anchor = div.find('a')
                    if not anchor:
                        continue

                    href = anchor['href']
                    link = self.PTT_URL + href
                    article_id = re.sub('\.html', '', href.split('/')[-1])
                    try:
                        article = self.parse(link, article_id, board, timeout=timeout, session=self.session)
                    except Exception as exc:
                        print(f'failed to parse article on {board} index {page_index}: {exc}')
                        continue

                    article_date = self.article_date(article)
                    if (start_date or end_date) and article_date is None:
                        print(f'skipping article without datetime on {board}: {article_id}')
                        continue
                    if end_date and article_date and article_date > end_date:
                        continue
                    if start_date and article_date and article_date < start_date:
                        should_stop = True
                        break

                    batch_saver.add(article)
                    if save_locally:
                        local_data.append(article)

                if should_stop:
                    break
            except requests.exceptions.RequestException as exc:
                print(f'failed to fetch board page {board} index {page_index}: {exc}')
            time.sleep(0.1)

        batch_saver.flush()
        if save_locally:
            self.store(filename, local_data)
        return batch_saver

    @staticmethod
    def article_date(article):
        datetime_utc8 = article.get('datetime_utc8')
        if not datetime_utc8:
            return None
        return datetime.strptime(datetime_utc8, '%Y-%m-%d %H:%M:%S').date()

    @staticmethod
    def parse_date_arg(date_text):
        return datetime.strptime(date_text, '%Y-%m-%d').date()

    def _build_index_url(self, board, page_index, latest_page):
        if page_index == latest_page:
            return f'{self.PTT_URL}/bbs/{board}/index.html'
        return f'{self.PTT_URL}/bbs/{board}/index{page_index}.html'

    @staticmethod
    def getLastPage(board, timeout=3):
        content = requests.get(
            url='https://www.ptt.cc/bbs/' + board + '/index.html',
            headers=PttWebCrawler.DEFAULT_HEADERS,
            cookies={'over18': '1'},
            timeout=timeout
        ).content.decode('utf-8')
        first_page = re.search(r'href="/bbs/' + board + '/index(\d+).html">&lsaquo;', content)
        if first_page is None:
            return 1
        return int(first_page.group(1)) + 1

    @staticmethod
    def store(filename, data, mode='w'):
        directory = os.path.dirname(filename)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(filename, mode) as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Saved to {filename}")

    @staticmethod
    def get(filename, mode='r'):
        with codecs.open(filename, mode, encoding='utf-8') as f:
            return json.load(f)


if __name__ == '__main__':
    c = PttWebCrawler()
