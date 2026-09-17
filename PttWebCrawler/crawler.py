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
from datetime import datetime, timedelta, timezone
from utils import BatchSaver, find_existing_article_ids

__version__ = '1.1'

# if python 2, disable verify flag in requests.get()
VERIFY = True
if sys.version_info[0] < 3:
    VERIFY = False
    requests.packages.urllib3.disable_warnings()

# PTT publishes in UTC+8. Pin the timezone so the crawler behaves identically
# no matter how the host machine's clock is configured.
TZ8 = timezone(timedelta(hours=8))

# Article ids and article meta dates can disagree by a few seconds, so a single
# out-of-range article near midnight must not end the crawl. Stop only after
# this many consecutive older articles (roughly two index pages).
OUT_OF_RANGE_THRESHOLD = 40

MAX_RETRIES = 3
RETRY_BACKOFF = 1.0
# PTT sits behind Cloudflare, which returns 52x for transient origin problems
RETRYABLE_STATUS = (408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524)

DEFAULT_TIMEOUT = 10
REQUEST_INTERVAL = 0.1

# A run is considered complete when at least this share of the articles listed
# on the index pages ended up accounted for.
COVERAGE_THRESHOLD = 99.0


def extract_author_id(s):
    match = re.search(r'^(.*?)\s*\(.*\)', s)
    if match:
        return match.group(1).strip()  # 去除前後的空白
    else:
        return None


def fetch_with_retry(http, url, timeout=DEFAULT_TIMEOUT, retries=MAX_RETRIES):
    """GET a PTT url, retrying transient failures with exponential backoff.

    Without this a single timeout used to silently drop a whole index page
    (20 articles) from the day's results.
    """
    last_error = None
    for attempt in range(retries):
        try:
            resp = http.get(url=url, verify=VERIFY, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code in RETRYABLE_STATUS:
                last_error = f'HTTP {resp.status_code}'
            else:
                raise ValueError(f'invalid url: {resp.url} (HTTP {resp.status_code})')
        except requests.exceptions.RequestException as exc:
            last_error = exc
        if attempt < retries - 1:
            time.sleep(RETRY_BACKOFF * (2 ** attempt))
    raise requests.exceptions.RequestException(
        f'failed after {retries} attempts: {url} ({last_error})'
    )


class CrawlReport(object):
    """Per-board bookkeeping so a run can prove it captured the whole day."""

    def __init__(self, board, label):
        self.board = board
        self.label = label
        self.listed = 0             # articles the index pages say are in range
        self.fetched = 0            # articles parsed successfully
        self.skipped_existing = 0   # already in MongoDB (scan mode only)
        self.out_of_range = 0       # id timestamp in range but meta date was not
        self.failed = []            # article ids that could not be parsed
        self.failed_pages = []      # index pages that could not be fetched
        self.saved = 0
        self.save_failures = 0

    @property
    def accounted(self):
        return self.saved + self.skipped_existing + self.out_of_range

    @property
    def coverage(self):
        if self.listed == 0:
            return 100.0
        return self.accounted * 100.0 / self.listed

    def is_complete(self):
        return (
            not self.failed
            and not self.failed_pages
            and self.save_failures == 0
            and self.coverage >= COVERAGE_THRESHOLD
        )

    def render(self):
        print(
            f'[{self.board}] {self.label} '
            f'listed={self.listed} fetched={self.fetched} saved={self.saved} '
            f'existing={self.skipped_existing} out_of_range={self.out_of_range} '
            f'failed={len(self.failed)} failed_pages={len(self.failed_pages)} '
            f'save_failures={self.save_failures} coverage={self.coverage:.1f}%'
        )
        if self.failed:
            print(f'[{self.board}] failed articles: {", ".join(self.failed)}')
        if self.failed_pages:
            print(f'[{self.board}] failed index pages: {self.failed_pages}')
        if not self.is_complete():
            print(f'[{self.board}] INCOMPLETE: day was not fully captured')


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
        group.add_argument('--mode', choices=['all', 'daily', 'scan'], help='Crawl mode')
        parser.add_argument('--date', help='Target date in YYYY-MM-DD (UTC+8)')
        parser.add_argument('--days', type=int, default=1, help='Number of days to crawl backward from --date')
        parser.add_argument('--offset', type=int, default=0,
                            help='Days before today (UTC+8) to use as target date; ignored when --date is given')
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
                if args.days < 1:
                    parser.error('--days must be greater than or equal to 1')
                if args.offset < 0:
                    parser.error('--offset must be greater than or equal to 0')
                if args.date:
                    target_date = self.parse_date_arg(args.date)
                else:
                    target_date = self.today() - timedelta(days=args.offset)

                if args.mode == 'all':
                    report = self.parse_all_articles(board)
                elif args.mode == 'scan':
                    report = self.scan_new_articles(board, target_date=target_date)
                else:
                    report = self.parse_articles_by_date(board, target_date=target_date, days=args.days)

                if report is not None and not report.is_complete():
                    sys.exit(1)
            else:
                parser.error('one of -i, -a, or --mode is required')

    def parse_articles(self, start, end, board, path='data', timeout=DEFAULT_TIMEOUT, save_locally=False):
        today = self.today().strftime('%Y%m%d')
        filename = f"{board}-{start}-{end}-{today}.json"
        filename = os.path.join(path, filename)
        report = CrawlReport(board, f'index {start}-{end}')
        batch_saver = BatchSaver()
        local_data = []
        for i in range(end - start + 1):
            index = start + i
            print('Processing index:', str(index))
            try:
                resp = fetch_with_retry(
                    self.session,
                    f"{self.PTT_URL}/bbs/{board}/index{index}.html",
                    timeout=timeout
                )
            except (requests.exceptions.RequestException, ValueError) as exc:
                print(f'failed to fetch board page {board} index {index}: {exc}')
                report.failed_pages.append(index)
                continue

            soup = BeautifulSoup(resp.text, 'lxml')
            for article_id, link, _pinned in self._index_entries(soup):
                report.listed += 1
                try:
                    article = self.parse(link, article_id, board, timeout=timeout, session=self.session)
                except Exception as exc:
                    print(f'failed to parse article on {board} index {index}: {exc}')
                    report.failed.append(article_id)
                    continue
                report.fetched += 1
                batch_saver.add(article)
                if save_locally:
                    local_data.append(article)
            time.sleep(REQUEST_INTERVAL)

        batch_saver.flush()
        report.saved = batch_saver.saved_count
        report.save_failures = batch_saver.failed_count
        if save_locally:
            self.store(filename, local_data)
        report.render()
        return report

    def parse_articles_by_date(self, board, target_date=None, days=1, path='data',
                               timeout=DEFAULT_TIMEOUT, save_locally=False):
        if target_date is None:
            target_date = self.today()
        if days < 1:
            raise ValueError('days must be greater than or equal to 1')

        start_date = target_date - timedelta(days=days - 1)
        if days == 1:
            filename = os.path.join(path, f"{board}-{target_date.strftime('%Y%m%d')}.json")
            label = f'{target_date}'
        else:
            filename = os.path.join(
                path,
                f"{board}-{start_date.strftime('%Y%m%d')}-{target_date.strftime('%Y%m%d')}.json"
            )
            label = f'{start_date}..{target_date}'
        return self._crawl_by_date_range(
            board=board,
            start_date=start_date,
            end_date=target_date,
            filename=filename,
            timeout=timeout,
            save_locally=save_locally,
            label=label,
        )

    def parse_all_articles(self, board, path='data', timeout=DEFAULT_TIMEOUT, save_locally=False):
        filename = os.path.join(path, f"{board}-all.json")
        return self._crawl_by_date_range(
            board=board,
            start_date=None,
            end_date=None,
            filename=filename,
            timeout=timeout,
            save_locally=save_locally,
            label='all',
        )

    def scan_new_articles(self, board, target_date=None, path='data',
                          timeout=DEFAULT_TIMEOUT, save_locally=False):
        """Lightweight intraday pass.

        Walks index pages only and derives each article's publish time from its
        id, then fetches just the articles MongoDB has never seen. This catches
        posts that would be deleted before the nightly full crawl runs, at a
        cost of roughly one request per index page.
        """
        if target_date is None:
            target_date = self.today()
        board = self.resolve_board_name(board, timeout=timeout)
        report = CrawlReport(board, f'scan {target_date}')

        candidates = self._collect_index_entries(
            board=board,
            start_date=target_date,
            end_date=target_date,
            report=report,
            timeout=timeout,
        )
        report.listed = len(candidates)

        existing = find_existing_article_ids(board, [article_id for article_id, _ in candidates])
        report.skipped_existing = sum(1 for article_id, _ in candidates if article_id in existing)

        batch_saver = BatchSaver()
        local_data = []
        for article_id, link in candidates:
            if article_id in existing:
                continue
            try:
                article = self.parse(link, article_id, board, timeout=timeout, session=self.session)
            except Exception as exc:
                print(f'failed to parse article on {board}: {exc}')
                report.failed.append(article_id)
                continue
            report.fetched += 1
            batch_saver.add(article)
            if save_locally:
                local_data.append(article)
            time.sleep(REQUEST_INTERVAL)

        batch_saver.flush()
        report.saved = batch_saver.saved_count
        report.save_failures = batch_saver.failed_count
        if save_locally:
            self.store(os.path.join(path, f"{board}-scan-{target_date.strftime('%Y%m%d')}.json"), local_data)
        report.render()
        return report

    def parse_article(self, article_id, board, path='data'):
        today = self.today().strftime('%Y%m%d')
        link = f"{self.PTT_URL}/bbs/{board}/{article_id}.html"
        filename = f'{board}-{article_id}-{today}.json'
        filename = os.path.join(path, filename)
        self.store(filename, self.parse(link, article_id, board), 'w')
        return filename

    @staticmethod
    def parse(link, article_id, board, timeout=DEFAULT_TIMEOUT, session=None):
        print(f'Processing article of {board}:', article_id)
        http = session or requests.Session()
        if session is None:
            http.headers.update(PttWebCrawler.DEFAULT_HEADERS)
            http.cookies.update({'over18': '1'})

        resp = fetch_with_retry(http, link, timeout=timeout)
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

    def _collect_index_entries(self, board, start_date, end_date, report, timeout=DEFAULT_TIMEOUT):
        """Page backwards through the board index and list the in-range articles.

        Only index pages are fetched here; publish times come from the article
        id, so this costs one request per page instead of one per article.
        """
        latest_page = self.getLastPage(board, timeout=timeout)
        entries = []
        out_of_range_streak = 0

        for page_index in range(latest_page, 0, -1):
            print('Processing index:', str(page_index))
            try:
                resp = fetch_with_retry(
                    self.session,
                    self._build_index_url(board, page_index, latest_page),
                    timeout=timeout
                )
            except (requests.exceptions.RequestException, ValueError) as exc:
                print(f'failed to fetch board page {board} index {page_index}: {exc}')
                report.failed_pages.append(page_index)
                continue

            soup = BeautifulSoup(resp.text, 'lxml')
            should_stop = False
            for article_id, link, pinned in self._index_entries(soup):
                published = self.article_id_datetime(article_id)
                published_date = published.date() if published else None

                if published_date is not None:
                    if end_date and published_date > end_date:
                        continue
                    if start_date and published_date < start_date:
                        # Pinned announcements sit at the bottom of the newest
                        # page and are years old; letting them count here used
                        # to end the crawl on the very first page.
                        if not pinned:
                            out_of_range_streak += 1
                            if out_of_range_streak >= OUT_OF_RANGE_THRESHOLD:
                                should_stop = True
                                break
                        continue
                    if not pinned:
                        out_of_range_streak = 0

                entries.append((article_id, link))

            if should_stop:
                break
            time.sleep(REQUEST_INTERVAL)

        self._retry_failed_pages(board, latest_page, start_date, end_date, report, entries, timeout)
        return entries

    def _retry_failed_pages(self, board, latest_page, start_date, end_date, report, entries, timeout):
        """Give index pages that failed outright one more pass.

        A dropped index page silently costs the day 20 articles, so failures are
        collected during the sweep and retried once the pressure of the sweep is
        over. Pages still failing here stay in the report and fail the run.
        """
        pending = list(report.failed_pages)
        if not pending:
            return
        print(f'retrying {len(pending)} failed index page(s) on {board}')
        report.failed_pages = []
        seen = {article_id for article_id, _ in entries}
        for page_index in pending:
            time.sleep(RETRY_BACKOFF)
            try:
                resp = fetch_with_retry(
                    self.session,
                    self._build_index_url(board, page_index, latest_page),
                    timeout=timeout
                )
            except (requests.exceptions.RequestException, ValueError) as exc:
                print(f'failed to fetch board page {board} index {page_index}: {exc}')
                report.failed_pages.append(page_index)
                continue

            soup = BeautifulSoup(resp.text, 'lxml')
            for article_id, link, pinned in self._index_entries(soup):
                if article_id in seen:
                    continue
                published = self.article_id_datetime(article_id)
                published_date = published.date() if published else None
                if published_date is not None:
                    if end_date and published_date > end_date:
                        continue
                    if start_date and published_date < start_date:
                        continue
                entries.append((article_id, link))
                seen.add(article_id)

    def _crawl_by_date_range(self, board, start_date, end_date, filename,
                             timeout=DEFAULT_TIMEOUT, save_locally=False, label=None):
        board = self.resolve_board_name(board, timeout=timeout)
        report = CrawlReport(board, label or 'all')
        entries = self._collect_index_entries(
            board=board,
            start_date=start_date,
            end_date=end_date,
            report=report,
            timeout=timeout,
        )
        report.listed = len(entries)

        batch_saver = BatchSaver()
        local_data = []
        for article_id, link in entries:
            try:
                article = self.parse(link, article_id, board, timeout=timeout, session=self.session)
            except Exception as exc:
                print(f'failed to parse article on {board}: {exc}')
                report.failed.append(article_id)
                continue

            # The id timestamp is when composing started; the meta date is the
            # authoritative publish time and decides what actually gets stored.
            article_date = self.article_date(article)
            if (start_date or end_date) and article_date is None:
                print(f'skipping article without datetime on {board}: {article_id}')
                report.out_of_range += 1
                continue
            if end_date and article_date > end_date:
                report.out_of_range += 1
                continue
            if start_date and article_date < start_date:
                report.out_of_range += 1
                continue

            report.fetched += 1
            batch_saver.add(article)
            if save_locally:
                local_data.append(article)
            time.sleep(REQUEST_INTERVAL)

        batch_saver.flush()
        report.saved = batch_saver.saved_count
        report.save_failures = batch_saver.failed_count
        if save_locally:
            self.store(filename, local_data)
        report.render()
        return report

    @staticmethod
    def _index_entries(soup):
        """Yield (article_id, link, pinned) for every article on an index page.

        Entries after the r-list-sep divider are pinned announcements: their
        dates say nothing about where the page sits in the board's history.
        """
        container = soup.find('div', class_='r-list-container')
        nodes = container.find_all('div', recursive=False) if container else soup.find_all('div', class_='r-ent')
        pinned = False
        for node in nodes:
            classes = node.get('class') or []
            if 'r-list-sep' in classes:
                pinned = True
                continue
            if 'r-ent' not in classes:
                continue
            anchor = node.find('a')
            if not anchor or not anchor.get('href'):
                continue  # deleted article
            href = anchor['href']
            article_id = re.sub(r'\.html$', '', href.split('/')[-1])
            yield article_id, PttWebCrawler.PTT_URL + href, pinned

    @staticmethod
    def article_id_datetime(article_id):
        """PTT article ids embed the publish epoch: M.<epoch>.A.<hash>."""
        match = re.match(r'^M\.(\d+)\.A', article_id)
        if not match:
            return None
        try:
            return datetime.fromtimestamp(int(match.group(1)), TZ8)
        except (ValueError, OverflowError, OSError):
            return None

    @staticmethod
    def article_date(article):
        datetime_utc8 = article.get('datetime_utc8')
        if not datetime_utc8:
            return None
        return datetime.strptime(datetime_utc8, '%Y-%m-%d %H:%M:%S').date()

    @staticmethod
    def today():
        return datetime.now(TZ8).date()

    @staticmethod
    def parse_date_arg(date_text):
        return datetime.strptime(date_text, '%Y-%m-%d').date()

    def _build_index_url(self, board, page_index, latest_page):
        if page_index == latest_page:
            return f'{self.PTT_URL}/bbs/{board}/index.html'
        return f'{self.PTT_URL}/bbs/{board}/index{page_index}.html'

    @classmethod
    def _board_session(cls):
        session = requests.Session()
        session.headers.update(cls.DEFAULT_HEADERS)
        session.cookies.update({'over18': '1'})
        return session

    @classmethod
    def resolve_board_name(cls, board, timeout=DEFAULT_TIMEOUT):
        """Return the board's canonical spelling as PTT reports it.

        PTT resolves board urls case-insensitively, so a misspelled board still
        returns 200 while writing an inconsistent `board` value to MongoDB.
        """
        session = cls._board_session()
        try:
            resp = fetch_with_retry(session, f'{cls.PTT_URL}/bbs/{board}/index.html', timeout=timeout)
        except (requests.exceptions.RequestException, ValueError) as exc:
            print(f'cannot resolve canonical name of board {board}: {exc}')
            return board
        match = re.search(r'href="/bbs/([^/"]+)/index\d*\.html"', resp.content.decode('utf-8'))
        if not match:
            return board
        canonical = match.group(1)
        if canonical != board:
            print(f'board name normalized: {board} -> {canonical}')
        return canonical

    @staticmethod
    def getLastPage(board, timeout=DEFAULT_TIMEOUT):
        session = PttWebCrawler._board_session()
        resp = fetch_with_retry(
            session,
            f'{PttWebCrawler.PTT_URL}/bbs/{board}/index.html',
            timeout=timeout
        )
        content = resp.content.decode('utf-8')
        # Do not embed the requested board name: PTT echoes its own spelling, so
        # a case mismatch would silently fall through to page 1.
        first_page = re.search(r'href="/bbs/[^/"]+/index(\d+)\.html">&lsaquo;', content)
        if first_page is None:
            print(f'no paging link on {board}; treating the board as a single page')
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
