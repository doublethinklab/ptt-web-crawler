# ptt-web-crawler (PTT 網路版爬蟲) [![Build Status](https://travis-ci.org/jwlin/ptt-web-crawler.svg?branch=master)](https://travis-ci.org/jwlin/ptt-web-crawler)

### [English Readme](#english_desc)
### [Live demo](http://app.castman.net/ptt-web-crawler)
### [Scrapy 版本](https://github.com/afunTW/ptt-web-crawler) by afunTW

特色

* 支援單篇及多篇文章抓取
* 過濾資料內空白、空行及特殊字元
* JSON 格式輸出
* 支援 Python 2.7, 3.4-3.6

輸出 JSON 格式
```
{
    "article_id": 文章 ID,
    "article_title": 文章標題 ,
    "author": 作者,
    "board": 板名,
    "content": 文章內容,
    "date": 發文時間,
    "ip": 發文位址,
    "message_count": { # 推文
        "all": 總數,
        "boo": 噓文數,
        "count": 推文數-噓文數,
        "neutral": → 數,
        "push": 推文數
    },
    "messages": [ # 推文內容
      {
        "push_content": 推文內容,
        "push_ipdatetime": 推文時間及位址,
        "push_tag": 推/噓/→ ,
        "push_userid": 推文者 ID
      },
      ...
      ]
}
```

### 參數說明

```commandline
python crawler.py -b 看板名稱 -i 起始索引 結束索引 (設為負數則以倒數第幾頁計算) 
python crawler.py -b 看板名稱 -a 文章ID 
python crawler.py -b 看板名稱 --mode daily --date YYYY-MM-DD --days N
python crawler.py -b 看板名稱 --mode daily --offset N
python crawler.py -b 看板名稱 --mode scan
python crawler.py -b 看板名稱 --mode all
```

`--date` 與 `--offset` 皆以 UTC+8 計算，與伺服器時區無關。`--offset N` 代表「今天往前推 N 天」，
未指定 `--date` 時使用；兩者同時給定時以 `--date` 為準。

### 範例

爬取 PublicServan 板第 100 頁 (https://www.ptt.cc/bbs/PublicServan/index100.html) 
到第 200 頁 (https://www.ptt.cc/bbs/PublicServan/index200.html) 的內容，
輸出至 `PublicServan-100-200.json`

* 直接執行腳本

```commandline
cd PttWebCrawler
python crawler.py -b PublicServan -i 100 200
```
    
* 呼叫 package

```commandline
python setup.py install
python -m PttWebCrawler -b PublicServan -i 100 200
```

* 作為函式庫呼叫

```python
from PttWebCrawler.crawler import *

c = PttWebCrawler(as_lib=True)
c.parse_articles(100, 200, 'PublicServan')
c.parse_articles_by_date('PublicServan', target_date=datetime(2026, 3, 20).date(), days=1)
c.scan_new_articles('PublicServan')
c.parse_all_articles('PublicServan')
```

### 依日期抓取

```commandline
# 抓指定日期的全部文章
python -m PttWebCrawler -b Gossiping --mode daily --date 2026-03-20

# 從指定日期往前抓 7 天
python -m PttWebCrawler -b Gossiping --mode daily --date 2026-03-20 --days 7

# 抓前一天的全部文章 (UTC+8)
python -m PttWebCrawler -b Gossiping --mode daily --offset 1

# 抓整個板的全部文章
python -m PttWebCrawler -b Gossiping --mode all
```

### 日內增量掃描 (scan)

```commandline
python -m PttWebCrawler -b Gossiping --mode scan
```

`scan` 只讀看板列表頁，用文章 ID 內嵌的發文時間 (`M.<epoch>.A.xxx`) 判斷日期，
再比對 MongoDB 已存的 `article_id`，只抓沒抓過的文章。

用途是在當天稍後補上會在隔夜完整補抓前被刪除的文章。成本約為每個列表頁一次請求，
若當天文章都已抓過則完全不發文章請求。

### 完整度檢查

每個看板跑完會輸出一行統計：

```
[Gossiping] 2026-09-16 listed=857 fetched=857 saved=857 existing=0 out_of_range=0 failed=0 failed_pages=0 save_failures=0 coverage=100.0%
```

* `listed` — 列表頁判定屬於該日期區間的文章數
* `out_of_range` — ID 時間落在區間內、但文章實際發文時間不在區間內 (跨日邊界的正常現象)
* `coverage` — `(saved + existing + out_of_range) / listed`

`coverage` 低於 99%、有文章抓取失敗、有列表頁抓取失敗、或有寫入失敗時，
行程會以 exit code 1 結束，方便 cron 或監控察覺。

### 排程

`run.sh` 接受 `daily` (預設) 與 `scan` 兩種模式：

```commandline
# 每天 02:00 (UTC+8) 完整補抓前一天，涵蓋全部看板
0 18 * * * /root/ptt-web-crawler/run.sh daily

# 每天 10:00 / 14:00 / 18:00 / 22:00 (UTC+8) 對 HatePolitics、Gossiping 做增量掃描
0 2,6,10,14 * * * /root/ptt-web-crawler/run.sh scan
```

日誌寫到 `logs/ptt-<mode>-<YYYYMMDD>.log`。

### 測試
```commandline
python test.py
```

***

<a name="english_desc"></a>ptt-web-crawler is a crawler for the web version of PTT, the largest online community in Taiwan. 

    usage: python crawler.py [-h] -b BOARD_NAME (-i START_INDEX END_INDEX | -a ARTICLE_ID) [-v]
    optional arguments:
      -h, --help                  show this help message and exit
      -b BOARD_NAME               Board name
      -i START_INDEX END_INDEX    Start and end index
      -a ARTICLE_ID               Article ID
      -v, --version               show program's version number and exit

Output would be `BOARD_NAME-START_INDEX-END_INDEX.json` (or `BOARD_NAME-ID.json`)
