#! /usr/bin/env python
# *- coding: utf-8 -*
from bs4 import BeautifulSoup
import urllib2
import time
import sqlite3

hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}

html_folder = 'pages'
quotes_folder = 'quotes'
link = 'http://bash.im/index/'
delim = '=' * 60
db_name = 'bash.db'


def get_pages(start, stop):
    for pagenum in xrange(start, stop):
        site = link + str(pagenum) + '.html'
        req = urllib2.Request(site, headers=hdr)
        print 'Fetching page {}...'.format(site)
        try:
            response = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            print e.fp.read()
        page = response.read()
        print 'Done!'

        filename = html_folder + '/' + str(pagenum) + '.html'
        with open(filename, 'w') as f:
            f.write(page)
        print 'Saved page to {}\n'.format(filename)

        time.sleep(1)


def parse_page(filename):
    with open(filename) as f:
        page = f.read()

    soup = BeautifulSoup(page, 'html.parser')

    res = []
    for el in soup.findAll("div", {"class": "quote"}):
        text = el.find("div", {"class": "text"})
        if text:
            text = str(text).replace('<div class="text">', '')
            text = text.replace('</div>', '')
            text = text.replace('<br>', '\n')
            text = text.replace('</br>', '')
            text = text.replace('<br/>', '')
            text = text.replace('&lt;', '<')
            text = text.replace('&gt;', '>')
            text = text.replace('\n\n', '\n')
            text = text.strip().decode('utf-8')
        else:
            continue

        acts = el.find("div", {"class": "actions"})
        date = acts.find("span", {"class": "date"}).string.strip()
        q_id = int(acts.find("a", {"class": "id"}).string.strip()[1:])
        try:
            rate = int(acts.find("span", {"class": "rating"}).string.strip())
        except ValueError:
            rate = None

        res.append((q_id, date, rate, text))

    return res


def save_quotes(start, stop):
    for pagenum in xrange(start, stop):
        filename = html_folder + '/' + str(pagenum) + '.html'
        quotes = parse_page(filename)

        quote_filename = quotes_folder + '/' + str(pagenum) + '.txt'
        with open(quote_filename, 'w') as f:
            for num, date, rate, text in quotes:
                data = delim + '\n'
                data += 'Quote: {}, date: {}, rate: {} text:\n {}\n'.format(num, date, rate, text.encode('utf-8'))
                f.write(data)
        print 'Saved {} page quotes. Count: {}'.format(pagenum, len(quotes))


def save_to_db(start, stop):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    for pagenum in xrange(start, stop):
        filename = html_folder + '/' + str(pagenum) + '.html'
        quotes = parse_page(filename)
        cur.executemany("INSERT INTO quotes VALUES (?, ?, ?, ?)", quotes)
        conn.commit()
        print 'Saved {} page quotes. Count: {}'.format(pagenum, len(quotes))
    conn.close()


def create_db():
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("DROP TABLE quotes")

    c.execute('''CREATE TABLE quotes
                 (id INTEGER PRIMARY KEY,
                 date TEXT,
                 rate INTEGER DEFAULT 0,
                 quote TEXT NOT NULL)''')

    cur.execute("CREATE UNIQUE INDEX ID_INDEX ON quotes (id)")

    conn.commit()
    conn.close()

if __name__ == '__main__':
    # get_pages(1, 1119)
    # save_quotes(1, 1119)
    # create_db()
    # save_to_db(1, 1119)

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # cut.execute("INSERT INTO quotes VALUES (?, ?, ?, ?)", (1, '12-34-56', 100, 'Hello world!'))

    # cur.execute("SELECT COUNT(*) FROM quotes")
    # print 'count: {}'.format(cur.fetchone())

    # cur.execute('SELECT MAX(rate) FROM quotes')
    # print 'rate id: {}'.format(cur.fetchone()[0])

    # cur.execute('SELECT MIN(rate) FROM quotes')
    # print 'rate id: {}'.format(cur.fetchone()[0])

    # cur.execute('SELECT quote, rate FROM quotes WHERE rate >= ? LIMIT ?', (150000, 3))
    # top_q = cur.fetchone()[0].encode('utf-8')

    cur.execute("""
        SELECT rate, quote
        FROM quotes
        WHERE rate >= ?
        ORDER BY rate DESC
        LIMIT ?
        """, (150000, 5))
    for rate, text in cur.fetchall():
        print 'rate: {}, text: {}\n\n'.format(rate, text.encode('utf-8'))

    # cur.execute("SELECT quote FROM quotes WHERE id = ?", (401241,))
    # print cur.fetchone()[0].encode('utf-8')

    # for item in cur.fetchall():
    #     print item[3].encode('utf-8'), '\n\n'

    cur.close()
    conn.close()
