import sqlite3
import json

DBPATH = 'markov.sqlite3'

class MarkovSearcher:
    def __init__(self, dbpath):
        self.db = sqlite3.connect(dbpath)

    def close(self):
        self.db.close()

    def search(self, keyword, loose=False):
        db = self.db
        print('='*30)
        if loose:
            keyword = '%'.join(keyword)
        keyword = '%' + keyword + '%'

        res = db.execute('SELECT key, value FROM items WHERE key LIKE ?', (keyword,))

        for key, value in res:
            values = json.loads(value)
            values = ','.join(values.keys())
            print(key+' : '+values)
        print('='*30)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('filepath', default=DBPATH, type=str, help='データベースのパス')
    parser.add_argument('--keyword', '-k', type=str, help='検索キーワード')
    parser.add_argument('--loose', '-l', action='store_true', help='検索条件を緩くする')

    args = parser.parse_args()

    searcher = MarkovSearcher(args.filepath)

    if args.keyword:
        searcher.search(args.keyword, args.loose)
    else:
        while 1:
            keyword = input('キーワード: ')
            if keyword == '/quit':
                break
            searcher.search(keyword, args.loose)
    sercher.close()
