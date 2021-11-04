import sqlite3
import json

DBPATH = 'markov.sqlite3'

def main(dbpath, keyword):
    db = sqlite3.connect(dbpath)
    print('='*30)
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

    args = parser.parse_args()

    if args.keyword:
        main(args.filepath, args.keyword)
    else:
        while 1:
            main(args.filepath, input('キーワード: '))
