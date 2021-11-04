import sqlite3
import json

DBPATH = 'markov.sqlite3'

def main():
    db = sqlite3.connect(DBPATH)
    keyword = input('キーワード: ')
    print('='*30)
    keyword = '%' + keyword + '%'

    res = db.execute('SELECT key, value FROM items WHERE key LIKE ?', (keyword,))

    for key, value in res:
        values = json.loads(value)
        values = ','.join(values.keys())
        print(key+' : '+values)
    print('='*30)

if __name__ == '__main__':
    while 1:
        main()
