import re
import json
import sqlite3
import logging
from janome.tokenizer import Tokenizer


class Analyzer:
    def __init__(self, text=None):
        self._logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        self.nodes = []
        self.DBPATH = 'markov.sqlite3'

        if text:
            self.analyze(text)

    def _getDb(self):
        self._logger.debug('connecting database "{}"'.format(self.DBPATH))
        return sqlite3.connect(self.DBPATH)

    def analyze(self, text):
        self._logger.debug('analyzing')

        text = re.sub('[^\S\n]+', '', text)
        lines = re.split('\n+', text)

        nodes = []
        tokenizer = Tokenizer()
        self._logger.debug('tokenizing')
        for line in lines:
            nodes.extend(tokenizer.tokenize(line))
        self._logger.debug('tokenizing is completed. result : {} words'.format(len(nodes)))

        self.nodes.extend(nodes)
        self._logger.debug('add to node. result : {} words'.format(len(self.nodes)))

        self._logger.debug('analyzing is completed')

    def countWord(self, num=1, key_tuple=False):
        self._logger.debug('counting words')
        result = {}
        for i in range(num-1, len(self.nodes)):
            currentNodes = self.nodes[i-num+1:i+1]
            key = tuple((i.node.surface for i in currentNodes))
            if not key_tuple:
                key = ''.join(key)
            if key not in result:
                result[key] = 0
            result[key] += 1
        result = sorted(result.items(), key=lambda i:i[1], reverse=True)

        self._logger.debug('counting is completed')
        return result

    def makeMarkov(self, wordNum=1, dic={}, key_tuple=True, value_simple=True):
        if wordNum <= 0:
            self._logger.error('wordNum must be greater than 1')
            raise Exception('wordNum must be greater than 1')
        self._logger.debug('making markov : option (wordNum:{}, dict input:{}, key_tuple:{} value\simple:{}'.format(wordNum, dic!={}, key_tuple, value_simple))
        num = wordNum + 1
        result = dic
        for i in range(num+1, len(self.nodes)):
            currentNodes = self.nodes[i-num+1:i+1]
            key = tuple((i.node.surface for i in currentNodes[:-1]))
            value = currentNodes[-1].node.surface
            if not key_tuple:
                key = ''.join(key)
            if value_simple:
                if key not in result:
                    result[key] = []
                result[key].append(value)
            else:
                if key not in result:
                    result[key] = {}
                if value not in result[key]:
                    result[key][value] = 0
                result[key][value] += 1
        self._logger.debug('making markov is completed')
        return result

    def saveMarkov_sqlite(self, wordNum=1, key_tuple=True, title=None):
        self._logger.debug('saving markov to database')
        markov = self.makeMarkov(wordNum, key_tuple=key_tuple, value_simple=False)
        
        self.checkDb()

        db = None
        try:
            db = self._getDb()
            for in_key, in_value in markov.items():
                if key_tuple:
                    #キーを配列形式にした場合
                    in_key = json.dumps(in_key, ensure_ascii=False)
                res = db.execute('SELECT value FROM items WHERE key = ?', (in_key,)).fetchone()
                if res:
                    out_value = json.loads(res[0])
                else:
                    db.execute('INSERT INTO items VALUES(?, "{}")', (in_key,))
                    out_value = {}
                for in_value_key in in_value.keys():
                    if in_value_key not in out_value:
                        out_value[in_value_key] = 0
                    out_value[in_value_key] += in_value[in_value_key]

                db.execute('UPDATE items SET value = ? WHERE key = ?', (json.dumps(out_value, ensure_ascii=False), in_key))
            if title:
                #指定されていれば、タイトル名も一緒に保存する。
                db.execute('INSERT INTO titles(name) VALUES(?);', (title,))
            self._logger.debug('committing to database')
            db.commit()
        except:
            self._logger.error('database writing error')
            raise
        finally:
            if db:
                db.close()
                self._logger.debug('database is closed')

        self._logger.debug('saving is complete')
        return

    def initDb(self):
        self._logger.debug('initializing database')
        db = None
        try:
            db = self._getDb()
            db.executescript('''
CREATE TABLE IF NOT EXISTS items(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS titles(id INTEGER PRIMARY KEY, name TEXT, at TEXT DEFAULT CURRENT_TIMESTAMP);''')
            db.commit()
        except:
            self.logger.error('database initializing error')
        finally:
            if db:
                db.close()
                self._logger.debug('database is closed')

    def checkDb(self):
        self._logger.debug('checking database')
        try:
            db = self._getDb()
            db.execute('select * FROM items LIMIT 1').fetchone()
            db.execute('select * FROM titles LIMIT 1').fetchone()
            initialized = True
        except sqlite3.OperationalError:
            self._logger.info('database {} is not initialized')
            initialized = False
        finally:
            if db:
                db.close()
                self._logger.debug('database is closed')
        if not initialized:
            self.initDb()
        self._logger.debug('cheking database is completed')

        
            
def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('command', type=str, choices=['markov', 'count'], help='コマンドを入力')
    parser.add_argument('in_file', type=str, help='読み込むファイルパス')
    parser.add_argument('--word_num', '-n', default=1, type=int, help='解析するときにキーとする単語数')
    parser.add_argument('--enc', '-e', default='utf-8', type=str, help='読み込むファイルのエンコード')
    parser.add_argument('--out', '-o', type=str, help='出力ファイルパス。markovの場合はsqlite3、countの場合はテキストファイル')
    parser.add_argument('--sep', '-s', default=':', type=str, help='出力時にkeyとvalueの間に入れるセパレーター')
    parser.add_argument('--title', type=str, help='sqliteに出力するときに一緒に記録するタイトル名')
    parser.add_argument('--key_array', '-k', action='store_true', help='キーを単語ごとに分けた配列形式にする')

    args = parser.parse_args()

    analyzer = Analyzer(open(args.in_file, encoding=args.enc).read())

    if args.command == 'count':
        logger.debug('単語のカウントを行います')

        count = analyzer.countWord(args.word_num)
        texts = ''
        for key, value in count:
            texts += '{}{}{}'.format(key, args.sep, value) + '\n'
        if args.out:
            logger.debug('ファイル書き出し中')
            f = open(args.out, 'w', encoding='utf-8')
            f.write(texts)
            f.close()
            logger.debug('完了')
        else:
            print(texts)
    elif args.command == 'markov':
        logger.debug('マルコフ連鎖の作成。sqliteでの出力を行います')
        if args.out:
            analyzer.DBPATH = args.out
        analyzer.saveMarkov_sqlite(args.word_num, key_tuple=args.key_array, title=args.title)
        logger.debug('sqlite出力完了')
    logger.debug('すべての操作が完了。')
            


if __name__ == '__main__':
    LOGLEVEL = logging.DEBUG
    logger = logging.getLogger(__name__)
    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(LOGLEVEL)
    streamHandler.setFormatter(logging.Formatter('%(levelname)s - %(name)s - %(message)s'))
    logger.addHandler(streamHandler)
    logger.setLevel(LOGLEVEL)

    main()
