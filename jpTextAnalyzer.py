import re
import time
import datetime
import json
import sqlite3
import logging
from janome.tokenizer import Tokenizer

class ProgressView:
    def __init__(self):
        self.name = ''
        self.end = 0
        self.freq = 1
        self._lastTime = 0

    def set(self, name, end):
        self.name = name
        self.end = end
        self.startTime = time.time()

    def setFreq(self, freq):
        self.freq = freq

    def setEnable(self, enable):
        if enable:
            self._enabled = True
        else:
            self._enabled = False

    def setVerbose(self, verbose):
        if verbose:
            self._verbose = True
        else:
            self._verbose = False

    def update(self, current):
        if self._enabled:
            now = time.time()
            if (now - self._lastTime) > self.freq:
                if self._verbose:
                    per = (current/self.end)
                    elapsedTime = now - self.startTime
                    speed = current/elapsedTime if elapsedTime != 0 else 0
                    predict = elapsedTime*(1-per)/per
                    predict_datetime = (datetime.datetime.now() + datetime.timedelta(seconds=predict)).strftime('%Y-%m-%d %H:%M:%S')
                    print('\r{} : {:>7.3f} %   {} / {}  speed:{:>.1f}/s  残り:{:>.1f}s  時刻:{}'.format(
                        self.name, per*100, current, self.end,
                        speed, predict, predict_datetime), end='')
                else:
                    print('\r{} : {:>7.3f} %   {} / {}'.format(self.name, (current/self.end)*100, current, self.end), end='')
                self._lastTime = now

            if current == self.end:
                print()


class Analyzer:
    def __init__(self):
        self._logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        self.nodes = []
        self.DBPATH = 'markov.sqlite3'
        self.progressView = ProgressView()

    def _getDb(self):
        self._logger.debug('connecting database "{}"'.format(self.DBPATH))
        return sqlite3.connect(self.DBPATH)

    def analyze(self, text):
        self._logger.info('analyzing')

        text = re.sub('[^\S\n]+', '', text)
        lines = re.split('\n+', text)

        nodes = []
        tokenizer = Tokenizer()
        self._logger.info('tokenizing : {} lines'.format(len(lines)))
        self.progressView.set('tokenize', len(lines))
        for i, line in enumerate(lines):
            self.progressView.update(i+1)
            nodes.extend(tokenizer.tokenize(line))
        self._logger.info('tokenizing is completed. result : {} words'.format(len(nodes)))

        self.nodes.extend(nodes)
        self._logger.info('add to node. result : {} words'.format(len(self.nodes)))

        self._logger.info('analyzing is completed')

    def countWord(self, num=1, key_tuple=False):
        self._logger.info('counting words')
        result = {}

        self.progressView('counting word', len(self.nodes))
        for i in range(num-1, len(self.nodes)):
            self.progressView.update(i+1)

            currentNodes = self.nodes[i-num+1:i+1]
            key = tuple((i.node.surface for i in currentNodes))
            if not key_tuple:
                key = ''.join(key)
            if key not in result:
                result[key] = 0
            result[key] += 1
        result = sorted(result.items(), key=lambda i:i[1], reverse=True)

        self._logger.info('counting is completed')
        return result

    def makeMarkov(self, wordNum=1, dic={}, key_tuple=True, value_simple=True):
        if wordNum <= 0:
            self._logger.error('wordNum must be greater than 1')
            raise Exception('wordNum must be greater than 1')
        self._logger.info('making markov : option (wordNum:{}, dict input:{}, key_tuple:{} value\simple:{}'.format(wordNum, dic!={}, key_tuple, value_simple))
        num = wordNum + 1
        result = dic

        self.progressView.set('making markov', len(self.nodes))
        for i in range(num+1, len(self.nodes)):
            self.progressView.update(i+1)

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
        self._logger.info('making markov is completed')
        return result

    def saveMarkov_sqlite(self, wordNum=1, key_tuple=True):
        self._logger.info('saving markov to database')
        markov = self.makeMarkov(wordNum, key_tuple=key_tuple, value_simple=False)
        
        self.mergeMarkovToDb(markov, key_tuple)

        self._logger.info('saving is complete')
        return


    def mergeMarkovToDb(self, markov, key_tuple):
        self._logger.info('merging to db')
        self.checkDb()

        db = None
        try:
            db = self._getDb()
            markov_items = markov.items()

            self.progressView.set('merge to db', len(markov_items))
            for i, (in_key, in_value) in enumerate(markov_items):
                self.progressView.update(i+1)

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
            self._logger.info('committing to database')
            db.commit()
        except:
            self._logger.error('database writing error')
            raise
        finally:
            if db:
                db.close()
                self._logger.debug('database is closed')
        self._logger.info('merge is complete')

    def loadMarkovFromDb(self, dbpath, key_tuple=True):
        self._logger.info('loading markov from db')
        db = None
        try:
            db = sqlite3.connect(dbpath)
            res = db.execute('SELECT key, value FROM items;').fetchall()
        except:
            self._logger.error('database writing error')
            raise
        finally:
            if db:
                db.close()
                self._logger.debug('database is closed')

        markov = {}
        self.progressView.set('loading from db', len(res))
        for i, (key, value) in enumerate(res):
            self.progressView.update(i+1)
            if key_tuple:
                key = tuple(json.loads(key))
            markov[key] = json.loads(value)
        self._logger.info('loading is complete')

        return markov

    def mergeDbToDb(self, in_dbpath, key_tuple=True):
        self._logger.info('merging DB to DB')
        markov = self.loadMarkovFromDb(in_dbpath, key_tuple)
        self.mergeMarkovToDb(markov, key_tuple)
        self._logger.info('merging DB to DB is complete')


    def initDb(self):
        self._logger.info('initializing database')
        db = None
        try:
            db = self._getDb()
            db.execute('CREATE TABLE items(key TEXT PRIMARY KEY, value TEXT);')
            db.commit()
        except:
            self.logger.error('database initializing error')
        finally:
            if db:
                db.close()
                self._logger.debug('database is closed')
        self._logger.info('initializing is complete')

    def checkDb(self):
        self._logger.info('checking database')
        try:
            db = self._getDb()
            db.execute('select * FROM items LIMIT 1').fetchone()
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
        self._logger.info('cheking database is completed')

        
            
def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('command', type=str, choices=['markov', 'count', 'merge'], help='コマンドを入力')
    parser.add_argument('--input', '-i', type=str, help='読み込むファイルパス')
    parser.add_argument('--out', '-o', type=str, help='出力ファイルパス。markovの場合はsqlite3、countの場合はテキストファイル')
    parser.add_argument('--debug', '-d', default='info', choices=['debug', 'info'], type=str, help='表示するログレベル')
    parser.add_argument('--enc', '-e', default='utf-8', type=str, help='読み込むファイルのエンコード')
    parser.add_argument('--word_num', '-n', default=3, type=int, help='解析するときにキーとする単語数')
    parser.add_argument('--key_array', '-k', action='store_false', help='キーを配列形式にしない')
    parser.add_argument('--sep', '-s', default=':', type=str, help='出力時にkeyとvalueの間に入れるセパレーター')
    parser.add_argument('--hide_progress', '-hp', action='store_true', help='進捗度を表示しない')
    parser.add_argument('--hide_verbose', '-hv', action='store_true', help='進捗度の詳細を表示しない')
    parser.add_argument('--progress_freq', '-pf', default=100, type=int, help='進捗度を表示する頻度(ミリ秒)')

    args = parser.parse_args()

    if args.debug == 'debug':
        LOGLEVEL = logging.DEBUG
    elif args.debug == 'info':
        LOGLEVEL = logging.INFO
    logger = logging.getLogger(__name__)
    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(LOGLEVEL)
    streamHandler.setFormatter(logging.Formatter('%(levelname)s - %(name)s - %(message)s'))
    logger.addHandler(streamHandler)
    logger.setLevel(LOGLEVEL)

    analyzer = Analyzer()
    analyzer.progressView.setFreq(args.progress_freq/1000)
    analyzer.progressView.setEnable(not args.hide_progress)
    analyzer.progressView.setVerbose(not args.hide_verbose)

    if args.command == 'merge':
        analyzer.DBPATH = args.out
        analyzer.mergeDbToDb(args.input)

    if args.command in ['count', 'markov']:
        analyzer.analyze(open(args.input, encoding=args.enc).read())

        if args.command == 'count':
            logger.info('単語のカウントを行います')

            count = analyzer.countWord(args.word_num)
            texts = ''
            for key, value in count:
                texts += '{}{}{}'.format(key, args.sep, value) + '\n'
            if args.out:
                logger.info('ファイル書き出し中')
                f = open(args.out, 'w', encoding='utf-8')
                f.write(texts)
                f.close()
                logger.info('完了')
            else:
                print(texts)
        elif args.command == 'markov':
            logger.info('マルコフ連鎖の作成。sqliteでの出力を行います')
            if args.out:
                analyzer.DBPATH = args.out
            analyzer.saveMarkov_sqlite(args.word_num, key_tuple=args.key_array)
            logger.info('sqlite出力完了')
    logger.info('すべての操作が完了。')
            


if __name__ == '__main__':
    main()
