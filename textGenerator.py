import sqlite3
import re
import json
import random
import logging

logger = logging.getLogger(__name__)

class TextGenerator:
    def __init__(self, dbpath, ensure_ascii=False):
        self._logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        self._db = sqlite3.connect(dbpath)
        self._ensure_ascii = ensure_ascii

    def _getDb(self):
        return self._db

    def init(self, key=None):
        self._logger.info('initializing')

        if key:
            self._text = [*key]
        else:
            self._text = self.getRandomKey()

        self._logger.info('initialized with {}'.format(self._text))
        return 

    def searchKey(self, keyword):
        self._logger.info('searching key : {}'.format(keyword))
        keyword = '%' + '%'.join(keyword) + '%'
        res = self._getDb().execute('SELECT key FROM items WHERE key LIKE ?', (keyword,)).fetchall()
        if res:
            self._logger.info('key was found : {} keys'.format(len(res)))
            self._logger.debug('key was found : {}'.format(res))
            key = json.loads(random.choice(res)[0])
            self._logger.info('select key at random : {}'.format(key))
            return key
        else:
            self._logger.info('key was not found')
            return None

    def getRandomKey(self):
        self._logger.info('getting key at random')

        dataLength = self._getDb().execute('SELECT id FROM items ORDER BY id DESC LIMIT 1;').fetchone()[0]
        key = None
        while 1:
            res = self._getDb().execute('SELECT key FROM items WHERE id > ? LIMIT 1', (random.randint(0, dataLength-1),)).fetchone()
            if res:
                key = json.loads(res[0])
                break
            else:
                self._logger.info('key was not found > retry')

        self._logger.info('choosed key is ( {} )'.format(key))
        return key
        

    def generate(self, num=100):
        self._logger.info('generating text : {} words'.format(num))

        for i in range(num):
            res = self.getSuffix(self._text[-3:])
            if not res:
                self._logger.info('no candidate found')
                return i + 1
            self._text.append(res)
        return i + 1

    def getText(self, strip=False):
        text = ''.join(self._text)
        if strip:
            res = re.search('.*?([。「」].+[。「」]).*?', text)
            if res:
                result = res.group(1)
                if result[0] in '。」':
                    result = result[1:]
                if result.endswith('「'):
                    result = result[:-1]
            else:
                result = ''
        else:
            result = text
            
        return result


    def getSuffix(self, prefix):
        res = self.getSuffixesFromDb(prefix)
        if not res:
            return None
        return self.chooseSuffix_random(res)


    def chooseSuffix_random(self, suffixes):
        countSum = sum([i[1] for i in suffixes.items()])
        choosedNum = random.randint(0, countSum-1)

        self._logger.debug('choosing suffix at random : sum > {}'.format(countSum))
        for suffix, count in suffixes.items():
            choosedNum -= int(count)
            if choosedNum < 0:
                return suffix

    def getSuffixesFromDb(self, prefix):
        self._logger.debug('finding suffixes from database : key > {}'.format(prefix))
        key = json.dumps(prefix, ensure_ascii=self._ensure_ascii)
        res = self._getDb().execute('SELECT value FROM items WHERE key = ?;', (key,)).fetchone()
        if res:
            self._logger.debug('suffixes was found')
            return json.loads(res[0])
        else:
            self._logger.info('no suffixes found')
            return None



if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('dbpath', type=str, help='データベースのパス')
    parser.add_argument('--key', '-k', type=str, help='テキストを生成するときに最初に使うキーワード(,区切り)')
    parser.add_argument('--keyword', '-kw', type=str, help='キーワードでキーを検索する')
    parser.add_argument('--strip', '-s', action='store_true', help='文頭と文末の余計な部分をカットする')
    parser.add_argument('--debug', '-d', default='warning', choices=['debug', 'info', 'warning'], type=str, help='表示するログレベル')
    parser.add_argument('--length', '-l', default=100, type=int, help='生成する単語数')
    args = parser.parse_args()

    if args.debug == 'debug':
        LOGLEVEL = logging.DEBUG
    elif args.debug == 'info':
        LOGLEVEL = logging.INFO
    elif args.debug == 'warning':
        LOGLEVEL = logging.WARNING
    logger = logging.getLogger(__name__)
    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(LOGLEVEL)
    streamHandler.setFormatter(logging.Formatter('%(levelname)s - %(name)s - %(message)s'))
    logger.addHandler(streamHandler)
    logger.setLevel(LOGLEVEL)

    generator = TextGenerator(args.dbpath)

    if args.key:
        key = args.key.split(',')
    elif args.keyword:
        res = generator.searchKey(args.keyword)
        if res:
            key = res
        else:
            print('キーが見つかりませんでした')
            exit()
    else:
        key = None

    generator.init(key)
    generator.generate(args.length)
    res = generator.getText(strip=args.strip)
    print(res)
