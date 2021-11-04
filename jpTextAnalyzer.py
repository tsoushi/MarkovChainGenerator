import re
import json
import sqlite3
import logging
from janome.tokenizer import Tokenizer


class Analyzer:
    DBPATH = 'markov.sqlite3'
    def __init__(self, text=None):
        self._logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        self.nodes = []

        if text:
            self.analyze(text)

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

    def makeMarkov(self, wordNum=1, dic={}, key_tuple=False, value_simple=True):
        self._logger.debug('making markov : option (wordNum:{}, dict input:{}, key_tuple:{} value\simple:{}'.format(wordNum, dic=={}, key_tuple, value_simple))
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

    def saveMarkov_sqlite(self, wordNum=0):
        self._logger.debug('saving markov to database')
        markov = self.makeMarkov(wordNum, key_tuple=False, value_simple=False)
        self._logger.debug('connecting to database "{}"'.format(self.DBPATH))
        db = sqlite3.connect(self.DBPATH)
        for in_key, in_value in markov.items():
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

            db.execute('UPDATE items SET value = ? WHERE key = ?', (json.dumps(out_value), in_key))
        self._logger.debug('committing to database')
        db.commit()
        db.close()
        self._logger.debug('database is closed')
        self._logger.debug('saving is complete')
        return

    def initDb(self):
        db = sqlite3.connect(self.DBPATH)
        db.execute('CREATE TABLE items(key TEXT PRIMARY KEY, value TEXT);')
        db.commit()
        db.close()
            

if __name__ == '__main__':
    LOGLEVEL = logging.DEBUG
    logger = logging.getLogger(__name__)
    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(LOGLEVEL)
    streamHandler.setFormatter(logging.Formatter('%(levelname)s - %(name)s - %(message)s'))
    logger.addHandler(streamHandler)
    logger.setLevel(LOGLEVEL)


