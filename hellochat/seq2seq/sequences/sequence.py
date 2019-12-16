import re
import unicodedata
from abc import ABC


def unicode_to_ascii(string):
    return ''.join(c for c in unicodedata.normalize('NFD', string) if unicodedata.category(c) != 'Mn')


def preprocess_sentence(words):
    w = unicode_to_ascii(words.lower().strip())
    w = re.sub(r"([?.!,¿])", r" \1 ", w)
    w = re.sub(r'[" "]+', " ", w)
    w = re.sub(r"[^a-zA-Z?.!,¿]+", " ", w)
    w = w.rstrip().strip()
    w = '<start> ' + w + ' <end>'
    return w


class Sequence(ABC):
    table_name = None

    def __init__(self, table_name):
        self.table_name = table_name
        self.check_db(table_name)

    def check_db(self, table_name):
        pass
