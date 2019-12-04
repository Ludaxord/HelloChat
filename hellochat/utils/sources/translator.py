import glob
import io
import re
import unicodedata

from hellochat.utils.sources.compression import Compression


class Translator(Compression):
    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/translator")
        self.init_default_table()

    def init_default_table(self):
        self.cursor, self.connection = self.get_cursor()
        columns = dict(translate_id="INTEGER PRIMARY KEY AUTOINCREMENT", non_translate="TEXT", translate="TEXT",
                       language="TEXT")
        self.create_table(self.cursor, "translator", columns)

    def get_json_files(self):
        json_files = glob.glob(f"{self.destination_folder}/*.txt")
        return json_files

    def __unicode_to_ascii(self, string):
        return ''.join(c for c in unicodedata.normalize('NFD', string) if unicodedata.category(c) != 'Mn')

    def __preprocess_sentence(self, words):
        w = self.__unicode_to_ascii(words.lower().strip())
        # Reference:- https://stackoverflow.com/questions/3645931/python-padding-punctuation-with-white-spaces-keeping-punctuation
        w = re.sub(r"([?.!,¿])", r" \1 ", w)
        w = re.sub(r'[" "]+', " ", w)
        # replacing everything with space except (a-z, A-Z, ".", "?", "!", ",")
        w = re.sub(r"[^a-zA-Z?.!,¿]+", " ", w)
        w = w.rstrip().strip()
        w = '<start> ' + w + ' <end>'
        return w

    def __create_dataset(self, path, num_examples):
        lines = io.open(path, encoding='UTF-8').read().strip().split('\n')

        word_pairs = [[self.__preprocess_sentence(w) for w in li.split('\t')] for li in lines[:num_examples]]

        return zip(*word_pairs)
