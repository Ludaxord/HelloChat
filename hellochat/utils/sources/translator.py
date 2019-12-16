import glob
import os

import pandas as pd

from hellochat.seq2seq.sequences.translator_sequences import TranslatorSequences
from hellochat.utils.sources.compression import Compression
from hellochat.utils.tools.printers import print_green, print_yellow, print_red, print_magenta, print_cyan, print_gray


class Translator(Compression):
    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/translator")
        self.init_default_table()

    def init_default_table(self):
        self.cursor, self.connection = self.get_cursor()
        columns = dict(translate_id="INTEGER PRIMARY KEY AUTOINCREMENT", non_translate="TEXT", translate="TEXT",
                       language="TEXT", translate_language="TEXT")
        self.create_table(self.cursor, "translator", columns)

    def run_tensorflow(self, lang=None):
        translator_sequences = TranslatorSequences("translator", self.cursor, self.connection)
        translator_sequences.init_preprocess(lang)

    def run_nltk(self):
        pass

    def get_json_files(self):
        json_files = glob.glob(f"{self.destination_folder}/*.txt")
        return json_files

    def set_values_to_db(self):
        json_files = self.get_json_files()
        for json_file in json_files:
            print_green(json_file)
            translate_language = self.__find_language(json_file)
            print_cyan(f"translate language => {translate_language}")
            with open(json_file, 'r') as temp_f:
                col_count = [len(li.split(",")) for li in temp_f.readlines()]
            column_names = [i for i in range(0, max(col_count))]
            dataset = pd.read_csv(json_file, header=None, delimiter="\t", names=column_names, error_bad_lines=False)
            self.dataset = dataset
            for index, data in dataset.iterrows():
                print_yellow(f"index => {index}")
                non_translate = None
                for i, d in enumerate(data):
                    if not pd.isnull(d):
                        if i == 0:
                            non_translate = d
                        elif i == 1:
                            translate = d
                            translate_id = self.__find_translation(translate, non_translate)
                            print_gray(f"translate_id => {translate_id}")
                            if translate_id:
                                self.__update_translation(translate, non_translate, "en", translate_language,
                                                          translate_id)
                            else:
                                self.__set_translation(translate, non_translate, "en", translate_language)

    def __find_language(self, file_name):
        base_name = os.path.basename(file_name)
        return os.path.splitext(base_name)[0]

    def __find_translation(self, translate, non_translate):
        try:
            translate = translate.replace("'", '"')
            non_translate = non_translate.replace("'", '"')
            query = "SELECT translate_id FROM translator WHERE translate = '{}' AND non_translate = '{}' LIMIT 1".format(
                translate, non_translate)
            if self.cursor is None:
                self.cursor, self.connection = self.get_cursor()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print_red(f"cannot find synonym {str(e)}")
            self.cursor, self.connection = self.get_cursor()
            return False

    def __update_translation(self, translate, non_translate, language, translate_language, translate_id):
        try:
            translate = translate.replace("'", '"')
            non_translate = non_translate.replace("'", '"')
            query = f"UPDATE translator SET translate = '{translate}', non_translate = '{non_translate}', language = '{language}', translate_language = '{translate_language}' WHERE translate_id = {translate_id};"
            print_magenta(f"update => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {translate_id}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()

    def __set_translation(self, translate, non_translate, language, translate_language):
        try:
            translate = translate.replace('"', "'")
            non_translate = non_translate.replace('"', "'")
            query = """INSERT INTO translator(translate, non_translate, language, translate_language) VALUES ("{}","{}","{}","{}")""".format(
                translate, non_translate, language, translate_language)
            print_cyan(f"set => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {translate}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()
