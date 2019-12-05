import glob

import pandas as pd

from hellochat.utils.sources.compression import Compression
from hellochat.utils.tools.printers import print_green, print_blue, print_yellow, print_magenta, print_red, print_cyan, \
    print_gray


class Synonyms(Compression):
    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/synonyms")
        self.init_default_table()

    def init_default_table(self):
        self.cursor, self.connection = self.get_cursor()
        columns = dict(synonym_id="INTEGER PRIMARY KEY AUTOINCREMENT", synonym="TEXT", synonym_parent="TEXT")
        self.create_table(self.cursor, "synonyms_dictionary", columns)

    def get_json_files(self):
        json_files = glob.glob(f"{self.destination_folder}/odm.txt")
        return json_files

    def set_values_to_db(self):
        json_files = self.get_json_files()
        print_green(json_files)
        for json_file in json_files:
            print_green(json_file)
            with open(json_file, 'r') as temp_f:
                col_count = [len(li.split(",")) for li in temp_f.readlines()]
            column_names = [i for i in range(0, max(col_count))]
            dataset = pd.read_csv(json_file, header=None, delimiter=",", names=column_names, error_bad_lines=False)
            self.dataset = dataset
            for index, data in dataset.iterrows():
                print_yellow(f"index => {index}")
                parent_verb = None
                for i, d in enumerate(data):
                    if not pd.isnull(d):
                        if i == 0:
                            parent_verb = d.strip()
                        else:
                            verb = d.strip()
                            print_magenta(f"parent_verb => {parent_verb}")
                            print_blue(f"verb => {verb}")
                            synonym_id = self.__find_synonym(verb)
                            print_gray(f"synonym_id => {synonym_id}")
                            self.__set_synonyms(synonym_id, verb, parent_verb)

    def __set_synonyms(self, synonym_id, verb, parent_verb, with_update=False):
        if not with_update:
            # Skip updating
            if not synonym_id:
                self.__set_synonym(verb, parent_verb)
        else:
            # With updating
            if synonym_id:
                self.__update_synonym(verb, parent_verb, synonym_id)
            else:
                self.__set_synonym(verb, parent_verb)

    def __find_synonym(self, verb):
        try:
            query = "SELECT synonym_id FROM synonyms_dictionary WHERE synonym = '{}' LIMIT 1".format(verb)
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

    def __update_synonym(self, verb, parent_verb, synonym_id):
        try:
            # verb = verb.replace("'", '"')
            # parent_verb = parent_verb.replace("'", '"')
            query = f"UPDATE synonyms_dictionary SET synonym = '{verb}', synonym_parent = '{parent_verb}' WHERE synonym_id = {synonym_id};"
            print_magenta(f"update => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {synonym_id}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()

    def __set_synonym(self, verb, parent_verb):
        try:
            # verb = verb.replace('"', "'")
            # parent_verb = parent_verb.replace('"', "'")
            query = """INSERT INTO synonyms_dictionary(synonym, synonym_parent) VALUES ("{}","{}")""".format(verb,
                                                                                                             parent_verb)
            print_cyan(f"set => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {verb}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()
