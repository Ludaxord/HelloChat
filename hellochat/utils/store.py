import json
import sqlite3

from hellochat.utils.compression import Compression


class Store(Compression):
    sql_transaction = []
    c = None

    def __init__(self, destination_folder):
        super().__init__(destination_folder)

    def init_default_table(self):
        self.c = self.__get_cursor()
        columns = dict(parent_id="TEXT PRIMARY KEY", comment_id="TEXT UNIQUE", parent="TEXT", comment="TEXT",
                       subreddit="TEXT", unix="INT", score="INT")
        self.create_table(self.c, "parent_reply", columns)

    def init_downloaded_data(self):
        row_counter = 0
        paired_rows = 0
        f_list = self.__get_dir_files(self.destination_folder)
        for f_name in f_list:
            with open(f_name, buffering=1000) as f:
                data = json.load(f)
                for element in data:
                    parent_id = element['parent_id']
                    body = self.__format_data(element['body'])
                    created_utc = element['created_utc']
                    score = element['score']
                    comment_id = element['name']
                    subreddit = element['subreddit']
                    parent_data = self.__find_parent(parent_id)
                    if score >= 2:
                        comment_score = self.__find_score(parent_id)
                        if score > comment_score:
                            pass

    def __find_score(self, pid):
        try:
            query = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
            if self.c is None:
                self.c = self.__get_cursor()
            self.c.execute(query)
            result = self.c.fetchone()
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print(f"cannot find score {str(e)}")
            return False

    def __find_parent(self, pid):
        try:
            query = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
            if self.c is None:
                self.c = self.__get_cursor()
            self.c.execute(query)
            result = self.c.fetchone()
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print(f"cannot find parent {str(e)}")
            return False

    def create_table(self, cursor, table_name, columns_dict):
        columns = ""
        for index, column_name, column_type in enumerate(columns_dict):
            if index == len(columns_dict) - 1:
                columns += f"{column_name} {column_type}"
            else:
                columns += f"{column_name} {column_type},"

        columns = f"({columns})"
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name}{columns}")

    def __get_cursor(self):
        connection = sqlite3.connect("hello_chat_main.db")
        cursor = connection.cursor()
        return cursor

    def __format_data(self, data):
        data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
        return data
