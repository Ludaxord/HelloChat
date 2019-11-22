import json
import os
import sqlite3
from pathlib import Path

import pandas as pd
from datetime import datetime

from jsoncomment import JsonComment

from hellochat.utils.compression import Compression
from hellochat.utils.printers import print_red, print_yellow, print_green, print_blue, print_magenta, print_cyan


class Store(Compression):
    sql_transaction = []
    cursor = None
    connection = None

    def __init__(self, destination_folder):
        super().__init__(destination_folder)
        self.init_default_table()

    def init_default_table(self):
        self.cursor, self.connection = self.__get_cursor()
        columns = dict(parent_id="TEXT PRIMARY KEY", comment_id="TEXT UNIQUE", parent="TEXT", comment="TEXT",
                       subreddit="TEXT", unix="INT", score="INT")
        self.create_table(self.cursor, "parent_reply", columns)

    def create_data_portions(self, limit):
        if self.cursor is None and self.connection is None:
            self.cursor, self.connection = self.__get_cursor()
            dataset = pd.read_sql(
                "SELECT * FROM parent_reply WHERE parent NOT NULL and score > 0 ORDER BY unix ASC", self.connection)
            return dataset

    def create_table(self, cursor, table_name, columns_dict):
        columns = ""
        for index, (column_name, column_type) in enumerate(columns_dict.items()):
            if index == len(columns_dict) - 1:
                columns += f"{column_name} {column_type}"
            else:
                columns += f"{column_name} {column_type},"

        columns = f"({columns})"
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name}{columns}")

    def init_downloaded_data(self):
        row_counter = 0
        paired_rows = 0
        f_list = self._get_dir_files(self.destination_folder)
        dir_path = Path(__file__).parent.parent.parent
        for f_name in f_list:
            if f_name.suffix == '.json':
                file = f"{dir_path}/{f_name}"
                print_blue(file)

                with open(file, buffering=1000) as f:
                    for data in f:
                        element = json.loads(data)
                        # for element in data:
                        row_counter += 1
                        print_blue(element)
                        parent_id = element['parent_id']
                        body = self.__format_data(element['body'])
                        created_utc = element['created_utc']
                        score = element['score']
                        try:
                            comment_id = element['name']
                        except Exception as e:
                            print_yellow(f"comment id by name do not exists, {e}")
                        try:
                            comment_id = element['id']
                        except Exception as e:
                            print_yellow(f"comment id by id do not exists, {e}")
                        subreddit = element['subreddit']
                        parent_data = self.__find_parent(parent_id)
                        print_green(
                            f"parent_id => {parent_id}, body => {body}, created_utc => {created_utc}, comment_id => {comment_id}, subreddit => {subreddit}, parent_data => {parent_data}")
                        if score >= 2:
                            comment_score = self.__find_score(parent_id)
                            print_cyan(f"comment_score => {comment_score}")
                            if comment_score:
                                if score > comment_score:
                                    if self.__acceptable(body):
                                        print_magenta(f"acceptable => {self.__acceptable(body)}")
                                        self.insert_or_replace_comment(comment_id, parent_id, parent_data, body,
                                                                       subreddit,
                                                                       created_utc, score)
                            else:
                                if self.__acceptable(body):
                                    print_magenta(f"acceptable => {self.__acceptable(body)}")
                                    if parent_data:
                                        self.insert_parent(True, comment_id, parent_id, parent_data, body, subreddit,
                                                           created_utc, score)
                                        paired_rows += 1
                                    else:
                                        self.insert_parent(False, comment_id, parent_id, None, body, subreddit,
                                                           created_utc,
                                                           score)
                        self.display_rows(row_counter, data, paired_rows)
                        self.clean_rows(row_counter, data)
            else:
                print_red(f"file of name {f_name} is not a json file")

    def insert_or_replace_comment(self, comment_id, parent_id, parent, comment, subreddit, time, score):
        try:
            query = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id = ?;""".format(
                parent_id, comment_id, parent, comment, subreddit, int(time), score, parent_id)
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update comment on id {comment_id}, {str(e)}")

    def insert_parent(self, has_parent, parent_id, comment_id, parent, comment, subreddit, time, score):
        try:
            query = """INSERT INTO parent_reply """
            if has_parent:
                query += """(parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}", "{}", "{}", "{}", "{}", "{}", "{}")""".format(
                    parent_id, comment_id, parent, comment,
                    subreddit, int(time), score)
            else:
                query += """(parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}", "{}", "{}", "{}", "{}", "{}")""".format(
                    parent_id, comment_id, comment,
                    subreddit, int(time), score)
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot insert parent comment of id {comment_id}, {str(e)}")

    def transaction_bldr(self, query):
        self.sql_transaction.append(query)
        if len(self.sql_transaction) > 1000:
            if self.cursor is None:
                self.cursor, self.connection = self.__get_cursor()
            self.cursor.execute("BEGIN TRANSACTION")
            for s in self.sql_transaction:
                try:
                    self.cursor.execute(s)
                except Exception as e:
                    print_yellow(f"cannot execute query {s}, {str(e)}")
            self.connection.commit()
            self.sql_transaction = []

    def clean_rows(self, row_counter, data, start_row=0):
        if row_counter > start_row:
            if row_counter % len(data) == 0:
                print_magenta("cleaning up!")
                query = "DELETE FROM parent_reply WHERE parent IS NULL"
                if self.cursor is None:
                    self.cursor, self.connection = self.__get_cursor()
                self.cursor.execute(query)
                self.connection.commit()
                self.cursor.execute("VACUUM")
                self.connection.commit()

    def display_rows(self, row_counter, data, paired_rows):
        if row_counter % len(data) == 0:
            print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows,
                                                                          str(datetime.now())))

    def __acceptable(self, data):
        if len(data.split(" ")) > 50 or len(data) < 1:
            return False
        elif len(data) > 1000:
            return False
        elif data == '[deleted]':
            return False
        elif data == '[removed]':
            return False
        else:
            return True

    def __find_score(self, pid):
        try:
            query = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
            if self.cursor is None:
                self.cursor, self.connection = self.__get_cursor()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print_red(f"cannot find score {str(e)}")
            return False

    def __find_parent(self, pid):
        try:
            query = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
            if self.cursor is None:
                self.cursor, self.connection = self.__get_cursor()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print_red(f"cannot find parent {str(e)}")
            return False

    def __get_cursor(self):
        connection = sqlite3.connect("hello_chat_main.db")
        cursor = connection.cursor()
        return cursor, connection

    def __format_data(self, data):
        data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
        return data
