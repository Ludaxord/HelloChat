import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from hellochat.utils.sources.compression import Compression
from hellochat.utils.tools.printers import print_red, print_yellow, print_green, print_blue, print_magenta, print_cyan


class Reddit(Compression):

    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/reddit")
        self.init_default_table()

    def init_default_table(self):
        self.cursor, self.connection = self.get_cursor()
        columns = dict(parent_id="TEXT PRIMARY KEY", comment_id="TEXT UNIQUE", parent="TEXT", comment="TEXT",
                       subreddit="TEXT", unix="INT", score="INT")
        self.create_table(self.cursor, "reddit_comments", columns)

    def create_data_portions(self, limit):
        if self.cursor is None and self.connection is None:
            self.cursor, self.connection = self.get_cursor()
            dataset = pd.read_sql(
                "SELECT * FROM reddit_comments WHERE parent NOT NULL AND parent != comment AND score > 0 ORDER BY unix ASC",
                self.connection)
            return dataset

    def set_values_to_db(self):
        row_counter = 0
        paired_rows = 0
        f_list = self._get_dir_files(self.destination_folder)
        dir_path = Path(__file__).parent.parent.parent.parent
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
                            if comment_score:
                                if score > comment_score:
                                    if self.__acceptable(body):
                                        self.insert_or_replace_comment(comment_id, parent_id, parent_data, body,
                                                                       subreddit,
                                                                       created_utc, score)
                            else:
                                if self.__acceptable(body):
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
            query = """UPDATE reddit_comments SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id = ?;""".format(
                parent_id, comment_id, parent, comment, subreddit, int(time), score, parent_id)
            print_magenta(f"update => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update comment on id {comment_id}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()

    def insert_parent(self, has_parent, parent_id, comment_id, parent, comment, subreddit, time, score):
        try:
            query = """INSERT INTO reddit_comments """
            if has_parent:
                query += """(parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}", "{}", "{}", "{}", "{}", "{}", "{}")""".format(
                    parent_id, comment_id, parent, comment,
                    subreddit, int(time), score)
            else:
                query += """(parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}", "{}", "{}", "{}", "{}", "{}")""".format(
                    parent_id, comment_id, comment,
                    subreddit, int(time), score)
            print_cyan(f"insert => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot insert parent comment of id {comment_id}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()

    def clean_rows(self, row_counter, data, start_row=0):
        if row_counter > start_row:
            if row_counter % len(data) == 0:
                print_magenta("cleaning up!")
                query = "DELETE FROM reddit_comments WHERE parent IS NULL"
                if self.cursor is None:
                    self.cursor, self.connection = self.get_cursor()
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
            query = "SELECT score FROM reddit_comments WHERE parent_id = '{}' LIMIT 1".format(pid)
            if self.cursor is None:
                self.cursor, self.connection = self.get_cursor()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print_red(f"cannot find score {str(e)}")
            self.cursor, self.connection = self.get_cursor()
            return False

    def __find_parent(self, pid):
        try:
            query = "SELECT comment FROM reddit_comments WHERE comment_id = '{}' LIMIT 1".format(pid)
            if self.cursor is None:
                self.cursor, self.connection = self.get_cursor()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print_red(f"cannot find parent {str(e)}")
            self.cursor, self.connection = self.get_cursor()
            return False

    def __format_data(self, data):
        data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
        return data
