import sys

from hellochat.utils.compression import Compression
import pandas as pd


class AppleIMessage(Compression):
    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/imessage")

    def __connect_with_i_message(self):
        cursor, connection = self.get_cursor(db="/Users/konraduciechowski/Library/Messages")
        return cursor, connection

    def get_imessages(self):
        cur, conn = self.__connect_with_i_message()
        cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        for name in cur.fetchall():
            print(name)
        messages = pd.read_sql_query("select * from message", conn)
        return messages.to_json()

    def load_imessages_from_file(self):
        dataset = pd.read_csv(f"{self.destination_folder}/imessage.csv", sep=',', encoding='utf-8',
                              error_bad_lines=False, low_memory=False,
                              usecols=['ROWID', 'guid', 'text', 'handle_id', 'date', 'date_read', 'date_delivered',
                                       'is_delivered', 'is_finished', 'is_emote', 'is_from_me', 'is_empty',
                                       'is_delayed', 'is_auto_reply', 'is_prepared', 'is_read', 'is_system_message',
                                       'is_sent', 'has_dd_results', 'is_spam', 'cache_has_attachments', 'item_type',
                                       'group_title', 'is_expirable', 'message_source', 'destination_caller_id',
                                       'ck_record_id', 'account', 'service'])
        return dataset
