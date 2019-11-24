import datetime
import time

import pandas as pd

from hellochat.utils.compression import Compression
from hellochat.utils.tools.printers import print_red, print_magenta, print_cyan, print_green


class AppleIMessage(Compression):
    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/imessage")
        self.init_default_table()

    def __connect_with_i_message(self):
        cursor, connection = self.get_cursor(db="/Users/konraduciechowski/Library/Messages")
        return cursor, connection

    def init_default_table(self):
        self.cursor, self.connection = self.get_cursor()
        columns = dict(guid="TEXT PRIMARY KEY", text="TEXT", handle_id="INT", date="INT", date_read="INT",
                       date_delivered="INT",
                       is_delivered="INT", is_finished="INT", is_emote="INT", is_from_me="INT", is_empty="INT",
                       is_delayed="INT", is_auto_reply="INT", is_prepared="INT", is_read="INT", is_system_message="INT",
                       is_sent="INT", has_dd_results="INT", is_spam="INT", cache_has_attachments="INT", item_type="INT",
                       group_title="TEXT", is_expirable="INT", message_source="INT", destination_caller_id="TEXT",
                       ck_record_id="TEXT", account="TEXT", service="TEXT", ROWID="INT")
        self.create_table(self.cursor, "imessage_chat", columns)

    def __find_message(self, pid):
        try:
            query = "SELECT text FROM imessage_chat WHERE guid = '{}' LIMIT 1".format(pid)
            if self.cursor is None:
                self.cursor, self.connection = self.get_cursor()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print_red(f"cannot find message {str(e)}")
            return False

    def set_values_to_db(self):
        dataset = self.load_imessages_from_file()
        dataset = dataset.values.tolist()
        for index, row in enumerate(dataset):
            print_green(row)
            ROWID = row[0]
            guid = row[1]
            text = row[2]
            handle_id = row[3]
            service = row[4]
            account = row[5]
            date = row[6]
            date_read = row[7]
            date_delivered = row[8]
            is_delivered = row[9]
            is_finished = row[10]
            is_emote = row[11]
            is_from_me = row[12]
            is_empty = row[13]
            is_delayed = row[14]
            is_auto_reply = row[15]
            is_prepared = row[16]
            is_read = row[17]
            is_system_message = row[18]
            is_sent = row[19]
            has_dd_results = row[20]
            cache_has_attachments = row[21]
            item_type = row[22]
            group_title = row[23]
            is_expirable = row[24]
            message_source = row[25]
            ck_record_id = row[26]
            destination_caller = row[27]
            is_spam = row[28]

            message = self.__find_message(guid)
            if not message:
                self.__set_message(ROWID, guid, text, handle_id, service, account, date, date_read,
                                   date_delivered,
                                   is_delivered,
                                   is_finished, is_emote, is_from_me, is_empty, is_delayed, is_auto_reply, is_prepared,
                                   is_read,
                                   is_system_message, is_sent, has_dd_results, cache_has_attachments, item_type,
                                   group_title,
                                   is_expirable, message_source, ck_record_id, destination_caller, is_spam)
            else:
                self.__update_message(ROWID, guid, text, handle_id, service, account, date, date_read,
                                      date_delivered,
                                      is_delivered,
                                      is_finished, is_emote, is_from_me, is_empty, is_delayed, is_auto_reply,
                                      is_prepared, is_read,
                                      is_system_message, is_sent, has_dd_results, cache_has_attachments, item_type,
                                      group_title,
                                      is_expirable, message_source, ck_record_id, destination_caller, is_spam)

    def get_imessages(self):
        cur, conn = self.__connect_with_i_message()
        cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        for name in cur.fetchall():
            print(name)
        messages = pd.read_sql_query("select * from message", conn)
        return messages.to_json()

    def get_dates(self, date, parent_date):
        t = (2001, 1, 1, 0, 0, 0, 0, 0, 0)
        t = time.mktime(t)
        date = date / 1000000000 + int(time.strftime("%s", time.gmtime(t)))
        parent_date = parent_date / 1000000000 + int(time.strftime("%s", time.gmtime(t)))
        formatted_date = datetime.datetime.fromtimestamp(int(date))
        formatted_parent_date = datetime.datetime.fromtimestamp(int(parent_date))
        return formatted_date, formatted_parent_date

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

    def __update_message(self, ROWID, guid, text, handle_id, service, account, date, date_read,
                         date_delivered,
                         is_delivered,
                         is_finished, is_emote, is_from_me, is_empty, is_delayed, is_auto_reply, is_prepared, is_read,
                         is_system_message, is_sent, has_dd_results, cache_has_attachments, item_type, group_title,
                         is_expirable, message_source, ck_record_id, destination_caller, is_spam):
        try:
            text = text.replace("'", '"')
            query = f"UPDATE imessage_chat SET ROWID = {ROWID}, guid = '{guid}', text = '{text}', handle_id = {int(handle_id)}, `date` = {int(date)}, date_read = {int(date_read)}, date_delivered = {int(date_delivered)}, is_delivered = {int(is_delivered)}, is_finished = {int(is_finished)}, is_emote = {int(is_emote)}, is_from_me = {int(is_from_me)}, is_empty = {int(is_empty)}, is_delayed = {int(is_delayed)}, is_auto_reply = {int(is_auto_reply)}, is_prepared = {int(is_prepared)}, is_read = {int(is_read)}, is_system_message = {int(is_system_message)}, is_sent = {int(is_sent)}, has_dd_results = {int(has_dd_results)}, is_spam = {int(is_spam)}, cache_has_attachments = {int(cache_has_attachments)}, item_type = {int(item_type)}, group_title = '{group_title}', is_expirable = {int(is_expirable)}, message_source = {int(message_source)}, destination_caller_id = '{destination_caller}', ck_record_id = '{ck_record_id}', account = '{account}', service = '{service}' WHERE guid = '{guid}';"

            print_magenta(f"update => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {guid}, {str(e)}")

    def __set_message(self, ROWID, guid, text, handle_id, service, account, date, date_read,
                      date_delivered,
                      is_delivered,
                      is_finished, is_emote, is_from_me, is_empty, is_delayed, is_auto_reply, is_prepared, is_read,
                      is_system_message, is_sent, has_dd_results, cache_has_attachments, item_type, group_title,
                      is_expirable, message_source, ck_record_id, destination_caller, is_spam):
        try:
            text = text.replace('"', "'")
            query = """INSERT INTO imessage_chat VALUES ("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")""".format(
                guid,
                text,
                int(handle_id),
                int(date),
                int(date_read),
                int(date_delivered),
                int(is_delivered),
                int(is_finished),
                int(is_emote),
                int(is_from_me),
                int(is_empty),
                int(is_delayed),
                int(is_auto_reply),
                int(is_prepared),
                int(is_read),
                int(is_system_message),
                int(is_sent),
                int(has_dd_results),
                int(is_spam),
                int(cache_has_attachments),
                int(item_type),
                group_title,
                int(is_expirable),
                int(message_source),
                destination_caller,
                ck_record_id,
                service,
                account,
                ROWID)
            print_cyan(f"set => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {guid}, {str(e)}")
