import glob
import json

from hellochat.utils.sources.compression import Compression
from hellochat.utils.tools.printers import print_green, print_yellow, print_red, print_cyan, print_magenta, print_blue


class FacebookMessenger(Compression):
    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/messenger")
        self.init_default_table()

    def get_json_files(self):
        json_files = glob.glob(f"{self.destination_folder}/messages/*/*/*.json")
        return json_files

    def init_default_table(self):
        self.cursor, self.connection = self.get_cursor()
        columns = dict(guid="TEXT PRIMARY KEY", text="TEXT", sender_name="TEXT", timestamp_ms="INT", type="TEXT",
                       photos_list="TEXT")
        self.create_table(self.cursor, "facebook_chat", columns)

    def set_values_to_db(self):
        json_files = self.get_json_files()
        for json_file in json_files:
            print_green(json_file)
            with open(json_file, buffering=1000, encoding='iso-8859-1') as f:
                j = json.load(f)
                print_blue(j)
                participants = j["participants"]
                for index, message in enumerate(reversed(j["messages"])):
                    sender_name = self.convert_encoding(message["sender_name"])
                    timestamp_ms = message["timestamp_ms"]
                    message_type = self.convert_encoding(message["type"])
                    sender_message_name = self.convert_encoding(participants[0]['name'])
                    if sender_message_name != "Konrad Uciechowski":
                        guid = f"{index}_{sender_message_name}_{timestamp_ms}"
                    else:
                        guid = f"{index}_facebook_user_{timestamp_ms}"

                    print_cyan(guid)

                    try:
                        content = self.convert_encoding(message["content"])
                    except Exception as e:
                        print_yellow(f"cannot get content, {e}")
                        content = ""

                    try:
                        photos = message["photos"]
                        photos_list = list()
                        for photo in photos:
                            photo_uri = photo["uri"]
                            photos_list.append(photo_uri)
                        photo_str = ', '.join([str(elem) for elem in photos_list])
                    except Exception as e:
                        print_yellow(f"cannot get photos, {e}")
                        photo_str = ""

                    exists = self.__find_message(guid)
                    if exists:
                        self.__update_message(guid, sender_name, timestamp_ms, message_type, content, photo_str)
                    else:
                        self.__set_message(guid, sender_name, timestamp_ms, message_type, content, photo_str)

    def __find_message(self, guid):
        try:
            query = "SELECT text FROM facebook_chat WHERE guid = '{}' LIMIT 1".format(guid)
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

    def __set_message(self, guid, sender_name, timestamp_ms, message_type, content, photos_list):
        try:
            content = content.replace('"', "'")
            query = """INSERT INTO facebook_chat VALUES ("{}","{}","{}","{}","{}","{}")""".format(guid,
                                                                                                  content,
                                                                                                  sender_name,
                                                                                                  timestamp_ms,
                                                                                                  message_type,
                                                                                                  photos_list)
            print_cyan(f"set => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {guid}, {str(e)}")

    def __update_message(self, guid, sender_name, timestamp_ms, message_type, content, photos_list):
        try:
            content = content.replace("'", '"')
            query = f"UPDATE facebook_chat SET guid = '{guid}', sender_name = '{sender_name}', timestamp_ms = {timestamp_ms}, type = '{message_type}', text = '{content}', photos_list = '{photos_list}' WHERE guid = '{guid}';"
            print_magenta(f"update => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {guid}, {str(e)}")
