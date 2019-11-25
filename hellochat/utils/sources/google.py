import glob
import json

from hellochat.utils.sources.compression import Compression
from hellochat.utils.tools.printers import print_green, print_blue, print_yellow, print_magenta, print_cyan, print_red


class Google(Compression):

    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/google")
        self.init_default_table()

    def init_default_table(self):
        self.cursor, self.connection = self.get_cursor()
        columns = dict(event_id="TEXT PRIMARY KEY", text="TEXT", sender_id="INT", fallback_name="TEXT", timestamp="INT",
                       conversation_id="TEXT", type="TEXT")
        self.create_table(self.cursor, "hangouts_chat", columns)

    def get_json_files(self):
        json_files = glob.glob(f"{self.destination_folder}/Takeout*/Hangouts/Hangouts.json")
        return json_files

    def get_participants(self, participant_data):
        participants = dict()
        for participant in participant_data:
            gaia_id = participant["id"]["gaia_id"]
            fallback_name = self.convert_encoding(participant["fallback_name"])
            print_cyan(f"gaia_id => {gaia_id}, fallback_name => {fallback_name}")
            participants[gaia_id] = fallback_name
        return participants

    def set_values_to_db(self):
        json_files = self.get_json_files()
        for json_file in json_files:
            print_green(json_file)
            with open(json_file, buffering=1000, encoding='iso-8859-1') as f:
                j = json.load(f)
                print_blue(j)
                conversations = j["conversations"]
                for conversation in conversations:
                    participant_data = conversation["conversation"]["conversation"]["participant_data"]
                    participants = self.get_participants(participant_data)
                    print_magenta(participants)
                    events = conversation["events"]
                    for event in events:
                        conversation_id = event["conversation_id"]["id"]
                        sender_id = event["sender_id"]["gaia_id"]
                        fallback_name = participants[sender_id]
                        timestamp = event["timestamp"]
                        event_id = event["event_id"]
                        try:
                            segment = event["chat_message"]["message_content"]["segment"][0]
                            event_type = segment["type"]
                            text = self.convert_encoding(segment["text"])
                        except Exception as e:
                            print_yellow(f"cannot find segment in {event}, {e}")
                        print_green(
                            f"conversation_id => {conversation_id}, sender_id => {sender_id}, fallback_name => {fallback_name}, timesttamp => {timestamp}, event_id => {event_id}, event_type => {event_type}, text => {text}")
                        message = self.__find_message(event_id)
                        if message:
                            self.__update_message(event_id, text, sender_id, fallback_name, timestamp, conversation_id,
                                                  event_type)
                        else:
                            self.__set_message(event_id, text, sender_id, fallback_name, timestamp, conversation_id,
                                               event_type)

    def __find_message(self, guid):
        try:
            query = "SELECT text FROM hangouts_chat WHERE event_id = '{}' LIMIT 1".format(guid)
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

    def __set_message(self, event_id, text, sender_id, fallback_name, timestamp, conversation_id, event_type):
        try:
            text = text.replace('"', "'")
            query = """INSERT INTO hangouts_chat VALUES ("{}","{}","{}","{}","{}","{}", "{}")""".format(event_id, text,
                                                                                                        int(sender_id),
                                                                                                        fallback_name,
                                                                                                        int(timestamp),
                                                                                                        conversation_id,
                                                                                                        event_type)
            print_cyan(f"set => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {event_id}, {str(e)}")

    def __update_message(self, event_id, text, sender_id, fallback_name, timestamp, conversation_id, event_type):
        try:
            text = text.replace("'", '"')
            query = f"UPDATE hangouts_chat SET event_id = '{event_id}', text = '{text}', sender_id = {sender_id}, fallback_name = '{fallback_name}', timestamp = '{timestamp}', conversation_id = '{conversation_id}', type = {event_type} WHERE event_id = '{event_id}';"
            print_magenta(f"update => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {event_id}, {str(e)}")
