from hellochat.utils.compression import Compression


class AppleIMessage(Compression):
    def __init__(self, destination_folder):
        super().__init__(destination_folder)

    def __connect_with_i_message(self):
        cursor, connection = self.get_cursor(db="/Users/konraduciechowski/Library/Messages")
        return cursor, connection

    def get_imessages(self):
        cur, conn = self.__connect_with_i_message()
        cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        for name in cur.fetchall():
            print(name)
