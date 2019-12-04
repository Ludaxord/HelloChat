from abc import ABC


class Sequence(ABC):
    table_name = None

    def __init__(self, table_name):
        self.table_name = table_name
        self.check_db(table_name)

    def check_db(self, table_name):
        pass
