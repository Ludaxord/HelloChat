from argparse import ArgumentParser


def chat_args():
    return Parser(
        args=[{"command": "--source", "type": str,
               "help": "source type options: reddit, messenger, imessage"},
              {"command": "--sources", "nargs": '+',
               "help": "list of source type options: reddit, messenger, imessage"}]).get_args()


class Parser:
    parser = None
    args = []

    def __init__(self, args=None):
        if args is None:
            args = []
        self.parser = self.__init_parser()
        self.__add_args(args)
        self.args = self.parser.parse_args()

    def get_parser(self):
        return self.parser

    def get_args(self):
        return self.args

    def __init_parser(self):
        parser = ArgumentParser()
        return parser

    def __add_args(self, args):
        for arg in args:
            if isinstance(arg, dict):
                arg_command = arg.get("command")
                arg_nargs = arg.get("nargs")
                arg_action = arg.get("action")
                arg_type = arg.get("type")
                arg_help = arg.get("help")
                self.parser.add_argument(arg_command, type=arg_type, nargs=arg_nargs, action=arg_action, help=arg_help)
