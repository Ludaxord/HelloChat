from argparse import ArgumentParser

from hellochat.utils.sources.apple import Apple
from hellochat.utils.sources.facebook import Facebook
from hellochat.utils.sources.google import Google
from hellochat.utils.sources.questions_and_answers import QuestionsAndAnswers
from hellochat.utils.sources.reddit import Reddit
from hellochat.utils.sources.synonyms import Synonyms
from hellochat.utils.sources.translator import Translator
from hellochat.utils.sources.wiki_questions_and_answers import WikiQuestionsAndAnswers


def chat_args():
    return Parser(
        args=[{"command": "--source", "type": str,
               "help": "source type options: reddit, messenger, imessage"},
              {"command": "--sources", "nargs": '+',
               "help": "list of source type options: reddit, messenger, imessage"},
              {"command": "--engine", "type": str,
               "help": "list of backend engines used to run deep learning model"}]).get_args()


def set_compressor(source_name, destination):
    if source_name == "apple":
        apple = Apple(destination)
        return apple
    elif source_name == "reddit":
        reddit = Reddit(destination)
        return reddit
    elif source_name == "facebook":
        facebook = Facebook(destination)
        return facebook
    elif source_name == "google":
        google = Google(destination)
        return google
    elif source_name == "q&a":
        q_and_a = QuestionsAndAnswers(destination)
        return q_and_a
    elif source_name == "wiki_q&a":
        wiki_q_and_a = WikiQuestionsAndAnswers(destination)
        return wiki_q_and_a
    elif source_name == "synonyms":
        synonyms = Synonyms(destination)
        return synonyms
    elif source_name == "translator":
        translator = Translator(destination)
        return translator
    else:
        return None


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
