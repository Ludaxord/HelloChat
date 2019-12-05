import glob
import pandas as pd

from hellochat.utils.sources.compression import Compression
from hellochat.utils.tools.printers import print_green, print_blue, print_red, print_cyan, print_magenta


class WikiQuestionsAndAnswers(Compression):
    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/q&a")
        self.init_default_table()

    def init_default_table(self):
        self.cursor, self.connection = self.get_cursor()
        columns = dict(question_id="TEXT", question="TEXT", document_id="TEXT", document_title="TEXT",
                       sentence_id="TEXT", sentence="TEXT", label="TEXT")
        self.create_table(self.cursor, "wiki_questions_and_answers", columns)

    def get_json_files(self):
        json_files = glob.glob(f"{self.destination_folder}/Question_Answer_Dataset_v1.2/S*/*.tsv")
        return json_files

    def set_values_to_db(self):
        json_files = self.get_json_files()
        for json_file in json_files:
            print_green(json_file)
            dataset = pd.read_csv(json_file, delimiter='\t')
            for index, data in dataset.iterrows():
                print_blue(data)
                question_id = data["QuestionID"]
                question = data["Question"]
                document_id = data["DocumentID"]
                document_title = data["DocumentTitle"]
                sentence_id = data["SentenceID"]
                sentence = data["Sentence"]
                label = data["Label"]
                print_green(
                    f"question_id => {question_id}, question => {question}, document_id => {document_id}, document_title => {document_title}, sentence_id => {sentence_id}, sentence => {sentence}, label => {label}")
                message = self.__find_message(document_id)
                if message:
                    self.__update_message(question_id, question, document_id, document_title,
                                          sentence_id, sentence, label)
                else:
                    self.__set_message(question_id, question, document_id, document_title,
                                       sentence_id, sentence, label)

    def __find_message(self, document_id):
        try:
            query = "SELECT sentence FROM wiki_questions_and_answers WHERE document_id = '{}' LIMIT 1".format(
                document_id)
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
            self.cursor, self.connection = self.get_cursor()
            return False

    def __update_message(self, question_id, question, document_id, document_title,
                         sentence_id, sentence, label):
        try:
            if type(question) is str:
                question = question.replace("'", '"')
            if type(sentence) is str:
                sentence = sentence.replace('"', '"')
            query = f"UPDATE wiki_questions_and_answers SET question_id = '{question_id}', question = '{question}', document_id = '{document_id}', document_title = '{document_title}', sentence_id = '{sentence_id}', sentence = '{sentence}', label = '{label}' WHERE document_id = '{document_id}' AND question = '{question}' AND answer = '{answer}';"
            print_magenta(f"update => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {document_id}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()

    def __set_message(self, question_id, question, document_id, document_title,
                      sentence_id, sentence, label):
        try:
            if type(question) is str:
                question = question.replace('"', "'")
            if type(sentence) is str:
                sentence = sentence.replace('"', "'")
            query = """INSERT INTO wiki_questions_and_answers VALUES ("{}","{}","{}","{}","{}","{}","{}")""".format(
                question_id, question, document_id, document_title,
                sentence_id, sentence, label)
            print_cyan(f"set => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {document_id}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()
