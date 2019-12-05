import glob

from hellochat.utils.sources.compression import Compression
import pandas as pd

from hellochat.utils.tools.printers import print_green, print_blue, print_red, print_magenta, print_cyan


class QuestionsAndAnswers(Compression):
    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/q&a")
        self.init_default_table()

    def init_default_table(self):
        self.cursor, self.connection = self.get_cursor()
        columns = dict(article_title="TEXT", question="TEXT", answer="TEXT", difficulty_from_questioner="TEXT",
                       difficulty_from_answerer="TEXT", article_file="TEXT")
        self.create_table(self.cursor, "questions_and_answers", columns)

    def get_json_files(self):
        json_files = glob.glob(f"{self.destination_folder}/Question_Answer_Dataset_v1.2/S*/*.txt")
        return json_files

    def set_values_to_db(self):
        json_files = self.get_json_files()
        for json_file in json_files:
            print_green(json_file)
            dataset = pd.read_csv(json_file, delimiter='\t', encoding='iso-8859-1')
            for index, data in dataset.iterrows():
                print_blue(data)
                article_title = data['ArticleTitle']
                question = data['Question']
                answer = data['Answer']
                difficulty_from_questioner = data['DifficultyFromQuestioner']
                difficulty_from_answerer = data['DifficultyFromAnswerer']
                article_file = data['ArticleFile']
                print_green(
                    f"article_title => {article_title}, question => {question}, answer => {answer}, difficulty_from_questioner => {difficulty_from_questioner}, difficulty_from_answerer => {difficulty_from_answerer}, article_file => {article_file}")
                message = self.__find_message(article_title, question, answer)
                if message:
                    self.__update_message(article_title, question, answer, difficulty_from_questioner,
                                          difficulty_from_answerer, article_file)
                else:
                    self.__set_message(article_title, question, answer, difficulty_from_questioner,
                                       difficulty_from_answerer, article_file)

    def __find_message(self, article_name, question, answer):
        try:
            query = "SELECT answer, question FROM questions_and_answers WHERE article_title = '{}' AND question = '{}' AND answer = '{}' LIMIT 1".format(
                article_name, question, answer)
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

    def __set_message(self, article_title, question, answer, difficulty_from_questioner,
                      difficulty_from_answerer, article_file):
        try:
            if type(question) is str:
                question = question.replace('"', "'")
            if type(answer) is str:
                answer = answer.replace('"', "'")
            query = """INSERT INTO questions_and_answers VALUES ("{}","{}","{}","{}","{}","{}")""".format(
                article_title, question, answer, difficulty_from_questioner,
                difficulty_from_answerer, article_file)
            print_cyan(f"set => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {article_title}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()

    def __update_message(self, article_title, question, answer, difficulty_from_questioner,
                         difficulty_from_answerer, article_file):
        try:
            if type(question) is str:
                question = question.replace("'", '"')
            if type(answer) is str:
                answer = answer.replace('"', '"')
            query = f"UPDATE questions_and_answers SET article_title = '{article_title}', question = '{question}', answer = '{answer}', difficulty_from_questioner = '{difficulty_from_questioner}', difficulty_from_answerer = '{difficulty_from_answerer}', article_file = '{article_file}' WHERE article_title = '{article_title}' AND question = '{question}' AND answer = '{answer}';"
            print_magenta(f"update => {query}")
            self.transaction_bldr(query)
        except Exception as e:
            print_red(f"cannot update message on id {article_title}, {str(e)}")
            self.cursor, self.connection = self.get_cursor()
