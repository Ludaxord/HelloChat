import re
import unicodedata

import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.model_selection import train_test_split

from hellochat.seq2seq.sequences.sequence import Sequence
from hellochat.utils.tools.printers import print_blue, print_magenta, print_red, print_cyan, print_yellow, print_gray


class TranslatorSequences(Sequence):
    cursor = None
    connection = None

    def __init__(self, table_name, cursor, connection):
        super().__init__(table_name)
        self.cursor = cursor
        self.connection = connection

    def init_preprocess(self):
        query = "SELECT * FROM translator"
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        target_arr = np.array([])
        input_arr = np.array([])
        target_lang_list = list()
        input_lang_list = list()
        for result in results:
            input_tensor, target_tensor, inp_lang, targ_lang = self.load_dataset(result)
            max_length_targ, max_length_inp = self.__max_length(target_tensor), self.__max_length(input_tensor)
            target_arr = np.append(target_arr, target_tensor)
            input_arr = np.append(input_arr, input_tensor)
            target_lang_list.append(targ_lang)
            input_lang_list.append(inp_lang)
            print_yellow(
                f"target translation token => {targ_lang} input translation token => {inp_lang}, target tensor => {target_tensor}, target tensor type => {type(target_tensor)}, input tensor => {input_tensor}, input tensor type => {type(input_tensor)} max input length => {max_length_inp}, max target length => {max_length_targ}")
        target_dataframe, input_dataframe = self.__create_data_frames(target_arr, input_arr)
        print_gray(f"target_dataframe => {target_dataframe}, input_dataframe => {input_dataframe}")
        input_tensor_train, input_tensor_val, target_tensor_train, target_tensor_val = self.__split_data(
            input_dataframe, target_dataframe)
        self.__input_target_iterator(input_lang_list, target_lang_list, input_tensor_train, target_tensor_train)

    def __input_target_iterator(self, input_lang_list, target_lang_list, input_tensor_train, target_tensor_train):
        for inp, targ in zip(input_lang_list, target_lang_list):
            print("Input Language; index to word mapping")
            self.__convert(inp, input_tensor_train[0])
            print("Target Language; index to word mapping")
            self.__convert(targ, target_tensor_train[0])
            dataset = self.__create_tf_dataset(input_tensor_train, inp, target_tensor_train, targ)

    def __create_data_frames(self, target_arr, input_arr):
        print(f"target_arr => {target_arr}, input_arr => {input_arr}")
        target_dataframe = pd.DataFrame({"Target": target_arr})
        input_dataframe = pd.DataFrame({"Input": input_arr})
        return target_dataframe, input_dataframe

    def __max_length(self, tensor):
        return max(len(t) for t in tensor)

    def __create_tf_dataset(self, input_tensor_train, inp_lang, target_tensor_train, targ_size, batch_size=64,
                            embedding_dim=256,
                            units=1024, drop_remainder=True):
        buffer_size = len(input_tensor_train)
        step_per_epoch = len(input_tensor_train)
        vocab_inp_size = len(inp_lang.word_index) + 1
        vocab_tar_size = len(targ_size.word_index) + 1

        dataset = tf.data.Dataset.from_tensor_slices((input_tensor_train, target_tensor_train)).shuffle(buffer_size)
        dataset = dataset.batch(batch_size, drop_remainder=drop_remainder)
        return dataset

    def __tokenize(self, lang):
        lang_tokenizer = tf.keras.preprocessing.text.Tokenizer(filters='')
        lang_tokenizer.fit_on_texts(lang)

        tensor = lang_tokenizer.texts_to_sequences(lang)

        tensor = tf.keras.preprocessing.sequence.pad_sequences(tensor, padding='post')

        return tensor, lang_tokenizer

    def __unicode_to_ascii(self, string):
        return ''.join(c for c in unicodedata.normalize('NFD', string) if unicodedata.category(c) != 'Mn')

    def __preprocess_sentence(self, words):
        w = self.__unicode_to_ascii(words.lower().strip())
        w = re.sub(r"([?.!,¿])", r" \1 ", w)
        w = re.sub(r'[" "]+', " ", w)
        w = re.sub(r"[^a-zA-Z?.!,¿]+", " ", w)
        w = w.rstrip().strip()
        w = '<start> ' + w + ' <end>'
        return w

    def __create_dataset(self, result):
        word_pairs = []
        try:
            translate_language = result[-1]
            language = result[-2]
            translate = result[-3].replace('"', "'")
            non_translate = result[-4].replace('"', "'")
            preprocessed_translate = self.__preprocess_sentence(translate)
            preprocessed_non_translate = self.__preprocess_sentence(non_translate)
            word_pairs.append(preprocessed_non_translate)
            word_pairs.append(preprocessed_translate)
            print_blue(
                f"translate => {translate_language}:{translate} | non_translate => {language}:{non_translate}")
            print_magenta(
                f"translate => {translate_language}:{preprocessed_translate} | non_translate => {language}:{preprocessed_non_translate}")
        except Exception as e:
            print_red(f"cannot preprocess data from result of structure {result}, {str(e)}")
        return zip(word_pairs)

    def __convert(self, lang, tensor):
        for t in tensor:
            if t != 0:
                print_cyan("%d ----> %s" % (t, lang.index_word[t]))

    def __split_data(self, input_tensor, target_tensor, test_size=0.2):
        input_tensor_train, input_tensor_val, target_tensor_train, target_tensor_val = train_test_split(input_tensor,
                                                                                                        target_tensor,
                                                                                                        test_size=test_size)
        print(len(input_tensor_train), len(target_tensor_train), len(input_tensor_val), len(target_tensor_val))
        return input_tensor_train, input_tensor_val, target_tensor_train, target_tensor_val

    def load_dataset(self, result):
        targ_lang, inp_lang = self.__create_dataset(result)
        print_cyan(f"target translation => {targ_lang[-1]} input translation => {inp_lang[-1]}")
        input_tensor, inp_lang_tokenizer = self.__tokenize(inp_lang)
        target_tensor, targ_lang_tokenizer = self.__tokenize(targ_lang)
        return input_tensor, target_tensor, inp_lang_tokenizer, targ_lang_tokenizer
