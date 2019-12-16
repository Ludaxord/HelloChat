import time

import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.model_selection import train_test_split

from hellochat.seq2seq.layers.bahdanau_attention import BahdanauAttention
from hellochat.seq2seq.models.decoder import Decoder
from hellochat.seq2seq.models.encoder import Encoder
from hellochat.seq2seq.sequences.sequence import Sequence, preprocess_sentence
from hellochat.utils.tools.printers import print_blue, print_magenta, print_red, print_cyan, print_yellow, print_gray, \
    print_green


class TranslatorSequences(Sequence):
    cursor = None
    connection = None

    def __init__(self, table_name, cursor, connection):
        super().__init__(table_name)
        self.cursor = cursor
        self.connection = connection

    def init_preprocess(self, lang=None):
        if lang is None:
            query = "SELECT * FROM translator"
        else:
            query = f"SELECT * FROM translator WHERE translate_language = '{lang}'"
        print_magenta(query)
        self.__get_pandas_sql(query)

    def __get_pandas_sql(self, query):
        results = pd.read_sql(query, self.connection)
        translations = results.iloc[:, 1:3]
        non_trans = results.loc[:, "non_translate"]
        trans = results.loc[:, "translate"]
        print_cyan(results)
        print_green(translations)
        trans_tensor, non_trans_tensor, trans_token, non_trans_token = self.__preprocess_translation(translations,
                                                                                                     non_trans, trans)
        dataset = pd.DataFrame(
            {"non_translate": non_trans, "translate": trans, "non_translate_tensor": non_trans_tensor,
             "translate_tensor": trans_tensor})
        print_green(dataset)
        # print_blue(f"trans_token => {trans_token} ||| non_trans_token => {non_trans_token}")
        # Experimental
        input_tensor_train, input_tensor_val, target_tensor_train, target_tensor_val = self.__split_data(
            dataset.loc[:, "non_translate_tensor"], dataset.loc[:, "translate_tensor"])
        self.__input_target_iterator(non_trans_token, trans_token, input_tensor_train, target_tensor_train)

    def __preprocess_translation(self, translations, non_trans, trans):
        print_magenta(f"type {type(non_trans)}, {type(trans)}")
        trans_token_arr = dict()
        trans_tensor_arr = np.array([])
        non_trans_token_arr = dict()
        non_trans_tensor_arr = np.array([])

        for index, row in translations.iterrows():
            non_translate = row["non_translate"]
            nt = non_trans[index]
            translate = row["translate"]
            t = trans[index]
            print_blue(f"non_translate => {non_translate} <==> {nt} ")
            print_magenta(f"translate => {translate} <==> {t}")
            if non_translate == nt and translate == t:
                non_trans[index] = preprocess_sentence(non_translate)
                trans[index] = preprocess_sentence(translate)
                trans_tensor, trans_tokenized = self.__tokenize(trans[index])
                non_trans_tensor, non_trans_tokenized = self.__tokenize(non_trans[index])

                max_length_targ, max_length_inp = self.__max_length(trans_tensor), self.__max_length(non_trans_tensor)

                trans_str_tensor = ""
                non_trans_str_tensor = ""

                for tt in trans_tensor[0]:
                    trans_str_tensor += f"{tt}||"
                k = trans_str_tensor.rfind("||")
                trans_str_tensor = trans_str_tensor[:k] + "" + trans_str_tensor[k + 2:]

                for ntt in non_trans_tensor[0]:
                    non_trans_str_tensor += f"{ntt}||"
                n = non_trans_str_tensor.rfind("||")
                non_trans_str_tensor = non_trans_str_tensor[:n] + "" + non_trans_str_tensor[n + 2:]

                print_gray(f"decoded => {trans_str_tensor} |and| {non_trans_str_tensor}")

                trans_tensor_arr = np.append(trans_tensor_arr, trans_str_tensor)
                non_trans_tensor_arr = np.append(non_trans_tensor_arr, non_trans_str_tensor)

                trans_token_arr[index] = trans_tokenized
                non_trans_token_arr[index] = non_trans_tokenized
                print_yellow(f"non translate => {non_trans[index]}")
                print_yellow(f"translate => {trans[index]}")
        return trans_tensor_arr, non_trans_tensor_arr, trans_token_arr, non_trans_token_arr

    def __input_target_iterator(self, input_lang_list, target_lang_list, input_tensor_train, target_tensor_train):
        dc = set(input_lang_list) & set(target_lang_list)
        for i in dc:
            print_green(f"dc index: {i}")
            try:
                inp = input_lang_list[i]
                inp_tensor = input_tensor_train[i]
                targ = target_lang_list[i]
                targ_tensor = target_tensor_train[i]
                print("Input Language; index to word mapping")
                self.__convert(inp, inp_tensor)
                print("Target Language; index to word mapping")
                self.__convert(targ, targ_tensor)
                try:
                    dataset = self.__create_tf_dataset(input_tensor_train, inp, target_tensor_train, targ)
                    print_gray(dataset)
                except Exception as e:
                    print_red(f"exception tf dataset {str(e)}")
            except Exception as e:
                print_red(f"-----------------------------------!#ERROR------------------------------------------------")
                print_red(f"cannot get value of index {i}")
                print_red(f"Exception: {str(e)}")

    def __max_length(self, tensor):
        return max(len(t) for t in tensor)

    def __create_tf_dataset(self, input_tensor_train, inp_lang, target_tensor_train, targ_lang):
        print_yellow(input_tensor_train)
        print_green(target_tensor_train)
        input_tensor_train, target_tensor_train = self.__convert_series_of_str_to_list(input_tensor_train,
                                                                                       target_tensor_train)

        BUFFER_SIZE = len(input_tensor_train)
        BATCH_SIZE = 64
        steps_per_epoch = len(input_tensor_train) // BATCH_SIZE
        embedding_dim = 256
        units = 1024
        vocab_inp_size = len(inp_lang.word_index) + 1
        vocab_tar_size = len(targ_lang.word_index) + 1

        dataset = tf.data.Dataset.from_tensor_slices((input_tensor_train, target_tensor_train)).shuffle(BUFFER_SIZE)
        dataset = dataset.batch(BATCH_SIZE, drop_remainder=True)

        self.__example(vocab_inp_size, dataset, embedding_dim, units, BATCH_SIZE, vocab_tar_size)

        return dataset

    def __example(self, vocab_inp_size, dataset, embedding_dim, units, batch_size, vocab_tar_size):

        example_input_batch, example_target_batch = next(iter(dataset))
        print_cyan(f"example => {example_input_batch.shape} || {example_target_batch.shape}")

        encoder = Encoder(vocab_inp_size, embedding_dim, units, batch_size)
        sample_hidden = encoder.initialize_hidden_state()
        sample_output, sample_hidden = encoder(example_input_batch, sample_hidden)

        attention_layer = BahdanauAttention(10)
        attention_result, attention_weights = attention_layer(sample_hidden, sample_output)

        decoder = Decoder(vocab_tar_size, embedding_dim, units, batch_size)
        sample_decoder_output, _, _ = decoder(tf.random.uniform((batch_size, 1)),
                                              sample_hidden, sample_output)

        print_green(f"Encoder output shape (batch size, sequence length, units) {sample_output.shape}")
        print_blue(f"Encoder hidden state shape (batch size, units) {sample_hidden.shape}")
        print_green("Attention result shape: (batch size, units) {}".format(attention_result.shape))
        print_blue("Attention weights shape: (batch_size, sequence_length, 1) {}".format(attention_weights.shape))
        print('Decoder output shape: (batch_size, vocab size) {}'.format(sample_decoder_output.shape))

    def __tokenize(self, lang):
        lang_tokenizer = tf.keras.preprocessing.text.Tokenizer(filters='')
        lang_tokenizer.fit_on_texts([lang])

        tensor = lang_tokenizer.texts_to_sequences([lang])

        tensor = tf.keras.preprocessing.sequence.pad_sequences(tensor, padding='post')

        print_cyan(f"tokenize lang => {lang} with tensor {tensor}")
        print_red(f"tokenized word index {lang_tokenizer.word_index}")

        return tensor, lang_tokenizer

    def __create_list_from_string(self, tensor):
        print_yellow(f"tensor => {tensor}")
        tensors = tensor.split("||")
        print_blue(f"tensors => {tensors}")
        tensors = [int(i) for i in tensors]
        print_green(f"tensors int => {tensors}")
        return tensors

    def __convert_series_of_str_to_list(self, input_lang_list, target_lang_list):
        connected_dc = zip(input_lang_list, target_lang_list)
        print_magenta(f"DC => {connected_dc}")
        for index, (i, t) in enumerate(connected_dc):
            print_magenta(f"{index}. {i} => {t}")

            try:
                print_yellow(f"before input => {input_lang_list[index]}")
                if type(input_lang_list[index]) is not list:
                    input_lang_list[index] = self.__create_list_from_string(input_lang_list[index])
                else:
                    print_gray(f"{input_lang_list[index]} is already list")
                print_blue(f"after input => {input_lang_list[index]}")
            except Exception as e:
                print_red(f"cannot convert series at index {index}, with input {i} {str(e)}")

            try:
                print_yellow(f"before target => {target_lang_list[index]}")
                if type(target_lang_list[index]) is not list:
                    target_lang_list[index] = self.__create_list_from_string(target_lang_list[index])
                else:
                    print_gray(f"{target_lang_list[index]} is already list")
                print_blue(f"after target => {target_lang_list[index]}")
            except Exception as e:
                print_red(f"cannot convert series at index {index}, with target {t} {str(e)}")

        return input_lang_list, target_lang_list

    def __convert(self, lang, tensor):
        print_magenta(f"convert word index {lang.word_index}")
        tensors = self.__create_list_from_string(tensor)
        for t in tensors:
            print_yellow(f"tensor index {t}")
            if t != 0:
                try:
                    print_cyan("%d ----> %s" % (t, lang.index_word[t]))
                except Exception as e:
                    print_red(f"no index of {t} in {lang.word_index}")
                    print_red(f"{str(e)}")

    def __split_data(self, input_tensor, target_tensor, test_size=0.2):
        print(input_tensor.shape)
        print(target_tensor.shape)
        input_tensor_train, input_tensor_val, target_tensor_train, target_tensor_val = train_test_split(input_tensor,
                                                                                                        target_tensor,
                                                                                                        test_size=test_size)
        print(len(input_tensor_train), len(target_tensor_train), len(input_tensor_val), len(target_tensor_val))
        return input_tensor_train, input_tensor_val, target_tensor_train, target_tensor_val
