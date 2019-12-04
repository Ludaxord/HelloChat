import io
import re
import unicodedata

from hellochat.seq2seq.sequences.sequence import Sequence
import tensorflow as tf


class TranslatorSequences(Sequence):

    def __init__(self, table_name):
        super().__init__(table_name)

    def __max_length(self, tensor):
        return max(len(t) for t in tensor)

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

    def __create_dataset(self, path, num_examples):
        lines = io.open(path, encoding='UTF-8').read().strip().split('\n')
        word_pairs = [[self.__preprocess_sentence(w) for w in li.split('\t')] for li in lines[:num_examples]]
        return zip(*word_pairs)

    def load_dataset(self, path, num_examples=None):
        targ_lang, inp_lang = self.__create_dataset(path, num_examples)
        input_tensor, inp_lang_tokenizer = self.__tokenize(inp_lang)
        target_tensor, targ_lang_tokenizer = self.__tokenize(targ_lang)
        return input_tensor, target_tensor, inp_lang_tokenizer, targ_lang_tokenizer
