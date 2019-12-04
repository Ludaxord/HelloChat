from hellochat.seq2seq.sequences.sequence import Sequence
import tensorflow as tf


class TranslatorSequences(Sequence):

    def __max_length(self, tensor):
        return max(len(t) for t in tensor)

    def __tokenize(self, lang):
        lang_tokenizer = tf.keras.preprocessing.text.Tokenizer(filters='')
        lang_tokenizer.fit_on_texts(lang)

        tensor = lang_tokenizer.texts_to_sequences(lang)

        tensor = tf.keras.preprocessing.sequence.pad_sequences(tensor, padding='post')

        return tensor, lang_tokenizer
