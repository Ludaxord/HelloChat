from tensorflow.keras import Model
from tensorflow.keras.layers import Embedding, GRU, Dense
from tensorflow import concat, expand_dims, reshape

from hellochat.seq2seq.layers.bahdanau_attention import BahdanauAttention


class Decoder(Model):
    def __init__(self, vocabulary_size, embedding_dim, decoded_units, batch_size):
        super().__init__()
        self.batch_size = batch_size
        self.decoded_units = decoded_units
        self.embedding = Embedding(vocabulary_size, embedding_dim)
        self.gru = GRU(self.decoded_units, return_sequences=True, return_state=True,
                       recurrent_initializer='glorot_uniform')
        self.dense = Dense(vocabulary_size)
        self.attention = BahdanauAttention(self.decoded_units)

    def __call__(self, x, hidden, enc_output):
        context_vecor, attention_weights = self.attention(hidden, enc_output)
        x = self.embedding(x)
        x = concat([expand_dims(context_vecor, 1), x], axis=-1)
        output, state = self.gru(x)
        output = reshape(output, (-1, output.shape[2]))
        x = self.dense(output)
        return x, state, attention_weights
