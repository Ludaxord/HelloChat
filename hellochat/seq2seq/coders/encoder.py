from tensorflow.keras import Model
from tensorflow.keras.layers import Embedding, GRU
from tensorflow import zeros


class Encoder(Model):

    def __init__(self, vocabulary_size, embedding_dim, encoded_units, batch_size):
        super().__init__()
        self.batch_size = batch_size
        self.encoded_units = encoded_units
        self.embedding = Embedding(vocabulary_size, embedding_dim)
        self.gru = GRU(self.encoded_units, return_sequences=True, return_state=True,
                       recurent_initializer='glorot_uniform')

    def __call__(self, x, hidden):
        x = self.embedding(x)
        output, state = self.gru(x, initial_state=hidden)
        return output, state

    def initialize_hidden_state(self):
        return zeros((self.batch_sz, self.enc_units))
