from tensorflow.keras.layers import Layer, Dense
from tensorflow import expand_dims, reduce_sum
import tensorflow as tf


class BahdanauAttention(Layer):
    def __init__(self, units):
        super().__init__()
        self.W1 = Dense(units)
        self.W2 = Dense(units)
        self.V = Dense(1)

    def __call__(self, query, values):
        hidden_with_time_axis = expand_dims(query, 1)
        score = self.V(tf.nn.tanh(self.W1(values) + self.W2(hidden_with_time_axis)))
        attention_weights = tf.nn.softmax(score, axis=1)
        context_vector = attention_weights * values
        context_vector = reduce_sum(context_vector, axis=1)
        return context_vector, attention_weights
