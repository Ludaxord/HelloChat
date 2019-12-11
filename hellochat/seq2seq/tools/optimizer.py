from tensorflow.keras.optimizers import Adam
from tensorflow.keras.losses import SparseCategoricalCrossentropy
from tensorflow.math import logical_not, equal
from tensorflow import cast, reduce_mean


class Optimizer:

    def __init__(self):
        self.optimizer = Adam()
        self.loss_object = SparseCategoricalCrossentropy(from_logits=True, reduction='none')

    def loss_function(self, real, pred):
        mask = logical_not(equal(real, 0))
        loss = self.loss_object(real, pred)

        mask = cast(mask, dtype=loss.dtype)
        loss *= mask
        return reduce_mean(loss)
