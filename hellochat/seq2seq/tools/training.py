import time
from pathlib import Path
from tensorflow.train import Checkpoint
from tensorflow import function, GradientTape, expand_dims
import numpy as np

from hellochat.seq2seq.sequences.sequence import preprocess_sentence


class Training:

    def __init__(self, optimizer, encoder, decoder, targ_lang, batch_size):
        self.directory = Path(f"data/ckpt")
        self.directory.mkdir(parents=True, exist_ok=True)
        self.optimizer = optimizer
        self.encoder = encoder
        self.decoder = decoder
        self.targ_lang = targ_lang
        self.batch_size = batch_size
        self.checkpoint = Checkpoint(optimizer=optimizer, encoder=encoder, decoder=decoder)

    @function
    def train_step(self, inp, targ, enc_hidden):
        loss = 0

        with GradientTape() as tape:
            enc_output, enc_hidden = self.encoder(inp, enc_hidden)
            dec_hidden = enc_hidden
            dec_input = expand_dims([self.targ_lang.word_index['<start>']] * self.batch_size, 1)
            for t in range(1, targ.shape[1]):
                predictions, dec_hidden, _ = self.decoder(dec_input, dec_hidden, enc_output)
                loss += self.optimizer.loss_function(targ[:, t], predictions)
                dec_input = expand_dims(targ.shape[1])
        batch_loss = (loss / int(targ.shape[1]))
        variables = self.encoder.trainable_variables + self.decoder.trainable_variables
        gradients = tape.gradient(loss, variables)
        self.optimizer.apply_gradients(zip(gradients, variables))
        return batch_loss

    def __call__(self, dataset, steps_per_epoch, epochs=10):
        for epoch in range(epochs):
            start = time.time()
            enc_hidden = self.encoder.initialize_hidden_state()
            total_loss = 0
            for (batch, (inp, targ)) in enumerate(dataset.take(steps_per_epoch)):
                batch_loss = self.train_step(inp, targ, enc_hidden)
                total_loss += batch_loss
                if batch % 100 == 0:
                    print('Epoch {} Batch {} Loss {:.4f}'.format(epoch + 1, batch, batch_loss.numpy()))
            if (epoch + 1) % 2 == 0:
                self.checkpoint.save(file_prefix=self.directory)
            print("Epoch {} Loss {:.4f}".format(epoch + 1, total_loss / steps_per_epoch))
            print("Time take for 1 epoch {} sec \n".format(time.time() - start))
