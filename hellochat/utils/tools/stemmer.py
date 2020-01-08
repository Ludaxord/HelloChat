from stempel import StempelStemmer


class Stemmer:

    def __init__(self, words):
        self.words = words

    def stem_using_stempel(self, stem_type="default", words=None):
        if stem_type == "polimorf":
            stemmer = StempelStemmer.polimorf()
        else:
            stemmer = StempelStemmer.default()
        if words is None:
            words = self.words
        stem_words = [stemmer.stem(w) for w in words]
        return stem_words
