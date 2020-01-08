from hellochat.utils.tools.parser import chat_args, set_compressor
from hellochat.utils.tools.printers import print_yellow
from hellochat.utils.tools.stemmer import Stemmer

args = chat_args()

source = args.source
sources = args.sources
engine = args.engine
lang = args.lang
words = args.words
destination = "data/unpacked"

dataset = None

print(f"source => {source}")
print(f"sources => {sources}")

if source is not None:
    compressor = set_compressor(source, destination)
    compressor.set_values_to_db()
elif sources is not None and engine is None and lang is None:
    for s in sources:
        compressor = set_compressor(s, destination)
        if compressor is not None:
            print_yellow(f"running source {s} and setting values to db")
            compressor.set_values_to_db()
            dataset = compressor.dataset
elif sources is not None and engine is not None and lang is None:
    for s in sources:
        compressor = set_compressor(s, destination)
        if compressor is not None:
            print_yellow(f"running source {s} and backend {engine}")
            compressor.run_backend(engine)
elif sources is not None and engine is not None and lang is not None:
    for s in sources:
        compressor = set_compressor(s, destination)
        if compressor is not None:
            print_yellow(f"running source {s} and backend {engine}")
            compressor.run_backend(engine, lang)
elif source is None and sources is None and words is not None:
    stem = Stemmer(words=words)
    stem_words = stem.stem_using_stempel(stem_type="polimotf")
    print_yellow(stem_words)
