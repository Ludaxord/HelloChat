from hellochat.utils.tools.parser import chat_args, set_compressor

args = chat_args()

source = args.source
sources = args.sources
destination = "data/unpacked"

dataset = None

print(f"source => {source}")
print(f"sources => {sources}")

if source is not None:
    compressor = set_compressor(source, destination)
    compressor.set_values_to_db()
elif sources is not None:
    for s in sources:
        compressor = set_compressor(s, destination)
        if compressor is not None:
            compressor.set_values_to_db()
            dataset = compressor.dataset
