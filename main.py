from hellochat.utils.tools.parser import chat_args, set_compressor

args = chat_args()

source = args.source
sources = args.sources
destination = "data/unpacked"

print(source)
print(sources)

if source is not None:
    compressor = set_compressor(source, destination)
    compressor.set_values_to_db()
elif sources is not None:
    for s in sources:
        compressor = set_compressor(s, destination)
        compressor.set_values_to_db()
