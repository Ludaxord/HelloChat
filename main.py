from hellochat.utils.parser import chat_args
from hellochat.utils.sources.apple_imessage import AppleIMessage
from hellochat.utils.sources.facebook_messenger import FacebookMessenger
from hellochat.utils.sources.reddit import Reddit


def set_compressor(source_name):
    if source_name == "imessage":
        imessage = AppleIMessage(destination)
        imessage.set_values_to_db()
    elif source_name == "reddit":
        reddit = Reddit(destination)
        # json = reddit.decompress_file("data/RC_2005-12.bz2", ".json")
        # json = reddit.decompress_file("data/RC_2019-06.zst", ".json")
        # reddit.download_dataset()
        # dataset = reddit.decompress_folder("data/")
        reddit.init_downloaded_data()
    elif source_name == "messenger":
        messenger = FacebookMessenger(destination)


args = chat_args()

source = args.source
sources = args.sources
destination = "data/unpacked"

print(source)
print(sources)

if source is not None:
    set_compressor(source)
elif sources is not None:
    for s in sources:
        set_compressor(s)
