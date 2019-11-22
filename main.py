from hellochat.utils.parser import chat_args
from hellochat.utils.sources.apple_imessage import AppleIMessage
from hellochat.utils.sources.facebook_messenger import FacebookMessenger
from hellochat.utils.sources.reddit import Reddit

args = chat_args()

source = args.source
if source == "imessage":
    imessage = AppleIMessage("data/unpacked")
    imessage.get_imessages()
elif source == "reddit":
    reddit = Reddit("data/unpacked")
    # json = compress.decompress_file("data/RC_2005-12.bz2", ".json")
    # json = compress.decompress_file("data/RC_2019-06.zst", ".json")
    # compress.download_dataset()
    # dataset = compress.decompress_folder("data/")
    reddit.init_downloaded_data()
elif source == "messenger":
    messenger = FacebookMessenger("data/unpacked")
