from hellochat.utils.parser import chat_args
from hellochat.utils.sources.apple_imessage import AppleIMessage
from hellochat.utils.sources.facebook_messenger import FacebookMessenger
from hellochat.utils.sources.reddit import Reddit

args = chat_args()

source = args.source
destination = "data/unpacked"

if source == "imessage":
    imessage = AppleIMessage(destination)
    dataset = imessage.load_imessages_from_file()
    print(dataset)
elif source == "reddit":
    reddit = Reddit(destination)
    # json = reddit.decompress_file("data/RC_2005-12.bz2", ".json")
    # json = reddit.decompress_file("data/RC_2019-06.zst", ".json")
    # reddit.download_dataset()
    # dataset = reddit.decompress_folder("data/")
    reddit.init_downloaded_data()
elif source == "messenger":
    messenger = FacebookMessenger(destination)
