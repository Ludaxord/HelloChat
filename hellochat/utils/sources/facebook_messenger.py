from hellochat.utils.compression import Compression


class FacebookMessenger(Compression):
    def __init__(self, destination_folder):
        super().__init__(destination_folder)
