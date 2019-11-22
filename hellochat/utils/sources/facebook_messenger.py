from hellochat.utils.compression import Compression


class FacebookMessenger(Compression):
    def __init__(self, destination_folder):
        super().__init__(f"{destination_folder}/messenger")
