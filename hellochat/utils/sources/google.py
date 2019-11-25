from hellochat.utils.sources.compression import Compression


class Google(Compression):

    def __init__(self, destination_folder):
        super().__init__(destination_folder)
