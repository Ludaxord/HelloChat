from hellochat.utils.compression import Compression

compress = Compression("data/unpacked")

# json = compress.decompress_file("data/RC_2005-12.bz2", ".json")
# json = compress.decompress_file("data/RC_2019-06.zst", ".json")

# compress.download_dataset()

dataset = compress.decompress_folder("data/")
