import lzma
from bz2 import BZ2File
from fileinput import FileInput
from pathlib import Path

import pandas as pd


class Compression:
    destination_folder = None

    def __init__(self, destination_folder):
        self.destination_folder = destination_folder

    def decompress_folder(self, compress_folder):
        f_list = self.__get_dir_files(compress_folder)
        dataset = pd.DataFrame()
        for file in f_list:
            try:
                json = self.decompress_file(file, ".json")
                csv, part_dataset = self.json_to_csv(json, True)
                dataset.join(part_dataset)
            except Exception as e:
                print(f"cannot decompress file {file}, {e}")
        return dataset

    def decompress_file(self, file_path, with_extension):
        file_path = Path(file_path)
        if file_path.suffix == ".bz2":
            self.__decompress_bz2_file(file_path, with_extension)
        elif file_path.suffix == ".zst":
            self.__decompress_zst_file(file_path, with_extension)
        elif file_path.suffix == ".xz":
            self.__decompress_xz_file(file_path, with_extension)
        file_name = f"{self.destination_folder}/{file_path.name}{with_extension}"
        self.replace_occurrences(file_name, "}", "},")

        return file_name

    def replace_occurrences(self, from_file, replace_from, replace_to):
        with FileInput(from_file, inplace=True) as file:
            for line in file:
                print(line.replace(replace_from, replace_to), end='')
        self.__append_array_to_file(from_file)
        print(f"all occurrences of {replace_from} in file {from_file} replaced with {replace_to}")

    def json_to_csv(self, json_file, with_data_frame):
        file_path = Path(json_file)
        with open(file_path) as file_input:
            data_frame = pd.read_json(file_input)
        csv_file = f"{self.destination_folder}/{file_path.name}.csv"
        data_frame.to_csv(csv_file, index=False)

        if with_data_frame:
            return csv_file, data_frame
        else:
            return csv_file, None

    def __get_dir_files(self, compress_folder):
        flist = []
        for p in Path(compress_folder).iterdir():
            if p.is_file():
                print(p)
                flist.append(p)
        return flist

    def __decompress_xz_file(self, file, with_extension):
        pass

    def __decompress_zst_file(self, file, with_extension):
        unpacked_file = lzma.open(file)
        data = unpacked_file.read()
        file_name = f"{self.destination_folder}/{file.name}{with_extension}"
        open(file_name, 'wb').write(data)
        print(f"unpacked bz2 file completed to {file_name}")

    def __decompress_bz2_file(self, file, with_extension):
        unpacked_file = BZ2File(file)
        data = unpacked_file.read()
        file_name = f"{self.destination_folder}/{file.name}{with_extension}"
        open(file_name, 'wb').write(data)
        print(f"unpacked bz2 file completed to {file_name}")

    def __append_array_to_file(self, file_name):
        with open(f'{file_name}', 'r+') as f:
            lines = f.readlines()
            f.seek(0)
            f.write("[")
            for line in lines:
                f.write(line)
            f.write(self.__replace_right(lines[-1], ",", "", 1))
            f.write("]")
            f.close()

    def __replace_right(self, source, target, replacement, replacements=None):
        return replacement.join(source.rsplit(target, replacements))
