import abc
import json
import os

import chardet as chardet
import pandas as pd


class FileProcessingAbstract(object):

    default_file_format = ""
    file_objects = []

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.__file_format = self.default_file_format
        self.__metadata = {}
        self.__file_stream = FileProcessingAbstract.open_file(self.file_path, "rb")
        self.__file_df = None
        self.extract_data_from_file()

    def __new__(cls, *args, **kwargs):
        instance = super(FileProcessingAbstract, cls).__new__(cls)
        cls.file_objects.append(instance)
        return instance

    def __del__(self):
        self.file_objects.pop(self.file_objects.index(self))

    def __str__(self):
        return json.dumps(self.metadata, indent=2)

    @property
    def file_format(self):
        return self.__file_format

    @file_format.setter
    def file_format(self, val):
        if self.file_format == "" or self.file_format is None:
            self.__file_format = val
        else:
            raise Exception("The file format is immutable")

    @property
    def metadata(self):
        return self.__metadata

    @metadata.setter
    def metadata(self, val):
        if self.metadata == {} or self.metadata is None:
            self.__metadata = val
        else:
            raise Exception("The file metadata is immutable")

    @property
    def file_stream(self):
        return self.__file_stream

    @file_stream.setter
    def file_stream(self, val):
        if self.file_stream is None:
            self.__file_stream = val
        else:
            raise Exception("The file stream is immutable")

    @property
    def file_df(self):
        return self.__file_df

    @file_df.setter
    def file_df(self, val):
        if self.file_df is None:
            self.__file_df = val
        else:
            raise Exception("The file data frame is immutable")

    @file_stream.deleter
    def file_stream(self):
        self.file_stream.close()

    @abc.abstractmethod
    def extract_data_from_file(self, sample_size=1024):
        """
            sets a dictionary in the instance variable "file_metadata"
        :param sample_size: int
        """
        metadata = {}
        self.metadata = metadata
        raise Exception("Must be implemented in every subclass")

    @staticmethod
    def open_file(file_path: str, opening_mode: str):
        """
            create a stream from a file
        :param opening_mode:
        :param file_path:
        :param path:
        :return: io.stream
        """
        return open(file_path, opening_mode)


class CSVProcessing(FileProcessingAbstract):

    default_file_format = "csv"

    def __init__(self, file_path: str):
        super().__init__(file_path)

    def extract_data_from_file(self, sample_size=1024):
        try:
            file_size = os.path.getsize(self.file_path)
            sample = self.file_stream.read(sample_size)
            result = chardet.detect(sample)
            detected_encoding = result['encoding']
            detected_confidence = result['confidence']
            self.file_df = pd.read_csv(self.file_path, encoding=detected_encoding)
            total_columns = self.file_df.shape[1]
            column_names = self.file_df.columns.tolist()
            converted_file_path = self.file_path.replace('.csv', '_converted_utf-8.csv')
            self.metadata = {
                "file_size": file_size,
                "detected_encoding": detected_encoding,
                "detected_confidence": detected_confidence,
                "total_columns": total_columns,
                "column_names": column_names,
                "converted_file_path": converted_file_path

            }
        except Exception as e:
            print(f"Error al detectar información del archivo CSV: {str(e)}")


if __name__ == "__main__":
    csvObj = CSVProcessing(r"path/to/csv")
    print(csvObj)