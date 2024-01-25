#alternativa 1, herencia
class FileProcessingAbstract(object):

    __file_format = ""

    def __init__(self, file_path: str):
        self.__file_path = file_path
        self.file_format = self.__file_format

    @property
    def file_format(self):
        return self.__file_format

    @file_format.setter
    def file_format(self, val):
        self.__file_format = val

    @property
    def file_path(self):
        return self.__file_path


class CSVProcessing1(FileProcessingAbstract):

    __file_format = "csv"

    def __init__(self, file_path):
        super().__init__(file_path)
        self.file_format = self.__file_format
    # agregar aqui todos metodos para manejar csvs

#alternativa 2, uso de meta clases
class FileProcessingUtilsMeta(type):
    def __new__(mcs, name, bases, dictionary):
        supported_formats = ["csv", "xml", "json"]
        class_format = name.split("Processing")[0].lower()
        if class_format not in supported_formats:
            raise Exception("The class you are implementing is not yet supported")
        dictionary.setdefault("file_format", name.split("Processing")[0].lower())
        dictionary.setdefault("get_file_format", lambda self: self.file_format)
        dictionary.setdefault("file_path", None)
        dictionary.setdefault("get_file_path", lambda self: self.file_path)
        obj = super().__new__(mcs, name, bases, dictionary)
        return obj


class CSVProcessing2(metaclass=FileProcessingUtilsMeta):
    # ejemplo de un clase creada a partir de la metaclase
    def __init__(self, file_path: str):
        self.file_path = file_path

    # agregar aqui todos metodos para manejar csvs


#alternativa 3, uso del patron builder - usar si empieza a notar que las clases que manejan los archivos empiezan a pedir
# muchos parametros en el __init__, o que el __init__ en si mismo empieza a crecer mucho

class FileProcessingDirector:

    __builder = None

    def __init__(self, file_path):
        self.__file_path = file_path

    def set_builder(self, builder):
        self.__builder = builder()
        self.__class_To_build = FileProcessingAbstract
        return self

    def make_file_processing_obj(self):
        file_obj = self.__class_To_build(self.__file_path)
        file_obj.file_format = self.__builder.get_file_format()
        return file_obj


class CSV_Builder:
    __FILE_FORMAT = "csv"

    def get_file_format(self):
        return self.__FILE_FORMAT



if __name__ == "__main__":
    csv_obj_1 = CSVProcessing1("path1")
    csv_obj_2 = CSVProcessing2("path2")
    csv_obj_3 = FileProcessingDirector("path3").set_builder(CSV_Builder).make_file_processing_obj()
    print(csv_obj_1.file_path)
    print(csv_obj_1.file_format)
    print("")
    print(csv_obj_2.file_path)
    print(csv_obj_2.file_format)
    print("")
    print(csv_obj_3.file_path)
    print(csv_obj_3.file_format)
