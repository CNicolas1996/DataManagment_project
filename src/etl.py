import pandas as pd
import chardet
import codecs
import os


class DataProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.detected_encoding = None
        self.detected_confidence = None
        self.total_rows = None
        self.total_columns = None
        self.column_names = None
        self.converted_file_path = None
        self.data = None

    def detect_file_info(self, sample_size=1024):
        try:
            # Obtener el tamaño total del archivo en bytes
            file_size = os.path.getsize(self.file_path)

            with open(self.file_path, 'rb') as f:
                # Leer una muestra para la detección del encoding
                sample = f.read(sample_size)
                result = chardet.detect(sample)
                self.detected_encoding = result['encoding']
                self.detected_confidence = result['confidence']

            # Estimar el número total de filas basado en el tamaño del archivo y la muestra
            estimated_rows = int((file_size / sample_size) * 1000)

            # Obtener el número total de columnas (usando nrows=0 para obtener solo el encabezado)
            df_info = pd.read_csv(self.file_path, nrows=0, encoding=self.detected_encoding)
            self.total_columns = df_info.shape[1]

            # Obtener los nombres de las columnas
            self.column_names = df_info.columns.tolist()

            print(f"Información del archivo CSV detectada:")
            print(f"  - Encoding: {self.detected_encoding} (Confidence: {self.detected_confidence})")
            print(f"  - Tamaño total del archivo: {file_size} bytes")
            print(f"  - Estimación de filas: {estimated_rows}")
            print(f"  - Número total de columnas: {self.total_columns}")
            print(f"  - Nombres de las columnas: {self.column_names}")

            # Crear un nuevo nombre de archivo para el archivo convertido
            self.converted_file_path = self.file_path.replace('.csv', '_converted_utf-8.csv')
        except Exception as e:
            print(f"Error al detectar información del archivo CSV: {str(e)}")

    def convert_to_utf8(self):
        try:
            if self.detected_encoding.lower() != 'utf-8':
                # Convertir el archivo a utf-8 usando codecs
                with codecs.open(self.file_path, 'r', encoding=self.detected_encoding, errors='replace') as source_file:
                    with codecs.open(self.converted_file_path, 'w', encoding='utf-8') as target_file:
                        target_file.write(source_file.read())

                print(f"Archivo convertido a UTF-8: {self.converted_file_path}")
            else:
                print("El archivo ya está en formato UTF-8. No es necesaria la conversión.")
        except Exception as e:
            print(f"Error durante la conversión a UTF-8: {str(e)}")

    import pandas as pd

    def read_csv(self, chunk_size=None, usecols=None, skiprows=None, sep=','):
        try:
            if self.detected_encoding is None:
                self.detect_file_info()

            # Convertir a utf-8 por defecto
            self.convert_to_utf8()

            # Leer el archivo CSV convertido o original en chunks
            file_path_to_read = self.converted_file_path or self.file_path
            print(f"Intentando leer el archivo convertido: {file_path_to_read}")

            if chunk_size is None:
                self.data = pd.read_csv(file_path_to_read, usecols=usecols, skiprows=skiprows, sep=sep,
                                        encoding='utf-8', on_bad_lines='warn')

                # Reporte de la información del archivo (si se leyó completo)
                print(f"Lectura exitosa del archivo CSV.")
                print(f"- Número de filas: {len(self.data)}")
                print(f"- Número de columnas: {len(self.data.columns)}")
                print(f"- Columnas: {list(self.data.columns)}")



                yield self.data  # Devuelve el DataFrame completo

            else:
                chunks = pd.read_csv(file_path_to_read, chunksize=chunk_size, usecols=usecols, skiprows=skiprows,
                                     sep=sep, encoding='utf-8', on_bad_lines='warn')
                for chunk in chunks:
                    yield chunk  # Devuelve cada chunk como un DataFrame

        except Exception as e:
            print(f"Error al leer el archivo CSV: {str(e)}")

    def get_column_names(self):
        return self.column_names


'''Ejemplo de uso y pruebitas'''

if __name__ == "__main__":
    csv_path = r"C:\Repositorios\DataManagment_project\TestData\BASE_DE_DATOS_DE_EMPRESAS_Y_O_ENTIDADES_ACTIVAS_-_JURISDICCI_N_C_MARA_DE_COMERCIO_DE_IBAGU__-_CORTE_A_31_DE_AGOSTO_DE_2023.csv"

    data_processor = DataProcessor(csv_path)
    data = data_processor.read_csv()

    for chunk in data_processor.read_csv(chunk_size=1000):
        print(chunk.shape)








