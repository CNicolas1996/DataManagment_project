import pandas as pd
import chardet
import codecs
import os
import numpy as np


def correct_bad_line(bad_line, column_names):
    # Reemplaza los campos adicionales con valores nulos
    bad_line = bad_line[:len(column_names)]
    for i in range(len(column_names), len(bad_line)):
        bad_line[i] = np.nan

    return bad_line


class ChunkedDataFrame:
    def __init__(self, data):
        self.data = data

    def report(self):
        """
        Genera un reporte de los chunks del archivo CSV.

        Devuelve:
            Un DataFrame con la siguiente información:
                * `chunk_number`: El número del chunk.
                * `chunk_size`: El tamaño del chunk.
                * `chunk_start`: El índice de inicio del chunk.
                * `chunk_end`: El índice de finalización del chunk.
        """

        chunks = self.data.to_list()

        chunks_description = {
            "chunk_number": [c + 1 for c in range(len(chunks))],
            "chunk_size": [len(chunk) for chunk in chunks],
            "chunk_start": [chunk.index[0] for chunk in chunks],
            "chunk_end": [chunk.index[-1] for chunk in chunks],
        }

        return pd.DataFrame(chunks_description)
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
        self.chunks_description = None

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

    def read_csv_summary(self, chunk_size=None, usecols=None, skiprows=None, sep=','):
        try:
            if self.detected_encoding is None:
                self.detect_file_info()

            self.convert_to_utf8()

            file_path_to_read = self.converted_file_path or self.file_path
            print(f"Intentando leer el archivo convertido: {file_path_to_read}")

            if chunk_size is None:
                self.data = pd.read_csv(file_path_to_read, usecols=usecols, skiprows=skiprows, sep=sep,
                                        encoding='utf-8', on_bad_lines='warn')

                print(f"Lectura exitosa del archivo CSV.")
                print(f"- Número de filas: {len(self.data)}")
                print(f"- Número de columnas: {len(self.data.columns)}")

                return self.data  # Devuelve el DataFrame completo

            else:
                chunks_info = []  # Lista para almacenar información de los chunks
                problematic_lines = []  # Lista para almacenar líneas problemáticas

                chunks = pd.read_csv(file_path_to_read, chunksize=chunk_size, usecols=usecols, skiprows=skiprows,
                                     sep=sep, encoding='utf-8', on_bad_lines='warn')

                for i, chunk in enumerate(chunks):
                    # Procesa cada chunk
                    chunk_info = {
                        "Chunk": i + 1,
                        "Número de filas": len(chunk),
                        "Número de columnas": len(chunk.columns),
                    }
                    chunks_info.append(chunk_info)

                    # Detecta y almacena líneas con un número incorrecto de campos
                    for j, line in enumerate(chunk.iterrows()):
                        if len(line[1]) != len(chunk.columns):
                            problematic_lines.append({
                                "Chunk": i + 1,
                                "Número de línea": j + 1,
                                "Número de campos": len(line[1]),
                                "Campos": line[1].tolist(),
                            })

                # Convierte la lista de información de chunks y líneas problemáticas a DataFrames
                chunks_summary = pd.DataFrame(chunks_info)
                problematic_lines_df = pd.DataFrame(problematic_lines)

                print(chunks_summary)
                print(problematic_lines_df)

                # Reinicia la lectura para procesar los chunks posteriormente
                chunks = pd.read_csv(file_path_to_read, chunksize=chunk_size, usecols=usecols, skiprows=skiprows,
                                     sep=sep, encoding='utf-8', on_bad_lines='warn')

                return chunks_summary, problematic_lines_df, chunks  # Devuelve el resumen de chunks, líneas problemáticas y los chunks para su procesamiento posterior

        except Exception as e:
            print(f"Error al leer el archivo CSV: {str(e)}")


'''Ejemplo de uso y pruebitas'''

# Ejemplo de uso
if __name__ == "__main__":
    csv_path = r"C:\Repositorios\DataManagment_project\TestData\BASE_DE_DATOS_DE_EMPRESAS_Y_O_ENTIDADES_ACTIVAS_-_JURISDICCI_N_C_MARA_DE_COMERCIO_DE_IBAGU__-_CORTE_A_31_DE_AGOSTO_DE_2023.csv"

    data_processor = DataProcessor(csv_path)
    chunks_summary, _, chunks = data_processor.read_csv_summary(chunk_size=1000)


    # Imprimir el DataFrame con la descripción de los chunks









