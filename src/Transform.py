import pandas as pd
import missingno as msno
import matplotlib.pyplot as plt
import seaborn as sns

class DataTransformer:
    def __init__(self, data):
        self.data = data
        self.null_handling_results = pd.DataFrame()

    def visualize_null_data(self):
        if isinstance(self.data, pd.io.parsers.TextFileReader):
            # Concatenar todos los chunks en un solo DataFrame
            concatenated_data = pd.concat(self.data, ignore_index=True)

            # Configurar el estilo de seaborn (opcional)
            sns.set(style="darkgrid")

            # Visualizar la matriz de nulos para el DataFrame completo con Seaborn
            plt.figure(figsize=(20, 15))
            ax = sns.heatmap(concatenated_data.isnull(), cbar=False, cmap="Blues", yticklabels=False)

            ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
            plt.title("Visualización de Nulos - DataFrame Completo")
            self.fig = plt.gcf()  # Obtener la figura actual
            plt.close()  # Cerrar la ventana de visualización

        elif isinstance(self.data, pd.DataFrame):
            # Configurar el estilo de seaborn (opcional)
            sns.set(style="whitegrid")

            # Visualizar la matriz de nulos para el DataFrame completo con Seaborn
            plt.figure(figsize=(20, 15))
            ax = sns.heatmap(self.data.isnull(), cbar=False, cmap="Blues", yticklabels=False)

            # Girar los labels 90 grados y ubicarlos de arriba a abajo
            ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
            plt.title("Visualización de Nulos - DataFrame Completo")
            self.fig = plt.gcf()  # Obtener la figura actual
            plt.close()  # Cerrar la ventana de visualización

        else:
            raise ValueError("El tipo de datos no es compatible.")

    def save_visualization(self, filename="visualization.png"):
        if self.fig:
            self.fig.savefig(filename)
            print(f"La visualización se ha guardado en {filename}")
        else:
            print("No se ha generado ninguna visualización.")

    def analyze_missing_values(self):
        if isinstance(self.data, pd.io.parsers.TextFileReader):
            # Analizar valores nulos para cada chunk
            results = {}
            for i, chunk in enumerate(self.data):
                chunk_info = self._get_chunk_info(chunk)
                results[f'Chunk {i + 1}'] = chunk_info
            return results
        elif isinstance(self.data, pd.DataFrame):
            # Analizar valores nulos para el DataFrame completo
            detailed_report = self._get_chunk_info(self.data)
            return detailed_report
        else:
            raise ValueError("El tipo de datos no es compatible.")

    def _get_chunk_info(self, chunk):
        null_counts = chunk.isnull().sum()
        columns_with_nulls = null_counts[null_counts > 0].index.tolist()

        # Crear DataFrame con información sobre valores nulos
        chunk_info = pd.DataFrame({'Null Count': null_counts[columns_with_nulls]})

        # Agregar columna con porcentaje de valores nulos
        chunk_info['Null Percentage'] = chunk_info['Null Count'] / len(chunk) * 100

        # Agregar columna con el tipo de datos de cada columna
        chunk_info['Data Type'] = chunk.dtypes[columns_with_nulls]

        # Agregar columna con el valor más frecuente (moda) y su frecuencia
        chunk_info['Most Frequent Value'] = chunk.mode().iloc[0][columns_with_nulls]
        chunk_info['Frequency'] = chunk.apply(
            lambda col: col.value_counts().iloc[0] if col.name in columns_with_nulls else None)

        # Establecer la primera columna como índice y darle un título
        chunk_info.index.name = 'Column'
        chunk_info.columns.name = 'Missing Values Report'

        return chunk_info, columns_with_nulls

    import pandas as pd

    def fill_missing_values(self, fill_method='ffill'):
        """
        Llena los valores faltantes en el DataFrame utilizando el método especificado.

        Parámetros:
        - fill_method: str, el método de llenado a utilizar ('ffill', 'mode', o 'bfill').

        Retorna:
        - DataFrame, el DataFrame con los valores faltantes llenados.
        """
        if fill_method == 'mode':
            # Llenar utilizando la moda para todas las columnas
            self.data = self.data.apply(lambda x: x.fillna(x.mode().iat[0]))
            print("Valores nulos llenados en el DataFrame usando moda.")
        elif fill_method == 'ffill':
            # Llenar hacia adelante
            self.data = self.data.ffill()
        elif fill_method == 'bfill':
            # Llenar hacia atrás
            self.data = self.data.bfill()
        else:
            # Si el método no es reconocido, mostrar un mensaje de error
            raise ValueError("Método de llenado no reconocido. Use 'ffill', 'mode', o 'bfill'.")

        return self.data

    def handle_missing_values(self, method='drop', columns=None, global_handling=False, fill_method=None):
        if columns is None:
            columns = self.data.columns

        if global_handling:
            print(f"Applying global handling for method: {method}")
            if method == 'drop':
                # Almacena resultados antes de manejar los valores nulos
                before_handling = self.data[columns].isnull().sum().rename('Before Handling')

                # Aplica lógica de manejo de valores nulos al DataFrame completo
                self.data.dropna(subset=columns, inplace=True)

                # Almacena resultados después de manejar los valores nulos
                after_handling = self.data[columns].isnull().sum().rename('After Handling')

                # Actualiza el registro de resultados
                self.null_handling_results = pd.concat([self.null_handling_results, before_handling, after_handling],
                                                       axis=1)
                print(f"Valores nulos manejados en el DataFrame para las columnas {columns}")
            elif method == 'fill':
                if fill_method:
                    self.fill_missing_values(fill_method)
                else:
                    print("Debe especificar un método de llenado para 'fill'.")

                # Almacena resultados después de manejar los valores nulos
                after_handling = self.data[columns].isnull().sum().rename('After Handling')
                # Actualiza el registro de resultados
                self.null_handling_results = pd.concat([self.null_handling_results, after_handling], axis=1)
            elif callable(method):
                # Almacena resultados antes de manejar los valores nulos
                before_handling = self.data[columns].isnull().sum().rename('Before Handling')

                # Si se proporciona una función, aplica la función al DataFrame completo
                self.data[columns] = self.data[columns].apply(method)

                # Almacena resultados después de manejar los valores nulos
                after_handling = self.data[columns].isnull().sum().rename('After Handling')

                # Actualiza el registro de resultados
                self.null_handling_results = pd.concat([self.null_handling_results, before_handling, after_handling],
                                                       axis=1)
                print(f"Valores nulos manejados en el DataFrame para las columnas {columns}")
            else:
                print("Método no válido. Use 'drop', 'fill', o una función personalizada.")

            # Devuelve información que pueda ser útil
            return self.null_handling_results










