# Data Processing Improved Script

## Descripción
Este script `data_processing_improved.py` está diseñado para realizar operaciones de procesamiento de datos en un entorno de PySpark. Incorpora una serie de mejoras como la modularización del código, la carga de configuraciones desde un archivo JSON, 
y la mejora en la legibilidad y mantenimiento del código.

## Dependencias
- Python 3.x
- PySpark
- Pandas

## Configuración
Antes de ejecutar el script, asegúrate de tener un archivo `config.json` en el mismo directorio. Este archivo debe contener las rutas a los datos y cualquier otra configuración relevante para el script.

Ejemplo de `config.json`:
```json
{
    "model_paths": {
        "train_data": "path/to/train_data",
        "test_data": "path/to/test_data",
        "oot_data": "path/to/oot_data",
        "model_variables_csv": "path/to/model_variables.csv"
    },
    "model_name": "NombreDelModelo"
}
```

## Uso
Para ejecutar el script, simplemente utiliza el comando:
```python
spark-submit data_processing_improved.py
```

Este comando iniciará el proceso de procesamiento de datos según las especificaciones del script y la configuración proporcionada.

## Documentación del Código
### Funciones Principales

get_spark_session(app_name: str) -> SparkSession: Inicializa y retorna una sesión de Spark.

load_data(spark, path): Carga datos desde un archivo Parquet.

analyze_grouped_data(df: DataFrame, group_cols: list) -> DataFrame: Realiza análisis de duplicados en un DataFrame.

average_target_by_group(df: DataFrame, group_col: str) -> DataFrame: Calcula el promedio de la columna 'target' agrupada por una columna específica.

drop_duplicates(df: DataFrame, cols: list) -> DataFrame: Elimina duplicados en un DataFrame basado en columnas específicas.

get_min_max_values(df: DataFrame, col_name: str): Calcula los valores mínimos y máximos de una columna en un DataFrame.

read_model_variables(csv_path: str, model_name: str): Lee y procesa las variables de modelamiento desde un archivo CSV.

join_table_with_dataframes(spark, df_train, df_test, df_oot, table_name, variables, codmes_range): Une una tabla con los DataFrames de train, test y oot.

## Estructura del Código
El script sigue una estructura modular con funciones claramente definidas para cada paso del proceso de procesamiento de datos. La función main() coordina el flujo general del script.

## Contribuciones
Las contribuciones al script son bienvenidas. Por favor, sigue las mejores prácticas de codificación y documentación al realizar cambios o mejoras.

## Licencia
Incluye información sobre la licencia si es aplicable.
