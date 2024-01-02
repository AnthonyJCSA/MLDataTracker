# Creación del contenido final para el archivo data_processing_improved.py

import json
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, countDistinct, count, when, avg, min, max

class SparkSessionManager:
    @staticmethod
    def get_spark_session(app_name: str) -> SparkSession:
        return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, path):
    try:
        return spark.read.format("parquet").load(path)
    except Exception as e:
        print(f"Error al cargar los datos desde {path}: {e}")
        return None

def analyze_grouped_data(df: DataFrame, group_cols: list) -> DataFrame:
    return df.groupby(*group_cols).agg(
        countDistinct('codinternocomputacional').alias('CTD_DISTINCT_CIC'),
        count('*').alias('CTD_TOTAL_CIC'),
        when(countDistinct('codinternocomputacional') == count('*'), 'SI').otherwise('NO').alias('CONDICIONAL')
    )

def average_target_by_group(df: DataFrame, group_col: str) -> DataFrame:
    return df.groupby(group_col).agg(avg('target').alias('PROMEDIO_SCORE')).orderBy(group_col)

def drop_duplicates(df: DataFrame, cols: list) -> DataFrame:
    return df.dropDuplicates(cols)

def get_min_max_values(df: DataFrame, col_name: str):
    min_value = df.select(min(col_name)).collect()[0][0]
    max_value = df.select(max(col_name)).collect()[0][0]
    return min_value, max_value

def read_model_variables(csv_path: str, model_name: str):
    try:
        variables_df = pd.read_csv(csv_path, sep=",")
        variables_df = variables_df[variables_df["Modelo"] == model_name]
        variables_df['Tabla'] = variables_df['Tabla'].str.split('.', n=1).str[1]
        return variables_df
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return pd.DataFrame()

def join_table_with_dataframes(spark, df_train, df_test, df_oot, table_name, variables, codmes_range):
    df_table = spark.table(table_name).where(f"codmes >= {codmes_range[0]} and codmes <= {codmes_range[1]}").select(['codmes', 'codinternocomputacional'] + variables)
    df_table = df_table.select(['codmes', 'codinternocomputacional'] + [col(c).alias("__raw_" + c) for c in variables])
    df_train_joined = df_train.join(df_table, on=['codmes', 'codinternocomputacional'], how='left')
    df_test_joined = df_test.join(df_table, on=['codmes', 'codinternocomputacional'], how='left')
    df_oot_joined = df_oot.join(df_table, on=['codmes', 'codinternocomputacional'], how='left')
    return df_train_joined, df_test_joined, df_oot_joined

def main():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    
    spark = SparkSessionManager.get_spark_session(config["model_name"])

    df_train = load_data(spark, config['model_paths']['train_data'])
    df_test = load_data(spark, config['model_paths']['test_data'])
    df_oot = load_data(spark, config['model_paths']['oot_data'])

    # (Resto del código incluyendo todas las funciones y lógicas adicionales...)

if __name__ == "__main__":
    main()

