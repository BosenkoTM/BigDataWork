#!/usr/bin/env python3
"""
Анализ данных землетрясений с использованием PySpark
Задача: найти тип землетрясения с максимальной средней магнитудой
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, stddev, min as spark_min, max as spark_max
from pyspark.sql.types import FloatType
import sys

def create_spark_session():
    """Создать Spark сессию"""
    spark = SparkSession.builder \
        .appName("Earthquake Analysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    return spark

def load_data(spark, filepath):
    """Загрузить данные из HDFS или локального файла"""
    print(f"Загрузка данных из: {filepath}")
    
    try:
        # Попробовать загрузить из HDFS
        df = spark.read.csv(filepath, header=True, inferSchema=True)
        print(f"Данные загружены из HDFS")
    except:
        # Если не удалось, загрузить локально
        df = spark.read.csv(filepath, header=True, inferSchema=True)
        print(f"Данные загружены локально")
    
    print(f"Количество строк: {df.count()}")
    return df

def clean_and_prepare(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {df.count()}")
    
    # Удалить строки с null в Magnitude
    df = df.filter(col('Magnitude').isNotNull())
    
    # Заполнить null в Type
    df = df.na.fill('Unknown', subset=['Type'])
    
    print(f"Количество строк после очистки: {df.count()}")
    print(f"Уникальных типов: {df.select('Type').distinct().count()}")
    
    return df

def analyze_magnitude_by_type(df):
    """Анализ средней магнитуды по типам"""
    print("\n=== Анализ средней магнитуды по типам ===")
    
    # Группировка и агрегация
    result = df.groupBy('Type') \
        .agg(
            avg('Magnitude').alias('Mean_Magnitude'),
            count('*').alias('Count'),
            stddev('Magnitude').alias('Std'),
            spark_min('Magnitude').alias('Min_Magnitude'),
            spark_max('Magnitude').alias('Max_Magnitude')
        ) \
        .orderBy(col('Mean_Magnitude').desc())
    
    return result

def main():
    # Путь к данным
    hdfs_path = "hdfs://localhost:9000/user/hadoop/input/database.csv"
    local_path = "/opt/data/database.csv"
    
    # Создать Spark сессию
    spark = create_spark_session()
    
    print("=== Анализ данных землетрясений с использованием PySpark ===")
    
    # Показать конфигурацию
    print("\n=== Конфигурация Spark ===")
    print(f"Version: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")
    
    # Загрузить данные
    try:
        df = load_data(spark, hdfs_path)
    except Exception as e:
        print(f"Не удалось загрузить из HDFS: {e}")
        print("Попытка загрузить локально...")
        df = load_data(spark, local_path)
    
    # Показать схему и первые строки
    print("\n=== Схема данных ===")
    df.printSchema()
    print("\nПервые 5 строк:")
    df.show(5)
    
    # Очистка данных
    df_clean = clean_and_prepare(df)
    
    # Анализ
    result = analyze_magnitude_by_type(df_clean)
    
    # Показать результаты
    print("\n=== Результаты ===")
    print("\nТипы землетрясений по средней магнитуде (топ-10):")
    result.show(10, truncate=False)
    
    # Найти максимальный тип
    max_row = result.first()
    print(f"\nТип землетрясения с максимальной средней магнитудой: '{max_row['Type']}'")
    print(f"Средняя магнитуда: {max_row['Mean_Magnitude']:.2f}")
    print(f"Количество землетрясений: {max_row['Count']}")
    print(f"Минимальная магнитуда: {max_row['Min_Magnitude']:.2f}")
    print(f"Максимальная магнитуда: {max_row['Max_Magnitude']:.2f}")
    
    # Сохранить результаты
    output_path = "results/magnitude_by_type_spark"
    result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"\nРезультаты сохранены в: {output_path}")
    
    # Попробовать сохранить в HDFS
    try:
        hdfs_output = "hdfs://localhost:9000/user/hadoop/output/magnitude_by_type"
        result.coalesce(1).write.mode("overwrite").option("header", "true").csv(hdfs_output)
        print(f"Результаты также сохранены в HDFS: {hdfs_output}")
    except Exception as e:
        print(f"Не удалось сохранить в HDFS: {e}")
    
    spark.stop()

if __name__ == '__main__':
    main()


