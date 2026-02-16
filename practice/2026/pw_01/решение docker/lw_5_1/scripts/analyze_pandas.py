#!/usr/bin/env python3
"""
Анализ данных землетрясений с использованием Pandas
Задача: найти тип землетрясения с максимальной средней магнитудой
"""
import pandas as pd
import sys
import os

def load_data(filepath):
    """Загрузить данные из CSV файла"""
    try:
        df = pd.read_csv(filepath, low_memory=False)
        print(f"Загружено строк: {len(df)}")
        return df
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df)}")
    
    # Удалить строки без магнитуды
    df = df[df['Magnitude'].notna()]
    
    # Заполнить пустые значения в Type
    df['Type'] = df['Type'].fillna('Unknown')
    
    print(f"Количество строк после очистки: {len(df)}")
    print(f"Уникальных типов: {df['Type'].nunique()}")
    
    return df

def analyze_magnitude_by_type(df):
    """Анализ средней магнитуды по типам"""
    print("\n=== Анализ средней магнитуды по типам ===")
    
    # Группировка по типу и вычисление средней магнитуды
    result = df.groupby('Type')['Magnitude'].agg(['mean', 'count', 'std', 'min', 'max']).reset_index()
    result.columns = ['Type', 'Mean_Magnitude', 'Count', 'Std', 'Min_Magnitude', 'Max_Magnitude']
    
    # Сортировка по средней магнитуде
    result = result.sort_values('Mean_Magnitude', ascending=False)
    
    return result

def find_max_mean_type(df):
    """Найти тип с максимальной средней магнитудой"""
    result = analyze_magnitude_by_type(df)
    
    print("\n=== Результаты ===")
    print("\nТипы землетрясений по средней магнитуде (топ-10):")
    print(result.head(10).to_string(index=False))
    
    max_type = result.iloc[0]
    print(f"\nТип землетрясения с максимальной средней магнитудой: '{max_type['Type']}'")
    print(f"Средняя магнитуда: {max_type['Mean_Magnitude']:.2f}")
    print(f"Количество землетрясений этого типа: {int(max_type['Count'])}")
    print(f"Минимальная магнитуда: {max_type['Min_Magnitude']:.2f}")
    print(f"Максимальная магнитуда: {max_type['Max_Magnitude']:.2f}")
    
    return result

def main():
    # Путь к файлу данных
    data_file = '/opt/data/database.csv'
    
    if not os.path.exists(data_file):
        # Попробовать локальный путь
        data_file = 'database.csv'
    
    if not os.path.exists(data_file):
        print(f"Файл не найден: {data_file}")
        sys.exit(1)
    
    print("=== Анализ данных землетрясений ===")
    print(f"Файл: {data_file}")
    
    # Загрузка данных
    df = load_data(data_file)
    
    # Показать базовую информацию
    print("\n=== Информация о данных ===")
    print(df.info())
    print("\nПервые 5 строк:")
    print(df.head())
    
    # Очистка данных
    df_clean = clean_data(df)
    
    # Анализ
    result = find_max_mean_type(df_clean)
    
    # Сохранить результаты
    output_file = 'results/magnitude_by_type.csv'
    os.makedirs('results', exist_ok=True)
    result.to_csv(output_file, index=False)
    print(f"\nРезультаты сохранены в: {output_file}")
    
    return result

if __name__ == '__main__':
    main()


