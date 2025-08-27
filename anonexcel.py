import pandas as pd
import numpy as np
import uuid
import re
from collections import defaultdict
import random
import string
import json
import os

class DataAnonymizer:
    def __init__(self, config_file=None):
        self.mapping_dicts = defaultdict(dict)
        self.column_types = {}
        self.dictionary_columns = {}  # Для ручной настройки словарных колонок
        self.config_file = config_file
        self.float_precision = {}  # Для хранения точности чисел с плавающей точкой
        
        # Загружаем конфигурацию, если файл существует
        if config_file and os.path.exists(config_file):
            self.load_config()
    
    def load_config(self):
        """Загружает конфигурацию из JSON файла"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.column_types = config.get('column_types', {})
                self.dictionary_columns = config.get('dictionary_columns', {})
                print(f"Загружена конфигурация из {self.config_file}")
        except Exception as e:
            print(f"Ошибка загрузки конфигурации: {e}")
    
    def save_config(self, df, output_file):
        """Сохраняет конфигурацию в JSON файл"""
        config = {
            'column_types': self.column_types,
            'dictionary_columns': self.dictionary_columns,
            'columns': list(df.columns),
            '_comment': {
                'dictionary_columns': 'true - всегда считать словарным, false - никогда не считать словарным, auto - автоматическое определение',
                'column_types': 'Типы данных для обработки: uuid, integer, float, alphanumeric, text, date, unknown'
            }
        }
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            print(f"Конфигурация сохранена в {output_file}")
        except Exception as e:
            print(f"Ошибка сохранения конфигурации: {e}")
    
    def is_dictionary_column(self, column_name, unique_count):
        """Определяет, является ли колонка словарной"""
        # Проверяем ручную настройку из конфигурации
        if column_name in self.dictionary_columns:
            setting = self.dictionary_columns[column_name]
            if setting == 'true':
                return True
            elif setting == 'false':
                return False
            # Для 'auto' продолжаем автоматическое определение
        
        # Автоматическое определение по количеству уникальных значений
        return unique_count < 100
    
    def detect_float_precision(self, value):
        """Определяет количество знаков после запятой для чисел с плавающей точкой"""
        if pd.isna(value) or not isinstance(value, (float, str)):
            return 0
        
        value_str = str(value)
        if '.' in value_str:
            decimal_part = value_str.split('.')[1]
            # Убираем trailing zeros для определения реальной точности
            return len(decimal_part.rstrip('0'))
        return 0
    
    def detect_column_type(self, series, column_name):
        """Определяет тип данных в колонке"""
        # Если тип уже задан в конфигурации, используем его
        if column_name in self.column_types:
            return self.column_types[column_name]
        
        non_null = series.dropna()
        if len(non_null) == 0:
            return 'unknown'
        
        # Проверяем на UUID (без дефисов)
        uuid_pattern = re.compile(r'^[0-9a-f]{32}$', re.I)
        uuid_samples = [str(x) for x in non_null.head(100) if pd.notna(x)]
        if all(uuid_pattern.match(x) for x in uuid_samples if x):
            return 'uuid'
        
        # Проверяем на UUID (с дефисами)
        uuid_pattern_with_dashes = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
        if all(uuid_pattern_with_dashes.match(str(x)) for x in non_null.head(100) if pd.notna(x)):
            return 'uuid'
        
        # Проверяем на даты
        try:
            pd.to_datetime(non_null.head(100), errors='raise')
            return 'date'
        except:
            pass
        
        # Проверяем на целые числа
        if all(isinstance(x, (int, np.integer)) or (isinstance(x, str) and x.isdigit()) for x in non_null.head(100)):
            return 'integer'
        
        # Проверяем на числа с плавающей точкой и определяем точность
        float_samples = non_null.head(100)
        if all(isinstance(x, (float, np.floating)) for x in float_samples):
            # Сохраняем максимальную точность для колонки
            max_precision = max(self.detect_float_precision(x) for x in float_samples if pd.notna(x))
            self.float_precision[column_name] = max_precision
            return 'float'
        
        # Проверяем строки, которые могут быть float
        try:
            float_values = [float(str(x).replace(',', '.')) for x in float_samples if pd.notna(x)]
            # Если все значения можно преобразовать в float
            max_precision = max(self.detect_float_precision(x) for x in float_values)
            self.float_precision[column_name] = max_precision
            return 'float'
        except:
            pass
        
        # Проверяем на алфавитно-цифровые идентификаторы
        alphanum_pattern = re.compile(r'^[A-Za-z]+[0-9]+$')
        if all(alphanum_pattern.match(str(x)) for x in non_null.head(100) if pd.notna(x)):
            return 'alphanumeric'
        
        # Проверяем на названия/текст
        word_pattern = re.compile(r'^[A-Za-z\s\.,&]+$', re.I)
        if any(word_pattern.match(str(x)) for x in non_null.head(100) if pd.notna(x)):
            return 'text'
        
        return 'unknown'
    
    def generate_random_words(self, n_words=3):
        """Генерирует случайные слова"""
        words = ['Company', 'Enterprise', 'Corporation', 'Limited', 'Group', 
                'International', 'Global', 'National', 'Regional', 'Local',
                'Solutions', 'Services', 'Technologies', 'Industries', 'Ventures']
        return ' '.join(random.sample(words, min(n_words, len(words))))
    
    def anonymize_uuid(self, value):
        """Анонимизирует UUID с сохранением маппинга (без дефисов)"""
        if pd.isna(value):
            return value
        
        if value not in self.mapping_dicts['uuid']:
            # Генерируем UUID и убираем дефисы
            new_uuid = str(uuid.uuid4()).replace('-', '')
            self.mapping_dicts['uuid'][value] = new_uuid
        return self.mapping_dicts['uuid'][value]
    
    def anonymize_integer(self, value, length=None):
        """Анонимизирует целое число"""
        if pd.isna(value):
            return value
        
        if length is None:
            length = len(str(int(value)))
        
        # Генерируем случайное число с таким же количеством цифр
        min_val = 10**(length-1)
        max_val = (10**length) - 1
        return str(random.randint(min_val, max_val))  # Возвращаем как строку
    
    def anonymize_float(self, value, column_name):
        """Анонимизирует число с плавающей точкой с сохранением формата"""
        if pd.isna(value):
            return value
        
        # Определяем количество знаков после запятой
        precision = self.float_precision.get(column_name, 0)
        
        if precision > 0:
            # Генерируем случайное число и форматируем с нужной точностью
            random_float = random.uniform(0, 1000000)
            formatted = f"{random_float:.{precision}f}"
            return formatted
        else:
            # Для целых чисел возвращаем как есть
            return str(value)
    
    def anonymize_alphanumeric(self, value):
        """Анонимизирует алфавитно-цифровые идентификаторы"""
        if pd.isna(value):
            return value
        
        value_str = str(value)
        
        # Разделяем буквенную и цифровую части
        letters = ''.join(filter(str.isalpha, value_str))
        numbers = ''.join(filter(str.isdigit, value_str))
        
        if value_str not in self.mapping_dicts['alphanumeric']:
            # Генерируем
