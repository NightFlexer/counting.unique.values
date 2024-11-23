import os
import gzip
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = 8

directory = '/home/user/data/'

def process_file(file_path):
    try:
        unique_numbers = set()
        total_lines = 0
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and re.match(r'^\d{11};', line):
                    number = line[:11]
                    unique_numbers.add(number)
                    total_lines += 1
        return total_lines, unique_numbers
    except Exception as e:
        print(f"Ошибка при обработке файла {file_path}: {e}")
        return 0, set()

def main():
    total_records = 0
    all_unique_numbers = set()
    file_statistics = {}

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_file, os.path.join(directory, file)): file for file in os.listdir(directory) if file.endswith('.gz')}
        for future in futures:
            total_lines, unique_numbers = future.result()
            file_name = futures[future]
            total_records += total_lines
            all_unique_numbers.update(unique_numbers)

            file_statistics[file_name] = {
                'total_records': total_lines,
                'unique_records': len(unique_numbers),
                'unique_percentage': (len(unique_numbers) / total_lines * 100) if total_lines > 0 else 0
            }

    # Уникальные записи
    unique_count = len(all_unique_numbers)

    print(f"Общее количество записей: {total_records}")
    print(f"Количество уникальных записей: {unique_count}")
    unique_percentage = (unique_count / total_records * 100) if total_records > 0 else 0
    print(f"Процент уникальных записей по отношению к общему количеству записей: {unique_percentage:.2f}%")

    for file_name, stats in file_statistics.items():
        print(f"Файл: {file_name} - содержит: общее количество записей: {stats['total_records']}, уникальных записей: {stats['unique_records']}, процент уникальных записей: {stats['unique_percentage']:.2f}%")

if __name__ == "__main__":
    main()