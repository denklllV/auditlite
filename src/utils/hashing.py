# src/utils/hashing.py
# Имя файла: src/utils/hashing.py

import hashlib

def calculate_md5(file_path: str) -> str:
    """
    Вычисляет MD5 хеш для файла.

    Args:
        file_path: Путь к файлу.

    Returns:
        MD5 хеш в виде hex-строки.
    """
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            # Читаем файл по частям для эффективной работы с большими файлами
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        # Обработка случая, если файл не найден
        return "file_not_found"

# Конец файла: src/utils/hashing.py