# src/main.py
# Имя файла: src/main.py

import sys
import os
import io
from pathlib import Path
from dotenv import load_dotenv
from googleapiclient.http import MediaIoBaseDownload

# --- Настройка путей и импортов ---
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.utils.google_clients import get_google_clients
# Импортируем наш парсер, он скоро понадобится
from src.parsers.pdf_parser import extract_text_from_pdf

# --- Загрузка переменных окружения ---
load_dotenv()
SHEET_NAME = os.getenv("SHEET_NAME")
SHEET_WORKSHEET_NAME = os.getenv("SHEET_WORKSHEET_NAME")

# --- Новые функции ---

def download_file_from_drive(drive_service: "Resource", file_id: str, save_path: Path) -> bool:
    """
    Скачивает файл из Google Drive по его ID и сохраняет локально.
    """
    try:
        print(f"⬇️  Начинаю скачивание файла с ID: {file_id}")
        request = drive_service.files().get_media(fileId=file_id)
        
        # Убедимся, что директория для сохранения существует
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        fh = io.FileIO(save_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"  > Прогресс скачивания: {int(status.progress() * 100)}%")
            
        print(f"✅ Файл успешно скачан и сохранен в: {save_path}")
        return True
    except Exception as e:
        print(f"❌ Ошибка при скачивании файла: {e}")
        return False

def update_task_status(worksheet: "gspread.Worksheet", row_index: int, new_status: str):
    """
    Обновляет статус задачи в указанной строке.
    """
    try:
        # Находим нужную колонку по имени 'Status'
        status_col = worksheet.find('Status').col
        worksheet.update_cell(row_index, status_col, new_status)
        print(f"🔄 Статус в строке {row_index} обновлен на '{new_status}'")
    except Exception as e:
        print(f"❌ Ошибка при обновлении статуса: {e}")

def find_next_task(worksheet: "gspread.Worksheet") -> dict | None:
    """
    Находит первую строку со статусом 'new' и возвращает ее как словарь.
    """
    all_records = worksheet.get_all_records()
    for index, row in enumerate(all_records):
        if row.get("Status") == 'new':
            row['row_index'] = index + 2
            print(f"✅ Найдена новая задача в строке {row['row_index']}: {row}")
            return row
    print("ℹ️ Новых задач со статусом 'new' не найдено.")
    return None

def main():
    """
    Главная функция-оркестратор.
    """
    print("🚀 Запуск AuditLite Python Worker...")
    
    try:
        gs_client, drive_service = get_google_clients()
        spreadsheet = gs_client.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(SHEET_WORKSHEET_NAME)
        
        task = find_next_task(worksheet)
        
        if not task:
            print("🏁 Работа завершена.")
            return

        # --- ОБНОВЛЕННАЯ ЛОГИКА ---
        
        row_idx = task['row_index']
        file_id = task.get('FileID')
        
        if not file_id:
            print(f"❌ В задаче (строка {row_idx}) отсутствует FileID. Пропускаю.")
            update_task_status(worksheet, row_idx, 'error_no_file_id')
            return

        # 1. Обновляем статус, чтобы другие обработчики не взяли эту задачу
        update_task_status(worksheet, row_idx, 'processing')

        # 2. Скачиваем файл
        file_name = task.get('FileName', f"{file_id}.pdf") # Используем имя файла или ID
        local_pdf_path = project_root / 'data' / 'artifacts' / file_name
        
        download_successful = download_file_from_drive(drive_service, file_id, local_pdf_path)

        if not download_successful:
            update_task_status(worksheet, row_idx, 'error_download_failed')
            return
            
        # 3. TODO: Вызвать парсер (следующий шаг)
        print(f"下一步: Вызвать парсер для файла {local_pdf_path}")
        
        # 4. Финальное обновление статуса
        update_task_status(worksheet, row_idx, 'processed_text_extracted') # Временный статус
        
    except Exception as e:
        print(f"❌ Произошла критическая ошибка: {e}")

if __name__ == "__main__":
    main()

# Конец файла: src/main.py