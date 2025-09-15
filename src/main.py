# src/main.py
# Имя файла: src/main.py

import sys
import os
import io
import json
from pathlib import Path
from dotenv import load_dotenv
from googleapiclient.http import MediaIoBaseDownload

# --- Настройка путей и импортов ---
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.utils.google_clients import get_google_clients
from src.parsers.pdf_parser import extract_text_from_pdf
from src.analytics.llm_mapper import map_text_to_structured_data

# --- Загрузка переменных окружения ---
load_dotenv()
SHEET_NAME = os.getenv("SHEET_NAME")
SHEET_WORKSHEET_NAME = os.getenv("SHEET_WORKSHEET_NAME")

# --- Вспомогательные функции (без изменений) ---
def download_file_from_drive(drive_service: "Resource", file_id: str, save_path: Path) -> bool:
    # ... (код без изменений) ...
    try:
        print(f"⬇️  Начинаю скачивание файла с ID: {file_id}")
        request = drive_service.files().get_media(fileId=file_id)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        fh = io.FileIO(save_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        print(f"✅ Файл успешно скачан и сохранен в: {save_path}")
        return True
    except Exception as e:
        print(f"❌ Ошибка при скачивании файла: {e}")
        return False

def update_task_status(worksheet: "gspread.Worksheet", row_index: int, new_status: str):
    # ... (код без изменений) ...
    try:
        status_col = worksheet.find('Status').col
        worksheet.update_cell(row_index, status_col, new_status)
        print(f"🔄 Статус в строке {row_index} обновлен на '{new_status}'")
    except Exception as e:
        print(f"❌ Ошибка при обновлении статуса: {e}")

# --- ИЗМЕНЕННАЯ ФУНКЦИЯ ПОИСКА ---
def find_next_task_by_status(worksheet: "gspread.Worksheet", status: str) -> dict | None:
    """
    Находит первую строку с ЗАДАННЫМ статусом.
    """
    print(f"🔎 Поиск задачи со статусом '{status}'...")
    all_records = worksheet.get_all_records()
    for index, row in enumerate(all_records):
        if row.get("Status") == status:
            row['row_index'] = index + 2
            print(f"  > Найдена задача в строке {row['row_index']}")
            return row
    return None

# --- НОВЫЕ ФУНКЦИИ-ОБРАБОТЧИКИ ДЛЯ КАЖДОГО ЭТАПА ---

def handle_text_extraction(task: dict, worksheet: "gspread.Worksheet", drive_service: "Resource"):
    """Обработчик для Этапа 2: Скачивание и извлечение текста."""
    print("\n--- 🟢 Этап 2: Извлечение Текста ---")
    row_idx = task['row_index']
    update_task_status(worksheet, row_idx, 'processing_text')
    
    file_id = task.get('FileID')
    file_name = task.get('FileName', f"{file_id}.pdf")
    local_pdf_path = project_root / 'data' / 'artifacts' / file_name
    
    if not download_file_from_drive(drive_service, file_id, local_pdf_path):
        update_task_status(worksheet, row_idx, 'error_download_failed')
        return

    extracted_text = extract_text_from_pdf(local_pdf_path)
    artifact_path = local_pdf_path.with_suffix('.json')
    artifact_data = {"case_id": file_id, "source_file": file_name, "raw_text": extracted_text}
    
    with open(artifact_path, 'w', encoding='utf-8') as f:
        json.dump(artifact_data, f, ensure_ascii=False, indent=4)
    print(f"📝 Артефакт с текстом сохранен: {artifact_path}")
    
    update_task_status(worksheet, row_idx, 'text_extracted')

def handle_llm_mapping(task: dict, worksheet: "gspread.Worksheet"):
    """Обработчик для Этапа 3: LLM маппинг."""
    print("\n--- 🔵 Этап 3: LLM Маппинг ---")
    row_idx = task['row_index']
    update_task_status(worksheet, row_idx, 'processing_llm')

    file_name = task.get('FileName')
    artifact_path = project_root / 'data' / 'artifacts' / Path(file_name).with_suffix('.json')

    if not artifact_path.exists():
        update_task_status(worksheet, row_idx, 'error_artifact_not_found')
        return

    with open(artifact_path, 'r', encoding='utf-8') as f:
        artifact_data = json.load(f)
    
    structured_items = map_text_to_structured_data(artifact_data.get("raw_text"))
    
    if structured_items is not None:
        # TODO: Сохранить structured_items в новый артефакт
        print("--- Структурированные данные от LLM ---")
        print(json.dumps(structured_items, indent=2, ensure_ascii=False))
        update_task_status(worksheet, row_idx, 'llm_mapped_successfully')
    else:
        update_task_status(worksheet, row_idx, 'error_llm_mapping_failed')

# --- ГЛАВНЫЙ ОРКЕСТРАТОР ---
def main():
    print("🚀 Запуск AuditLite Python Worker...")
    try:
        gs_client, drive_service = get_google_clients()
        spreadsheet = gs_client.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(SHEET_WORKSHEET_NAME)
        
        # --- НОВАЯ ЛОГИКА КОНВЕЙЕРА ---
        # Сначала ищем задачи для Этапа 2
        task_for_extraction = find_next_task_by_status(worksheet, 'new')
        if task_for_extraction:
            handle_text_extraction(task_for_extraction, worksheet, drive_service)
            print("🏁 Работа на итерации завершена (выполнен Этап 2).")
            return # Выходим, чтобы при следующем запуске начать сначала

        # Если для Этапа 2 задач нет, ищем для Этапа 3
        task_for_mapping = find_next_task_by_status(worksheet, 'text_extracted')
        if task_for_mapping:
            handle_llm_mapping(task_for_mapping, worksheet)
            print("🏁 Работа на итерации завершена (выполнен Этап 3).")
            return

        print("✅ Нет новых задач ни для одного из этапов. Все в порядке.")

    except Exception as e:
        print(f"❌ Произошла критическая ошибка: {e}")

if __name__ == "__main__":
    main()

# Конец файла: src/main.py