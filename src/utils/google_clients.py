# src/utils/google_clients.py
# Имя файла: src/utils/google_clients.py

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build, Resource
from pathlib import Path

# Определяем "области" (permissions), к которым наш скрипт будет иметь доступ.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
# Строим абсолютный путь к файлу ключа от корня проекта
project_root = Path(__file__).resolve().parent.parent.parent
SERVICE_ACCOUNT_FILE = project_root / 'google-credentials.json'
# --- КОНЕЦ ИЗМЕНЕНИЯ ---

def get_google_clients() -> tuple[gspread.Client, Resource]:
    """
    Аутентифицируется с помощью сервисного аккаунта и возвращает
    клиенты для работы с Google Sheets (gspread) и Google Drive API.

    Returns:
        Кортеж, содержащий (gspread_client, drive_service).
    """
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    
    # Клиент для Google Sheets
    gspread_client = gspread.authorize(creds)
    
    # Клиент для Google Drive
    drive_service = build('drive', 'v3', credentials=creds)
    
    return gspread_client, drive_service

# Конец файла: src/utils/google_clients.py