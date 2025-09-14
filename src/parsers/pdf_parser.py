# src/parsers/pdf_parser.py
# Имя файла: src/parsers/pdf_parser.py

import pdfplumber
from pathlib import Path

def extract_text_from_pdf(pdf_path: Path) -> str:
    """
    Извлекает весь текст из указанного PDF-файла.

    Args:
        pdf_path: Путь к PDF-файлу (объект Path).

    Returns:
        Строку, содержащую весь извлеченный текст,
        или сообщение об ошибке.
    """
    if not pdf_path.exists():
        return f"Ошибка: Файл не найден по пути {pdf_path}"

    all_text = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                # Добавляем разделитель, чтобы понимать, где кончается страница
                all_text.append(f"--- СТРАНИЦА {page_num} ---")
                page_text = page.extract_text()
                if page_text:
                    all_text.append(page_text)
        
        return "\n".join(all_text)
    except Exception as e:
        return f"Ошибка при обработке PDF: {e}"

# Конец файла: src/parsers/pdf_parser.py