# src/analytics/llm_mapper.py
# Имя файла: src/analytics/llm_mapper.py

import os
import json
import re # Импортируем модуль для регулярных выражений
from huggingface_hub import InferenceClient
from dotenv import load_dotenv

load_dotenv()
HF_TOKEN = os.getenv("HF_API_TOKEN")

client = InferenceClient(token=HF_TOKEN)

MODEL_ID = "mistralai/Mistral-7B-Instruct-v0.2"

def generate_messages_for_llm(raw_text: str) -> list[dict]:
    # ... (эта функция остается БЕЗ ИЗМЕНЕНИЙ) ...
    """
    Генерирует структурированный список сообщений для диалоговой модели.
    """
    system_prompt = """
    Ты — элитный ИИ-ассистент для анализа строительных смет.
    Твоя задача — извлечь из сырого текста сметы все позиции (материалы и работы)
    и представить их в виде СТРОГОГО JSON-массива.

    Правила:
    1. Каждая позиция должна быть объектом с полями: "name", "unit", "quantity", "price_per_unit".
    2. Если какое-то поле не найдено, ставь null.
    3. Не выдумывай данные. Если строка не является позицией сметы (например, заголовок раздела), игнорируй ее.
    4. Твой ответ должен быть ТОЛЬКО JSON-массивом, без каких-либо вводных слов или пояснений.
    
    Пример:
    Входной текст: "1. Краска белая - 10 л - 5000 руб. 2. Покраска стен - 20 кв.м. - 15000 руб."
    Твой идеальный ответ:
    [
        {
            "name": "Краска белая",
            "unit": "л",
            "quantity": 10,
            "price_per_unit": 500.0
        },
        {
            "name": "Покраска стен",
            "unit": "кв.м.",
            "quantity": 20,
            "price_per_unit": 750.0
        }
    ]
    """
    user_prompt = f"Вот текст сметы для анализа:\n\n---\n{raw_text}\n---"

    messages = [
        {"role": "user", "content": system_prompt + "\n\n" + user_prompt}
    ]
    return messages

def map_text_to_structured_data(raw_text: str) -> list | None:
    """
    Отправляет сырой текст в LLM через диалоговый endpoint и парсит ответ.
    """
    messages = generate_messages_for_llm(raw_text)
    response_content = "" # Инициализируем переменную
    try:
        print("💬 Отправляю запрос к LLM (метод chat_completion)...")
        response_generator = client.chat_completion(
            messages=messages,
            model=MODEL_ID,
            max_tokens=2048,
            temperature=0.1,
        )
        response_content = response_generator.choices[0].message.content
        print("✅ Ответ от LLM получен.")
        
        # --- УЛУЧШЕННЫЙ ПАРСИНГ JSON ---
        # Ищем самый большой JSON-массив или объект в тексте
        json_match = re.search(r'\[.*\]|\{.*\}', response_content, re.DOTALL)
        
        if not json_match:
            print("❌ Ошибка: В ответе LLM не найден JSON-массив или объект.")
            print(f"   Полученный ответ: {response_content}")
            return None

        json_string = json_match.group(0)
        structured_data = json.loads(json_string)
        # --- КОНЕЦ УЛУЧШЕНИЯ ---
        
        return structured_data
        
    except json.JSONDecodeError:
        print("❌ Ошибка: LLM вернула невалидный JSON.")
        print(f"   Полученный ответ: {response_content}")
        return None
    except Exception as e:
        print(f"❌ Ошибка при обращении к LLM API: {e}")
        return None

# Конец файла: src/analytics/llm_mapper.py