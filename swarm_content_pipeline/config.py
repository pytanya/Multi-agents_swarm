"""
Конфигурация мультиагентной системы «Конвейер генерации контента».

Поддерживает российские OpenAI-совместимые прокси:
- RouterAI (https://routerai.ru/api/v1) — приоритет
- OpenRouter (https://openrouter.ai/api/v1) — fallback

Настройка через .env файл в корне проекта:
    ROUTERAI_API_KEY=sk-...
    ROUTERAI_BASE_URL=https://routerai.ru/api/v1
    OPENROUTER_API_KEY=sk-or-...
    OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
    LLM_MODEL=deepseek/deepseek-v4-flash
    BRAVE_API_KEY=...
"""

import os
from dotenv import load_dotenv

# Загружаем .env из корня проекта
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "output")

# Параметры пайплайна
MAX_ATTEMPTS = 3
MAX_HUMAN_ATTEMPTS = 3
RESEARCH_RESULTS_COUNT = 7
SEARCH_FALLBACK_COUNT = 5  # число результатов для fallback-поиска

# --- Провайдер LLM (РФ-дружественный) ---
# Приоритет: ROUTERAI > OPENROUTER > стандартный OPENAI
LLM_API_KEY = (
    os.getenv("ROUTERAI_API_KEY")
    or os.getenv("OPENROUTER_API_KEY")
    or os.getenv("OPENAI_API_KEY")
    or ""
)

LLM_BASE_URL = (
    os.getenv("ROUTERAI_BASE_URL")
    or os.getenv("OPENROUTER_BASE_URL")
    or "https://routerai.ru/api/v1"
)

# Модель для всех агентов (можно переопределить через LLM_MODEL)
# По умолчанию используем deepseek-v4-flash через RouterAI/OpenRouter
DEFAULT_MODEL = os.getenv("LLM_MODEL", "deepseek/deepseek-v4-flash")

# Можно переопределить модели для каждого агента отдельно через переменные:
# RESEARCHER_MODEL, WRITER_MODEL, EDITOR_MODEL, PUBLISHER_MODEL
RESEARCHER_MODEL = os.getenv("RESEARCHER_MODEL", DEFAULT_MODEL)
WRITER_MODEL = os.getenv("WRITER_MODEL", DEFAULT_MODEL)
EDITOR_MODEL = os.getenv("EDITOR_MODEL", DEFAULT_MODEL)
PUBLISHER_MODEL = os.getenv("PUBLISHER_MODEL", DEFAULT_MODEL)

# --- Основной поиск: Yandex Search API (Yandex Cloud) ---
# Нужен API-ключ с ролью ai.search в каталоге Yandex Cloud
# Документация: https://cloud.yandex.com/docs/search-api/
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY", "")
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID", "")

# --- Таймауты ---
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "120"))  # таймаут для client.run(), сек

# --- Fallback-поиск: Tavily Search API ---
# Документация: https://docs.tavily.com/
# Нужен API-ключ с сайта https://app.tavily.com/
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
