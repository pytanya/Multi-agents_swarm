"""
Инструменты для мультиагентной системы «Конвейер генерации контента».

Содержит:
- search_web: поиск информации через DuckDuckGo (без API-ключа)
- search_yandex: основной поиск через Yandex Search API v2 (нужен API-ключ)
- search_tavily: fallback-поиск через Tavily Search API (нужен API-ключ)
- save_article: сохранение статьи в Markdown-файл
"""

import base64
import datetime
import json
import os
import re
import warnings
from typing import Any

import requests

# Подавляем RuntimeWarning о переименовании duckduckgo_search -> ddgs
warnings.filterwarnings("ignore", message=".*ddgs.*")

try:
    from ddgs import DDGS
except ImportError:
    from duckduckgo_search import DDGS

from bs4 import BeautifulSoup

from . import config
from .utils import safe_print  # единая реализация (DRY, без дублирования из workflow.py)



def search_web(query: str) -> str:
    """
    Поиск информации через DuckDuckGo.

    Не требует API-ключа. Возвращает JSON-строку с массивом результатов.
    В случае недоступности DuckDuckGo возвращает пустой JSON-массив.

    Args:
        query: Поисковый запрос.

    Returns:
        JSON-строка со списком словарей с ключами title, snippet, link.
    """
    try:
        with DDGS(timeout=5) as ddgs:
            results = list(
                ddgs.text(query, max_results=config.RESEARCH_RESULTS_COUNT)
            )
        return json.dumps(
            [
                {"title": r.get("title", ""),
                 "snippet": r.get("body", ""),
                 "link": r.get("href", "")}
                for r in results
            ],
            ensure_ascii=False,
        )
    except Exception as e:
        safe_print(f"[tools] Ошибка при поиске в DuckDuckGo: {e}")
        return "[]"


def _clean_hlword(text: str) -> str:
    """Удаляет теги <hlword> из текста, возвращая чистую строку."""
    return re.sub(r'</?hlword[^>]*>', '', text).strip()


def search_tavily(query: str) -> str:
    """
    Поиск информации через Tavily Search API.

    Требуется TAVILY_API_KEY в .env.
    Документация: https://docs.tavily.com/
    Используется как fallback, если Yandex Search не дал результатов.

    Args:
        query: Поисковый запрос.

    Returns:
        JSON-строка со списком словарей с ключами title, snippet, link.
        Если API-ключ не настроен или произошла ошибка — возвращает "[]".
    """
    if not config.TAVILY_API_KEY:
        safe_print("[tools] TAVILY_API_KEY не настроен. Tavily-поиск недоступен.")
        return "[]"

    try:
        url = "https://api.tavily.com/search"
        headers = {
            "Content-Type": "application/json",
        }
        payload = {
            "api_key": config.TAVILY_API_KEY,
            "query": query,
            "search_depth": "basic",
            "max_results": config.SEARCH_FALLBACK_COUNT,
            "include_answer": False,
            "include_raw_content": False,
        }
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # Tavily возвращает результаты в data["results"]
        results = data.get("results", [])
        if not results:
            safe_print("[tools] Tavily Search не вернул результатов.")
            return "[]"

        safe_print(f"[tools] Tavily Search: получено {len(results)} результатов")
        return json.dumps(
            [
                {
                    "title": r.get("title", ""),
                    "snippet": r.get("content", "") or r.get("snippet", ""),
                    "link": r.get("url", ""),
                }
                for r in results
            ],
            ensure_ascii=False,
        )

    except Exception as e:
        safe_print(f"[tools] Ошибка при поиске через Tavily Search: {e}")
        return "[]"


def search_yandex(query: str) -> str:
    """
    Поиск информации через Yandex Search API v2 (Yandex Cloud).

    Требуются YANDEX_API_KEY и YANDEX_FOLDER_ID в .env.
    Работает в РФ, бесплатный лимит: 10000 запросов/мес.
    Документация: https://cloud.yandex.com/docs/search-api/v2/

    Использует FORMAT_XML, возвращающий структурированные данные
    с титулами, URL и пассажами (сниппетами).

    Args:
        query: Поисковый запрос.

    Returns:
        JSON-строка со списком словарей с ключами title, snippet, link.
        Если API-ключ не настроен или произошла ошибка — возвращает "[]".
    """
    if not config.YANDEX_API_KEY or not config.YANDEX_FOLDER_ID:
        safe_print("[tools] YANDEX_API_KEY или YANDEX_FOLDER_ID не настроены. Fallback-поиск недоступен.")
        return "[]"

    try:
        url = "https://searchapi.api.cloud.yandex.net/v2/web/search"
        headers = {
            "Authorization": f"Api-Key {config.YANDEX_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "query": {
                "searchType": "SEARCH_TYPE_RU",
                "queryText": query,
            },
            "folderId": config.YANDEX_FOLDER_ID,
            "responseFormat": "FORMAT_XML",
        }
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # Ответ: {"rawData": "<Base64-encoded XML>"}
        raw_b64 = data.get("rawData", "")
        if not raw_b64:
            safe_print("[tools] Yandex Search вернул пустой rawData.")
            return "[]"

        xml_bytes = base64.b64decode(raw_b64)
        xml_text = xml_bytes.decode("utf-8", errors="replace")

        # Парсим XML через BeautifulSoup с lxml-xml парсером
        soup = BeautifulSoup(xml_text, "lxml-xml")
        docs = soup.find_all("doc")

        if not docs:
            safe_print("[tools] Yandex Search не вернул результатов (0 doc).")
            return "[]"

        # Лимитируем количество результатов
        max_results = config.SEARCH_FALLBACK_COUNT
        results = []
        for doc in docs[:max_results]:
            url_el = doc.find("url")
            title_el = doc.find("title")
            passage_el = doc.find("passage")
            lang_el = doc.find("lang")

            link = url_el.text.strip() if url_el and url_el.text else ""
            title = _clean_hlword(title_el.text) if title_el and title_el.text else ""
            snippet = _clean_hlword(passage_el.text) if passage_el and passage_el.text else ""
            lang = lang_el.text if lang_el and lang_el.text else ""

            if not link:
                continue

            results.append({
                "title": title,
                "snippet": snippet,
                "link": link,
                "lang": lang,
            })

        if not results:
            safe_print("[tools] Yandex Search не вернул валидных результатов после парсинга.")
            return "[]"

        safe_print(f"[tools] Yandex Search: получено {len(results)} результатов")
        return json.dumps(results, ensure_ascii=False)

    except Exception as e:
        safe_print(f"[tools] Ошибка при поиске через Yandex Search: {e}")
        return "[]"


def _sanitize_content(content: str) -> str:
    """Заменяет эмодзи и проблемные символы на ASCII-аналоги."""
    content = content.replace("\u2705", "[OK]")
    content = content.replace("\u274c", "[NO]")
    content = content.replace("\u26a0\ufe0f", "[WARN]")
    content = content.replace("\u26a0", "[WARN]")
    content = content.replace("\U0001f4a1", "[i]")
    content = content.replace("\U0001f4dd", "[i]")
    content = content.replace("\u2764", "(heart)")
    # Обобщённый — заменяем любые остальные эмодзи
    import re as _re
    # Удаляем surrogate pairs и другие проблемные символы
    content = _re.sub(r'[\U0001f300-\U0001ffff]', '', content)
    return content


def save_article(content: str, topic: str) -> str:
    """
    Сохраняет текст статьи как .md файл в директорию output/.

    Args:
        content: Текст статьи в Markdown-формате.
        topic: Тема статьи (используется для формирования имени файла).

    Returns:
        Полный путь к сохранённому файлу.
    """
    safe_topic = topic.replace(" ", "_")[:30]
    safe_topic = "".join(c for c in safe_topic if c.isalnum() or c in ("_", "-")).rstrip("._-")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_topic}_{timestamp}.md"

    output_dir = os.path.abspath(config.OUTPUT_DIR)
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, filename)
    # Заменяем проблемные Unicode-символы перед записью
    safe_content = _sanitize_content(content)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(safe_content)

    safe_print(f"[tools] Статья сохранена: {filepath}")
    return filepath
