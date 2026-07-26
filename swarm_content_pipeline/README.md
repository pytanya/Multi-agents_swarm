# Конвейер генерации контента (Мультиагентная система на Swarm)

**Автономный пайплайн из 4 агентов** для генерации качественных статей по заданной теме с циклом доработок и Human-in-the-loop ревью.

---

## Архитектура

```
Пользователь (тема)
    │
    ▼
┌─────────────────────────────────┐
│  Researcher (прямой API-поиск)   │──► Yandex Search API (основной)
│  _search_multi_source()          │──► DuckDuckGo (дополнение)
│  (детерминированный код, no LLM) │──► Tavily API (резерв)
└────────────┬────────────────────┘
             │ research_data (SharedContext)
             ▼
┌─────────────────┐
│   WriterAgent    │──► Черновик статьи
│  (deepseek-v4)   │    (Swarm / LLM)
└────────┬────────┘
         │ draft
         ▼
┌─────────────────┐
│   EditorAgent    │──► Проверка качества
│  (deepseek-v4)   │    (Swarm / LLM)
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
  APPROVED   REVISION_REQUIRED (< 3 попыток)
    │         │
    ▼         └────► Writer (доработка)
┌─────────────────┐
│  Human Review    │──► approved / revision / rejected
│  (веб-интерфейс) │
└────────┬────────┘
         │ approved
         ▼
┌─────────────────┐
│ PublisherAgent   │──► save_article ──► output/*.md
│  (deepseek-v4)   │    (Swarm / LLM)
└─────────────────┘
    │
    ▼
  Готовая статья (.md)
```

### Модель оркестрации

**«Диктатор с арбитражем»:**
- **Editor → Writer:** арбитраж — Editor решает, нужно ли возвращать на доработку
- **Editor → Publisher:** диктатор — Editor имеет финальное veto-право
- **Лимит доработок:** 3 попытки (защита от зацикливания)
- **Human Review:** финальное решение остаётся за человеком

---

## Архитектурные решения и обоснование

### 1. Поиск: прямые API-вызовы вместо Swarm tool_calling

**Проблема, с которой столкнулись:**
В ходе реализации обнаружено, что модель `deepseek-v4-flash` через RouterAI
**ненадёжно вызывает инструменты** через Swarm function_calling:
LLM тратит ~37 секунд на обдумывание и возвращает ответ без `tool_call`.

```
# Что происходило при Swarm tool_calling:
[11:37:28] [Researcher] Swarm-запрос запущен...
[11:38:05] [WARN] ResearcherAgent не вызвал инструменты. Прямой поиск...
# 37 секунд потрачено впустую
```

**Корень проблемы:**
Swarm function_calling работает надёжно только с нативными OpenAI-моделями
(GPT-4o и выше). Для дешёвых/прокси-моделей вызов tools — best effort:
LLM может проигнорировать инструменты, особенно если считает,
что ответит из параметрических знаний.

**Принятое решение — гибридная архитектура:**

| Тип операции | Инструмент | Обоснование |
|---|---|---|
| Поиск информации | Прямые API-вызовы | Детерминирован, быстрый (~1-2 сек), не зависит от LLM |
| Написание текста | Swarm + WriterAgent | Требует языкового понимания и генерации |
| Рецензия | Swarm + EditorAgent | Требует оценочного суждения LLM |
| Публикация | Swarm + PublisherAgent | Требует форматирования + вызова `save_article` |

**Это соответствует best practice для production агентных систем:**
> *«Детерминированные операции → код; генеративные операции → LLM»*

### 2. Мультиисточниковый поиск (Yandex + DDG)

Вместо «первый успешный» — **комбинирование источников** для полноты:

```
Yandex Search API  ──► основные результаты (РФ-релевантность, ~5 источников)
        │
        ▼ (если < 7 результатов)
DuckDuckGo         ──► дополнительные уникальные источники
        │
        ▼ (если < 3 результатов)
Tavily API         ──► резерв
```

**Дедупликация по URL** — ни один источник не попадёт дважды.
Итог: до 7+ уникальных источников вместо 5 от одного провайдера.

### 3. Dependency Inversion: workflow не знает о pipeline_runner

**Проблема (было):**
```python
# workflow.py — бизнес-логика импортировала UI/инфраструктурный слой
from .pipeline_runner import PipelineLogger  # нарушение DIP
PipelineLogger.log("...", stage="researcher")
```

**Решение (стало) — DI через thread-local callbacks (`utils.py`):**
```
pipeline_runner  →  utils.set_log_callback(fn)
                 →  utils.set_event_callback(fn)
workflow         →  utils.safe_print(...)    # вызывает callback если установлен
tools            →  utils.safe_print(...)    # та же единая реализация
```

Граф зависимостей стал ациклическим:
```
pipeline_runner → utils ← workflow
                        ← tools
```

### 4. Устранение дублирования _safe_print (DRY)

`_safe_print` была скопирована в `workflow.py` и `tools.py` (~80 строк каждый).
Вынесена в единый `utils.py` → один источник правды, одно место для изменений.

---

## Установка

1. Перейдите в директорию проекта:
   ```bash
   cd Multi-agents_swarm
   ```

2. Установите зависимости:
   ```bash
   pip install -r swarm_content_pipeline/requirements.txt
   ```

3. Настройте API-ключ LLM. Создайте файл `.env` в корне проекта:
   ```env
   ROUTERAI_API_KEY=sk-.....
   ROUTERAI_BASE_URL=https://routerai.ru/api/v1
   LLM_MODEL=deepseek/deepseek-v4-flash
   ```

   Провайдер LLM:
   - **RouterAI** (https://routerai.ru) — **основной и единственный рабочий вариант**. Настоятельно рекомендуется.
   - **OpenRouter** (https://openrouter.ai) — формально закодирован как fallback в `config.py`, но **на практике может не работать** (не все модели доступны). Не рассчитывайте на него как на надёжный резерв.
   - **OpenAI** — если указан `OPENAI_API_KEY` (запасной вариант).

4. Настройте Yandex Search API (основной поисковик):
   - Перейдите в Yandex Cloud (https://cloud.yandex.com)
   - Создайте каталог и запишите его Folder ID
   - Создайте API-ключ (сервисный аккаунт с ролью `ai.search`)
   - Добавьте в `.env`:
     ```env
     YANDEX_API_KEY=ваш_api_ключ
     YANDEX_FOLDER_ID=ваш_folder_id
     ```

   > Без Yandex API автоматически используется DuckDuckGo (без ключа).

5. (Опционально) Tavily Search API — резервный поиск:
   - Получите ключ на https://app.tavily.com/
   - Добавьте `TAVILY_API_KEY=...` в `.env`

---

## Запуск

### CLI (аргумент командной строки):
```bash
python swarm_content_pipeline/main.py "Искусственный интеллект в 2026 году"
```

### CLI (интерактивный режим):
```bash
python swarm_content_pipeline/main.py
```

### Web UI (FastAPI + SSE):
```bash
uvicorn swarm_content_pipeline.server:app --reload --port 8000
# Откройте http://localhost:8000
```

---

## Описание агентов

| Агент | Роль | Реализация | Инструменты |
|-------|------|------------|-------------|
| **Researcher** | Сбор фактов | Прямые API-вызовы (без LLM) | `search_yandex`, `search_web`, `search_tavily` |
| **WriterAgent** | Написание черновика | Swarm + deepseek-v4-flash | — |
| **EditorAgent** | Проверка качества (5 критериев) | Swarm + deepseek-v4-flash | — |
| **PublisherAgent** | Markdown-форматирование + сохранение | Swarm + deepseek-v4-flash | `save_article` |

> Модель можно переопределить через `RESEARCHER_MODEL`, `WRITER_MODEL`, `EDITOR_MODEL`, `PUBLISHER_MODEL` или `LLM_MODEL` для всех сразу.

### Критерии проверки EditorAgent:
1. Соответствие теме
2. Логическая структура
3. Отсутствие фактических ошибок
4. Грамматика и стиль
5. Полнота раскрытия

---

## Структура проекта

```
swarm_content_pipeline/
├── __init__.py          # Файл пакета
├── main.py              # Точка входа (CLI)
├── agents.py            # Определение 4 агентов (Swarm Agent)
├── tools.py             # Инструменты: search_web, search_yandex, search_tavily, save_article
├── workflow.py          # Оркестрация пайплайна + SharedContext (память агентов)
├── pipeline_runner.py   # Async runner + SSE-очередь + Human Review блокировка
├── server.py            # FastAPI: REST API + SSE + Web UI
├── config.py            # Конфигурация (лимиты, модели, пути)
├── utils.py             # Общие утилиты: safe_print, log_event, DI-callbacks
└── requirements.txt     # Зависимости
output/                  # Директория для готовых статей
└── .gitkeep
```

---

## Пример выполнения

```bash
python swarm_content_pipeline/main.py "Виды уязвимостей при использовании ИИ агентов"

# Вывод:
# === Конвейер генерации контента (Мультиагентная система) ===
#    Researcher -> Writer -> Editor -> Publisher
#
# [Researcher] Ищу информацию по теме: Виды уязвимостей при использовании ИИ агентов
#    [Researcher] Yandex Search API: +5 источников
#    [Researcher] DuckDuckGo: +4 источников
# [Researcher] Поиск завершён за 1.4 сек
#    Найдено источников: 9
#
# [Writer] Пишу черновик статьи...
#    Черновик готов (5568 символов)
#
# [Editor] Проверка качества (попытка 1/3)...
#    [Editor] Статья одобрена!
#
# [Human Review] Ожидание решения пользователя...
#    [Human Review] Статья одобрена пользователем!
#
# [Publisher] Финализирую и сохраняю статью...
# [tools] Статья сохранена: ...\output\...md
```

---

## Соответствие требованиям ДЗ

| Требование | Реализация |
|-----------|------------|
| **3-5 шагов** | 4 шага: Researcher → Writer → Editor → Publisher |
| **Ветвление/проверка** | Editor: APPROVED → Publisher, REVISION_REQUIRED → Writer (цикл до 3) |
| **Инструменты** | `search_yandex`, `search_web`, `search_tavily`, `save_article` |
| **Память** | `SharedContext` dataclass — единое состояние между агентами |
| **Код + пример** | Пайплайн из `main.py`, результат — `.md` файл |
| **Формат сдачи** | `swarm_content_pipeline/` + README + пример вывода |

---

## Технологии

- **Python 3.10+**
- [OpenAI Swarm](https://github.com/openai/swarm) — фреймворк для мультиагентных систем (Writer, Editor, Publisher)
- [Yandex Search API v2](https://cloud.yandex.com/docs/search-api/) — основной поиск (до 10 000 запросов/мес бесплатно)
- [DuckDuckGo Search](https://pypi.org/project/duckduckgo-search/) — дополнительный поиск без API-ключа
- [Tavily Search API](https://docs.tavily.com/) — резервный поиск
- **deepseek/deepseek-v4-flash** — языковая модель (через RouterAI)
- **RouterAI** (https://routerai.ru) — РФ-дружественный прокси для LLM (рекомендуется)
- **OpenRouter** (https://openrouter.ai) — альтернативный прокси (может работать нестабильно)
- **FastAPI + SSE** — веб-интерфейс с событиями в реальном времени
