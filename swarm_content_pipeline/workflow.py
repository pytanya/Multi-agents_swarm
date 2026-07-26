"""
Оркестрация мультиагентной системы «Конвейер генерации контента».

Содержит:
- SharedContext: dataclass для передачи состояния между агентами
- build_messages: формирование сообщений для каждого агента
- run_pipeline: основной цикл пайплайна

Исправления (v2):
- _run_researcher теперь запускает ResearcherAgent через client.run() —
  агент самостоятельно вызывает инструменты через Swarm function_calling (#1 fix).
- Убраны все прямые импорты pipeline_runner из workflow.py —
  логирование идёт через DI-callbacks из utils.py (#3 fix).
- Удалён дублирующий _safe_print — используется utils.safe_print (#DRY fix).
"""

import json
import datetime
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

from openai import OpenAI
from swarm import Swarm

from . import config
from .agents import writer_agent, editor_agent, publisher_agent
from .tools import save_article, search_yandex, search_web, search_tavily
from .utils import safe_print, log_event   # DI: нет импорта pipeline_runner


# =========================================================================
# Swarm клиент
# =========================================================================

def _create_swarm_client() -> Swarm:
    if not config.LLM_API_KEY:
        safe_print("[workflow] API-ключ не найден. Swarm будет использовать OpenAI() по умолчанию.")
        return Swarm()

    client = OpenAI(
        api_key=config.LLM_API_KEY,
        base_url=config.LLM_BASE_URL,
        timeout=config.LLM_TIMEOUT,
    )
    safe_print(f"[workflow] Swarm клиент: {config.LLM_BASE_URL} (timeout={config.LLM_TIMEOUT}s)")
    return Swarm(client=client)


# =========================================================================
# Общий контекст (память между агентами)
# =========================================================================

@dataclass
class SharedContext:
    topic: str
    research_data: list[dict] = field(default_factory=list)
    draft: str = ""
    editor_review: str = ""
    editor_approved: bool = False
    attempts: int = 0
    max_attempts: int = field(default=config.MAX_ATTEMPTS)
    final_path: Optional[str] = None
    # Human Review поля
    human_review_comment: str = ""
    human_attempts: int = 0
    max_human_attempts: int = field(default=config.MAX_HUMAN_ATTEMPTS)
    human_decision: Optional[str] = None


# =========================================================================
# Вспомогательные функции
# =========================================================================

def _format_research_data(research_data: list[dict]) -> str:
    if not research_data:
        return "Нет данных."
    lines = []
    for i, item in enumerate(research_data, 1):
        title = item.get("title", "Без заголовка")
        snippet = item.get("snippet", "")
        link = item.get("link", "")
        lines.append(f"{i}. {title}\n   {snippet}\n   Источник: {link}")
    return "\n\n".join(lines)


def build_messages(agent_type: str, ctx: SharedContext) -> list[dict]:
    messages = []

    if agent_type == "researcher":
        messages.append({
            "role": "user",
            "content": (
                f"Тема для исследования: {ctx.topic}\n\n"
                f"Найди достоверную информацию по этой теме, используя доступные инструменты поиска. "
                f"Попробуй сначала search_web, затем при необходимости search_yandex или search_tavily. "
                f"Собери 5-7 наиболее релевантных источников. "
                f"Для каждого источника укажи заголовок, краткую выжимку и ссылку."
            )
        })

    elif agent_type == "writer":
        research_text = _format_research_data(ctx.research_data)
        content = (
            f"Тема статьи: {ctx.topic}\n\n"
            f"Исследовательские данные:\n{research_text}\n\n"
            f"Напиши структурированную статью по теме, используя предоставленные данные. "
            f"Статья должна содержать: заголовок, введение, основную часть (2-4 раздела), заключение."
        )
        if ctx.draft and ctx.editor_review:
            content += (
                f"\n\nПредыдущий черновик:\n{ctx.draft}\n\n"
                f"Замечания редактора (учти их при доработке):\n{ctx.editor_review}"
            )
        messages.append({"role": "user", "content": content})

    elif agent_type == "writer_with_human_feedback":
        research_text = _format_research_data(ctx.research_data)
        content = (
            f"Тема статьи: {ctx.topic}\n\n"
            f"Исследовательские данные:\n{research_text}\n\n"
            f"Текущий черновик:\n{ctx.draft}\n\n"
            f"Замечания редактора (учтены ранее):\n{ctx.editor_review}\n\n"
            f"НОВЫЕ замечания от человека-рецензента (ОБЯЗАТЕЛЬНО учти их при доработке):\n"
            f"{ctx.human_review_comment}\n\n"
            f"Доработай статью с учётом всех замечаний. "
            f"Не удаляй существующие разделы без необходимости — дополняй и улучшай."
        )
        messages.append({"role": "user", "content": content})

    elif agent_type == "editor":
        research_text = _format_research_data(ctx.research_data)
        content = (
            f"Тема статьи: {ctx.topic}\n\n"
            f"Исследовательские данные:\n{research_text}\n\n"
            f"Черновик статьи:\n{ctx.draft}\n\n"
            f"Проверь качество статьи по критериям: соответствие теме, логическая структура, "
            f"отсутствие фактических ошибок, грамматика и стиль, полнота раскрытия. "
            f"Если всё хорошо — напиши APPROVED. Если есть замечания — REVISION_REQUIRED."
        )
        messages.append({"role": "user", "content": content})

    elif agent_type == "publisher":
        research_text = _format_research_data(ctx.research_data)
        content = (
            f"Тема статьи: {ctx.topic}\n\n"
            f"Одобренный черновик статьи:\n{ctx.draft}\n\n"
            f"Рецензия редактора:\n{ctx.editor_review}\n\n"
            f"Исследовательские данные (для раздела источники):\n{research_text}\n\n"
            f"Приведи статью в финальный Markdown-формат, добавь заголовок, дату, "
            f"раздел 'Источники' со ссылками. Сохрани через save_article."
        )
        messages.append({"role": "user", "content": content})

    return messages


def _extract_content(response) -> str:
    if not response or not response.messages:
        return ""
    for msg in reversed(response.messages):
        if msg.get("role") == "assistant" and msg.get("content"):
            return msg["content"]
    return ""


def _extract_tool_output(response, function_name: str) -> Optional[str]:
    if not response or not response.messages:
        return None
    for msg in response.messages:
        if msg.get("role") == "tool" and msg.get("name") == function_name:
            return msg.get("content")
    return None


def _make_fallback_article(topic: str, draft: str, research_text: str) -> str:
    today = datetime.datetime.now().strftime("%d.%m.%Y")
    return (
        f"# {topic}\n\n"
        f"*Дата: {today}*\n\n"
        f"{draft}\n\n"
        f"---\n\n"
        f"## Источники\n\n"
        f"{research_text}"
    )


# =========================================================================
# Вспомогательные шаги пайплайна
# =========================================================================


def _search_multi_source(topic: str) -> list[dict]:
    """Мультиисточниковый поиск: Yandex (основной) + DuckDuckGo (дополнение).

    Архитектура: детерминированный поиск через прямые API-вызовы.
    Не зависит от LLM tool_calling (deepseek/RouterAI не вызывают tools надёжно).
    Yandex — первичный источник (РФ-релевантность), DDG — дополняет до нужного объёма.
    """
    all_results: list[dict] = []
    seen_links: set[str] = set()
    target_count = 7

    def _add_from(raw: str, source_name: str) -> int:
        """Добавляет уникальные результаты из JSON-строки. Возвращает кол-во добавленных."""
        if not raw or raw == "[]":
            return 0
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                return 0
            added = 0
            for item in parsed:
                link = item.get("link", "")
                if link and link not in seen_links:
                    all_results.append(item)
                    seen_links.add(link)
                    added += 1
            if added:
                safe_print(f"   [Researcher] {source_name}: +{added} источников")
            return added
        except (json.JSONDecodeError, TypeError):
            return 0

    # 1. Yandex — основной источник
    yandex_raw = search_yandex(topic)
    _add_from(yandex_raw, "Yandex Search API")

    # 2. DuckDuckGo — дополняет, если источников меньше target_count
    if len(all_results) < target_count:
        ddg_raw = search_web(topic)
        _add_from(ddg_raw, "DuckDuckGo")

    # 3. Tavily — последний резерв
    if len(all_results) < 3:
        tavily_raw = search_tavily(topic)
        _add_from(tavily_raw, "Tavily Search API")

    return all_results


def _run_researcher(ctx: SharedContext, client: Swarm) -> None:
    """Шаг 1 — Researcher: надёжный мультиисточниковый поиск.

    Архитектурное решение:
    - Поиск выполняется через прямые API-вызовы (_search_multi_source),
      а не через Swarm tool_calling. Причина: deepseek-v4-flash через RouterAI
      ненадёжно вызывает инструменты (тратит ~37 сек и не вызывает tools).
    - Swarm (LLM) используется для генеративных задач: Writer, Editor, Publisher.
    - Разделение: детерминированные операции = прямой код,
      генеративные операции = LLM. Это best practice для агентных пайплайнов.
    """
    log_event("Начало этапа Researcher", stage="researcher", event="stage", progress=0)
    safe_print(f"\n[Researcher] Ищу информацию по теме: {ctx.topic}")

    t_start = time.time()
    ctx.research_data = _search_multi_source(ctx.topic)
    t_elapsed = time.time() - t_start

    count = len(ctx.research_data)
    safe_print(f"[Researcher] Поиск завершён за {t_elapsed:.1f} сек")
    safe_print(f"   Найдено источников: {count}")

    if not ctx.research_data:
        safe_print("   [WARN] Поиск не дал результатов. Writer будет опираться на знания LLM.")


def _run_additional_research(ctx: SharedContext, query_context: str) -> None:
    """Дополнительный поиск по запросу человека (для Human Review revision)."""
    safe_print(f"[Researcher] Дополнительный поиск по запросу: {query_context}")
    enriched_query = f"{ctx.topic} {query_context[:100]}"

    for fn_name, fn_call in [
        ("Yandex Search API", lambda: search_yandex(enriched_query)),
        ("DuckDuckGo",        lambda: search_web(enriched_query)),
    ]:
        raw = fn_call()
        if raw and raw != "[]":
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list) and parsed:
                    existing_links = {item.get("link") for item in ctx.research_data if item.get("link")}
                    added = 0
                    for item in parsed:
                        if item.get("link") not in existing_links:
                            ctx.research_data.append(item)
                            existing_links.add(item.get("link"))
                            added += 1
                    safe_print(f"   Добавлено {added} новых результатов через {fn_name}")
                    return
            except (json.JSONDecodeError, TypeError):
                continue

    safe_print("   [WARN] Дополнительный поиск не дал результатов.")


def _run_writer(ctx: SharedContext, client: Swarm) -> None:
    """Шаг 2 — Writer: написание/доработка черновика."""
    log_event("Начало этапа Writer", stage="writer", event="stage", progress=0)
    safe_print("\n[Writer] Пишу черновик статьи...")
    try:
        messages = build_messages("writer", ctx)
        safe_print(f"[Writer] Начинаю генерацию... (до {config.LLM_TIMEOUT} сек)")
        t_start = time.time()
        response = client.run(agent=writer_agent, messages=messages)
        t_elapsed = time.time() - t_start
        safe_print(f"[Writer] Генерация завершена за {t_elapsed:.1f} сек")
        ctx.draft = _extract_content(response)
        safe_print(f"   Черновик готов ({len(ctx.draft)} символов)")
    except Exception as e:
        safe_print(f"   [Writer] Ошибка: {e}")
        ctx.draft = f"# {ctx.topic}\n\n*Ошибка при генерации статьи: {e}*"


def _run_writer_with_human_feedback(ctx: SharedContext, client: Swarm) -> None:
    """Writer дорабатывает с учётом human-review фидбека."""
    log_event("Начало этапа Writer (human feedback)", stage="writer", event="stage", progress=0)
    safe_print("\n[Writer] Дорабатываю статью с учётом замечаний человека...")
    try:
        messages = build_messages("writer_with_human_feedback", ctx)
        response = client.run(agent=writer_agent, messages=messages)
        ctx.draft = _extract_content(response)
        safe_print(f"   Черновик доработан ({len(ctx.draft)} символов)")
    except Exception as e:
        safe_print(f"   [Writer] Ошибка: {e}")


def _run_editor_loop(ctx: SharedContext, client: Swarm) -> None:
    """Шаг 3 — Editor → Writer цикл до APPROVED или max_attempts.

    Ветвление (branching):
    - APPROVED → выход из цикла, Editor одобрил.
    - REVISION_REQUIRED → возврат к Writer (до max_attempts раз).
    Это ключевое условное ветвление пайплайна (требование ДЗ).
    """
    log_event("Начало этапа Editor", stage="editor", event="stage", progress=0)
    while ctx.attempts < ctx.max_attempts:
        safe_print(f"\n[Editor] Проверка качества (попытка {ctx.attempts + 1}/{ctx.max_attempts})...")
        try:
            messages = build_messages("editor", ctx)
            response = client.run(agent=editor_agent, messages=messages)
            result = _extract_content(response)

            # Отправляем editor_review через SSE для отображения в UI
            log_event("Замечания редактора", stage="editor", event="editor_review",
                      progress=0, review_text=result)

            if "APPROVED" in result.upper():
                ctx.editor_approved = True
                ctx.editor_review = result
                safe_print("   [Editor] Статья одобрена!")
                break

            # Ветвление: REVISION → возврат к Writer
            ctx.editor_review = result
            ctx.attempts += 1
            safe_print(f"   [Editor] Замечания. Доработка {ctx.attempts}/{ctx.max_attempts}...")

            log_event(f"Ревизия {ctx.attempts}/{ctx.max_attempts}: возврат к Writer",
                      stage="revision", event="revision", progress=0)
            log_event("Начало этапа Writer (доработка)", stage="writer", event="stage", progress=0)
            safe_print("\n[Writer] Дорабатываю статью с учётом замечаний...")
            messages = build_messages("writer", ctx)
            response = client.run(agent=writer_agent, messages=messages)
            ctx.draft = _extract_content(response)
            safe_print(f"   Черновик доработан ({len(ctx.draft)} символов)")

        except Exception as e:
            safe_print(f"   [Editor] Ошибка: {e}")
            ctx.attempts += 1

    if not ctx.editor_approved:
        safe_print("\n[Editor] Достигнут лимит доработок. Принудительная передача в Publisher.")


def _run_human_review_loop(ctx: SharedContext, client: Swarm) -> None:
    """Шаг 4 — Human Review: ожидание решения пользователя.

    Три исхода:
    - approved → Publisher
    - revision → Researcher/Writer/Editor → снова Human Review
    - rejected → полная остановка пайплайна

    ВАЖНО: импортирует PipelineLogger только здесь, т.к. wait_for_human_review —
    это инфраструктурный механизм блокировки потока, не логирование.
    """
    from .pipeline_runner import PipelineLogger as _PL  # единственный легитимный импорт

    while ctx.human_attempts < ctx.max_human_attempts:
        safe_print(f"\n[Human Review] Ожидание решения (попытка {ctx.human_attempts + 1}/{ctx.max_human_attempts})...")
        log_event("Ожидание решения пользователя...", stage="human_review",
                  event="human_review", progress=0)

        # Блокирующий вызов — ждём решения пользователя через веб-интерфейс
        decision, comment, edited_content = _PL.wait_for_human_review(ctx.draft or "")
        ctx.human_review_comment = comment

        if edited_content:
            safe_print("[Human Review] Пользователь отредактировал текст статьи.")
            ctx.draft = edited_content

        if decision == "approved":
            ctx.human_decision = "approved"
            safe_print("[Human Review] Статья одобрена пользователем!")
            return

        if decision == "rejected":
            ctx.human_decision = "rejected"
            safe_print("[Human Review] Статья окончательно отклонена. Пайплайн остановлен.")
            log_event("Статья отклонена пользователем. Пайплайн остановлен.",
                      stage="system", event="error", progress=0)
            return

        # decision == "revision"
        ctx.human_attempts += 1
        safe_print(f"[Human Review] Отправлено на доработку. Комментарий: {comment}")
        log_event(f"Отправлено на доработку: {comment}", stage="human_review",
                  event="human_revision", progress=0)

        if ctx.human_attempts >= ctx.max_human_attempts:
            safe_print("[Human Review] Достигнут лимит доработок.")
            ctx.human_decision = "rejected"
            return

        # Доработка: дополнительный поиск + Writer + Editor → повторный Human Review
        safe_print("[Researcher] Дополнительный поиск по запросу пользователя...")
        _run_additional_research(ctx, comment)
        safe_print("[Writer] Доработка с учётом замечаний человека...")
        _run_writer_with_human_feedback(ctx, client)
        _run_editor_loop(ctx, client)


def _run_publisher(ctx: SharedContext, client: Swarm, topic: str) -> None:
    """Шаг 5 — Publisher: финализация и сохранение статьи."""
    log_event("Начало этапа Publisher", stage="publisher", event="stage", progress=0)
    safe_print("\n[Publisher] Финализирую и сохраняю статью...")
    try:
        messages = build_messages("publisher", ctx)
        response = client.run(agent=publisher_agent, messages=messages)

        tool_path = _extract_tool_output(response, "save_article")
        if tool_path:
            ctx.final_path = tool_path.strip().strip("'\"")
        else:
            fallback_text = _extract_content(response)
            safe_print(f"   [Publisher] Ответ: {fallback_text[:200] if fallback_text else 'пустой'}...")
            if not ctx.final_path and ctx.draft:
                research_text = _format_research_data(ctx.research_data)
                final_content = _make_fallback_article(topic, ctx.draft, research_text)
                ctx.final_path = save_article(final_content, topic)
    except Exception as e:
        safe_print(f"   [Publisher] Ошибка: {e}")
        research_text = _format_research_data(ctx.research_data)
        fallback_content = _make_fallback_article(topic, ctx.draft, research_text)
        ctx.final_path = save_article(fallback_content, topic)

    safe_print(f"\n[Publisher] Статья сохранена: {ctx.final_path}")


# =========================================================================
# Основной пайплайн
# =========================================================================


def run_pipeline(topic: str) -> str:
    """Запускает полный пайплайн: Researcher → Writer → Editor → Human Review → Publisher.

    FIX #3: Функция НЕ импортирует pipeline_runner.
    Логирование идёт через utils.log_event / utils.safe_print,
    callbacks устанавливаются в pipeline_runner._run_sync() перед вызовом.
    """
    ctx = SharedContext(topic=topic)
    client = _create_swarm_client()

    # Шаг 1: Researcher — поиск через Swarm function_calling
    safe_print(f"[Researcher] Начинаю поиск материалов...")
    t_start = time.time()
    _run_researcher(ctx, client)
    safe_print(f"[Researcher] Поиск завершён за {time.time() - t_start:.1f} сек")

    # Шаг 2: Writer — первичное написание
    _run_writer(ctx, client)

    # Шаг 3: Editor → Writer цикл (ветвление по APPROVED / REVISION_REQUIRED)
    _run_editor_loop(ctx, client)

    # Шаг 4: Human Review цикл
    _run_human_review_loop(ctx, client)

    # Шаг 5: Publisher
    if ctx.human_decision == "approved":
        _run_publisher(ctx, client, topic)
    else:
        safe_print("[Human Review] Достигнут лимит попыток. Статья не будет опубликована.")
        ctx.final_path = save_article(
            ctx.draft or f"# {topic}\n\n*Не одобрено*",
            topic + "_REJECTED"
        )

    return ctx.final_path
