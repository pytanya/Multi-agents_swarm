"""
Общие утилиты мультиагентной системы.

Содержит:
- safe_print: единственный безопасный print для Windows + SSE-callback (устраняет дублирование)
- log_event: отправка структурированных событий через callback
- set_log_callback / set_event_callback: DI-паттерн через thread-local storage

Зависимость pipeline_runner → workflow убрана:
  pipeline_runner устанавливает callbacks через utils,
  workflow.py использует utils и НЕ знает о pipeline_runner.
"""

import sys
import threading
from typing import Optional, Callable

# ---------------------------------------------------------------------------
# Thread-local хранилище для callback-функций (каждый поток — свой callback)
# ---------------------------------------------------------------------------

_thread_local = threading.local()


def set_log_callback(callback: Optional[Callable[[str], None]]) -> None:
    """Устанавливает callback для print-сообщений текущего потока.

    Вызывается из pipeline_runner перед запуском пайплайна.
    callback(message: str) — получает строку лога для SSE-трансляции.
    """
    _thread_local.log_callback = callback


def get_log_callback() -> Optional[Callable[[str], None]]:
    """Возвращает log-callback текущего потока (или None)."""
    return getattr(_thread_local, "log_callback", None)


def set_event_callback(
    callback: Optional[Callable[..., None]]
) -> None:
    """Устанавливает callback для структурированных событий текущего потока.

    callback(message, *, stage, event, progress, result_path, review_text)
    — совместим с сигнатурой PipelineLogger.log().
    """
    _thread_local.event_callback = callback


def get_event_callback() -> Optional[Callable[..., None]]:
    """Возвращает event-callback текущего потока (или None)."""
    return getattr(_thread_local, "event_callback", None)


# ---------------------------------------------------------------------------
# safe_print — единая реализация (устраняет дублирование между workflow / tools)
# ---------------------------------------------------------------------------

def safe_print(*args, **kwargs) -> None:
    """Безопасный print для Windows-консоли + SSE-трансляция.

    - Заменяет эмодзи на ASCII-аналоги.
    - Обрабатывает UnicodeEncodeError.
    - Вызывает log_callback (если установлен) для SSE-стриминга.
    """
    safe_args = []
    message_parts = []
    for arg in args:
        if isinstance(arg, str):
            arg = arg.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
            arg = arg.replace("\u2705", "[OK]").replace("\u274c", "[NO]")
            arg = arg.replace("\u26a0\ufe0f", "[WARN]").replace("\u26a0", "[WARN]")
            arg = arg.replace("\U0001f4a1", "[i]").replace("\U0001f4dd", "[i]")
            arg = arg.replace("\u2764", "(heart)")
            message_parts.append(arg)
        else:
            message_parts.append(str(arg))
        safe_args.append(arg)

    try:
        print(*safe_args, **kwargs)
    except UnicodeEncodeError:
        ascii_args = [
            a.encode("ascii", errors="replace").decode("ascii") if isinstance(a, str) else a
            for a in safe_args
        ]
        print(*ascii_args, **kwargs)

    # DI: вызываем callback если установлен (pipeline_runner регистрирует его)
    callback = get_log_callback()
    if callback:
        full_msg = " ".join(message_parts)
        try:
            callback(full_msg)
        except Exception as _e:
            print(f"[utils.safe_print] log_callback error: {_e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# log_event — структурированные события (stage, error, complete, ...)
# ---------------------------------------------------------------------------

def log_event(
    message: str,
    stage: str = "system",
    event: str = "log",
    progress: int = 0,
    result_path: Optional[str] = None,
    review_text: Optional[str] = None,
) -> None:
    """Отправляет структурированное событие через event_callback.

    Если callback не установлен — молча пропускает (консольный режим).
    Используется вместо прямого импорта pipeline_runner.PipelineLogger.
    """
    callback = get_event_callback()
    if callback:
        try:
            callback(
                message,
                stage=stage,
                event=event,
                progress=progress,
                result_path=result_path,
                review_text=review_text,
            )
        except Exception as _e:
            print(f"[utils.log_event] event_callback error: {_e}", file=sys.stderr)
