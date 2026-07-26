"""
Асинхронный Runner для пайплайна генерации контента.

Запускает синхронный пайплайн в отдельном потоке (run_in_executor)
и транслирует события через asyncio.Queue для SSE-потока.

Callback-механизм:
    PipelineLogger.callback — устанавливается раннером перед запуском.
    Все вызовы _safe_print() в workflow.py и tools.py будут
    перенаправлены в SSE-очередь соответствующей задачи.
"""

import asyncio
import datetime
import logging
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Optional

from .workflow import run_pipeline
from .utils import set_log_callback, set_event_callback


# ---------------------------------------------------------------------------
# Модели данных
# ---------------------------------------------------------------------------

@dataclass
class PipelineEvent:
    """Событие пайплайна для SSE-потока."""
    event: str          # "stage", "log", "error", "complete", "cancelled", "progress"
    stage: str          # "researcher", "writer", "editor", "publisher", "system"
    message: str
    progress: int       # 0-100
    result_path: Optional[str] = None
    review_text: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.datetime.now().isoformat())

    def as_dict(self) -> dict:
        d = {
            "event": self.event,
            "stage": self.stage,
            "message": self.message,
            "progress": self.progress,
            "timestamp": self.timestamp,
        }
        if self.result_path:
            d["result_path"] = self.result_path
        if self.review_text:
            d["review_text"] = self.review_text
        return d


@dataclass
class TaskInfo:
    """Информация о фоновой задаче."""
    task_id: str
    topic: str
    status: str                 # "running", "completed", "error", "cancelled", "rejected"
    progress: int = 0
    result_path: Optional[str] = None
    error: Optional[str] = None
    rejection_reason: Optional[str] = None  # причина отклонения человеком
    created_at: str = field(default_factory=lambda: datetime.datetime.now().isoformat())
    completed_at: Optional[str] = None


# ---------------------------------------------------------------------------
# PipelineLogger — thread-safe логгер для SSE
# ---------------------------------------------------------------------------

import queue as _queue  # thread-safe queue

class PipelineLogger:
    """
    Глобальный логгер, перенаправляющий utils.safe_print() / utils.log_event() в SSE-очередь.
    Потокобезопасен: использует queue.Queue (thread-safe) внутри.
    """

    _current_task = threading.local()
    # Используем queue.Queue вместо asyncio.Queue для thread-safe доступа
    _queues: dict[str, _queue.Queue[PipelineEvent]] = {}
    _lock = threading.Lock()
    # Общий thread-pool executor для всех задач
    # max_workers=4 позволяет запускать до 4 пайплайнов параллельно
    # (или 2 пайплайна с учётом, что client.run() может блокировать поток)
    _executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="pipeline")

    # Хранилище для human review (ожидание решения пользователя)
    _pending_reviews: dict[str, dict] = {}
    _review_lock = threading.Lock()

    # Handshake-механизм: pipeline-worker ждёт, пока SSE-клиент подключится
    _ready_events: dict[str, threading.Event] = {}

    @classmethod
    def register_ready_event(cls, task_id: str) -> threading.Event:
        event = threading.Event()
        with cls._lock:
            cls._ready_events[task_id] = event
        return event

    @classmethod
    def signal_ready(cls, task_id: str) -> None:
        with cls._lock:
            event = cls._ready_events.pop(task_id, None)
            if event:
                event.set()

    @classmethod
    def wait_for_ready(cls, task_id: str, timeout: float = 30.0) -> bool:
        event = cls._ready_events.get(task_id)
        if event:
            return event.wait(timeout=timeout)
        return True

    @classmethod
    def set_current_task(cls, task_id: str) -> None:
        """Устанавливает task_id для текущего потока."""
        cls._current_task.task_id = task_id

    @classmethod
    def get_current_task(cls) -> Optional[str]:
        """Возвращает task_id текущего потока или None."""
        return getattr(cls._current_task, "task_id", None)

    @classmethod
    def register_queue(cls, task_id: str) -> _queue.Queue:
        """Создаёт потокобезопасную очередь для задачи."""
        q: _queue.Queue[PipelineEvent] = _queue.Queue()
        with cls._lock:
            cls._queues[task_id] = q
        return q

    @classmethod
    def unregister_queue(cls, task_id: str) -> None:
        """Удаляет очередь задачи."""
        with cls._lock:
            cls._queues.pop(task_id, None)

    @classmethod
    def get_queue(cls, task_id: str) -> Optional[_queue.Queue]:
        with cls._lock:
            return cls._queues.get(task_id)

    @classmethod
    def log(cls, message: str, stage: str = "system", event: str = "log", progress: int = 0, result_path: Optional[str] = None, review_text: Optional[str] = None) -> None:
        """
        Публикует событие в очередь текущей задачи.
        Вызывается из utils.log_event() / utils.safe_print().
        Полностью потокобезопасен.
        """
        task_id = cls.get_current_task()
        if not task_id:
            print(f"[PipelineLogger] ERROR: No task_id found for thread {threading.current_thread().name}", file=sys.stderr)
            return

        q = cls.get_queue(task_id)
        if q is None:
            return

        evt = PipelineEvent(
            event=event,
            stage=stage,
            message=message,
            progress=progress,
            result_path=result_path,
            review_text=review_text,
        )
        # queue.Queue потокобезопасен — можно вызывать из любого потока
        q.put_nowait(evt)

    # ------------------------------------------------------------------
    # Human Review — блокировка пайплайна до решения пользователя
    # ------------------------------------------------------------------

    @classmethod
    def wait_for_human_review(cls, content: str) -> tuple[str, str, str]:
        """
        Блокирует поток пайплайна до получения решения пользователя.
        Вызывается из workflow.py (в thread pool).
        Возвращает (decision, comment, edited_content):
          - ("approved", "", "") или ("approved", "", "отредактированный текст")
          - ("revision", "комментарий", "отредактированный текст")
          - ("rejected", "", "")
        """
        task_id = cls.get_current_task()
        if not task_id:
            return ("approved", "", "")  # fallback: если нет task_id — пропускаем ревью

        review_event = threading.Event()
        with cls._review_lock:
            cls._pending_reviews[task_id] = {
                "content": content,
                "event": review_event,
                "decision": None,
                "comment": "",
                "edited_content": "",
            }

        # Блокируем поток, пока пользователь не примет решение (таймаут 1 час)
        review_event.wait(timeout=3600.0)  # 1 час таймаут
        if not review_event.is_set():
            # Таймаут - автопринимаем статью
            with cls._review_lock:
                review = cls._pending_reviews.pop(task_id, {})
            return ("approved", "", content)

        with cls._review_lock:
            review = cls._pending_reviews.pop(task_id, {})

        decision = review.get("decision", "approved")
        comment = review.get("comment", "")
        edited_content = review.get("edited_content", "")
        return (decision, comment, edited_content)

    @classmethod
    def submit_human_decision(cls, task_id: str, decision: str, comment: str = "", edited_content: str = "") -> bool:
        """
        Пользователь принял решение. Разблокирует поток пайплайна.
        Вызывается из server.py (HTTP handler).
        Возвращает True, если задача найдена и ожидает решения.
        """
        with cls._review_lock:
            review = cls._pending_reviews.get(task_id)
            if not review:
                return False
            review["decision"] = decision
            review["comment"] = comment
            review["edited_content"] = edited_content
            review["event"].set()  # разблокировка потока пайплайна
        return True

    @classmethod
    def get_review_content(cls, task_id: str) -> Optional[str]:
        """Возвращает контент, ожидающий ревью."""
        with cls._review_lock:
            review = cls._pending_reviews.get(task_id)
            if review:
                return review.get("content")
            return None

    @classmethod
    def is_pending_review(cls, task_id: str) -> bool:
        """Проверяет, ожидает ли задача решения пользователя."""
        with cls._review_lock:
            return task_id in cls._pending_reviews


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class PipelineRunner:
    """
    Запускает пайплайн в фоновом потоке и транслирует события через SSE.
    """

    def __init__(self):
        self._tasks: dict[str, TaskInfo] = {}
        self._cancelled: set[str] = set()

    async def run(self, topic: str, max_attempts: int = 3) -> str:
        """Запускает пайплайн. Возвращает task_id."""
        task_id = str(uuid.uuid4())

        # Регистрируем очередь и handshake-event для SSE
        PipelineLogger.register_queue(task_id)
        PipelineLogger.register_ready_event(task_id)

        # Создаём информацию о задаче
        info = TaskInfo(task_id=task_id, topic=topic, status="running")
        self._tasks[task_id] = info

        # Публикуем событие начала (синхронно — очередь thread-safe)
        self._publish_event(task_id, PipelineEvent(
            event="stage", stage="system",
            message=f"Запуск пайплайна для темы: {topic}",
            progress=0,
        ))

        # Запускаем в thread pool
        loop = asyncio.get_running_loop()
        loop.run_in_executor(
            PipelineLogger._executor,
            self._run_sync,
            task_id, topic, max_attempts,
        )

        # Ожидаем готовности воркера (таймаут 30 сек)
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: PipelineLogger.wait_for_ready(task_id, timeout=30.0)
        )

        return task_id

    def _run_sync(self, task_id: str, topic: str, max_attempts: int) -> None:
        """Синхронный запуск пайплайна (выполняется в thread pool).

        FIX #3: устанавливает thread-local callbacks из utils.py,
        чтобы workflow.py мог логировать в SSE очередь без прямого
        импорта pipeline_runner (Dependency Inversion).
        """
        PipelineLogger.set_current_task(task_id)
        PipelineLogger.signal_ready(task_id)  # handshake: сигнал готовности

        # --- DI: регистрируем callbacks для текущего потока ---
        def _log_callback(message: str) -> None:
            """Определяет stage по префиксу и пишет в SSE-очередь."""
            stage = "system"
            if message.startswith(("[Researcher]", "[research")) or any(
                kw in message for kw in ("Yandex", "DuckDuckGo", "Tavily")
            ):
                stage = "researcher"
            elif message.startswith(("[Writer]", "[writer")):
                stage = "writer"
            elif message.startswith(("[Editor]", "[editor")):
                stage = "editor"
            elif message.startswith(("[Publisher]", "[publisher")):
                stage = "publisher"
            elif message.startswith(("[Human Review]", "[Human", "[human")):
                stage = "human_review"
            elif "Ошибка" in message or "Error" in message:
                stage = "error"
            event = "error" if stage == "error" else "log"
            PipelineLogger.log(message, stage=stage, event=event)

        set_log_callback(_log_callback)
        set_event_callback(PipelineLogger.log)
        # -------------------------------------------------------

        print(f"[PipelineRunner] Starting pipeline for task_id={task_id}", file=sys.stderr)

        try:
            # Проверка на отмену
            if task_id in self._cancelled:
                PipelineLogger.log("Задача отменена до запуска", "system", "cancelled")
                self._update_task_status(task_id, "cancelled")
                self._cancelled.discard(task_id)
                return

            # Запускаем синхронный пайплайн
            result_path = run_pipeline(topic)

            # Проверка на отмену после завершения
            if task_id in self._cancelled:
                PipelineLogger.log("Задача отменена", "system", "cancelled")
                self._update_task_status(task_id, "cancelled")
                self._cancelled.discard(task_id)
                return

            if result_path:
                # Определяем финальный статус и причину отклонения
                from .workflow import SharedContext as _SC  # не можем передать ctx наружу, поэтому кодируем через result_path
                is_rejected = result_path and "_REJECTED" in str(result_path)
                if is_rejected:
                    self._update_task_status(task_id, "rejected", result_path=result_path)
                    # Извлекаем причину отклонения из пендинг ревью (last_comment)
                    pending = PipelineLogger._pending_reviews.get(task_id, {})
                    last_comment = pending.get("comment", "")
                    info = self._tasks.get(task_id)
                    if info:
                        info.rejection_reason = last_comment or "Причина не указана"
                    PipelineLogger.log(
                        f"Статья отклонена: {result_path}",
                        "system", "rejected", 0,
                        result_path=result_path,
                    )
                else:
                    self._update_task_status(task_id, "completed", result_path=result_path)
                    PipelineLogger.log(
                        f"Статья сохранена: {result_path}",
                        "system", "complete", 100,
                        result_path=result_path,
                    )
            else:
                self._update_task_status(task_id, "error", error="Пайплайн не вернул путь к файлу")
                PipelineLogger.log("Пайплайн не вернул путь к файлу", "system", "error")

        except Exception as e:
            error_msg = str(e)
            self._update_task_status(task_id, "error", error=error_msg)
            PipelineLogger.log(f"Ошибка: {error_msg}", "system", "error")
            import traceback
            traceback.print_exc()

        finally:
            # Очищаем thread-local callbacks (DI cleanup)
            set_log_callback(None)
            set_event_callback(None)
            PipelineLogger.set_current_task(None)
            self._cancelled.discard(task_id)

    def cancel(self, task_id: str) -> bool:
        """Отменяет задачу. Возвращает True, если задача найдена."""
        info = self._tasks.get(task_id)
        if info is None:
            return False
        if info.status != "running":
            return False
        self._cancelled.add(task_id)

        # Пытаемся положить событие отмены в очередь
        q = PipelineLogger.get_queue(task_id)
        if q:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.call_soon_threadsafe(q.put_nowait, PipelineEvent(
                        event="cancelled", stage="system",
                        message="Задача отменена пользователем",
                        progress=0,
                    ))
            except RuntimeError:
                pass

        return True

    def get_task(self, task_id: str) -> Optional[TaskInfo]:
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> list[TaskInfo]:
        return list(self._tasks.values())

    def _update_task_status(
        self, task_id: str,
        status: str,
        result_path: Optional[str] = None,
        error: Optional[str] = None,
        rejection_reason: Optional[str] = None,
    ) -> None:
        info = self._tasks.get(task_id)
        if info is None:
            return
        info.status = status
        info.progress = 100 if status == "completed" else info.progress
        if result_path:
            info.result_path = result_path
        if error:
            info.error = error
        if rejection_reason:
            info.rejection_reason = rejection_reason
        if status in ("completed", "error", "cancelled", "rejected"):
            info.completed_at = datetime.datetime.now().isoformat()

    def _publish_event(self, task_id: str, event: PipelineEvent) -> None:
        """Публикует событие в очередь задачи (синхронно, thread-safe)."""
        q = PipelineLogger.get_queue(task_id)
        if q is not None:
            q.put_nowait(event)


# ---------------------------------------------------------------------------
# Глобальный экземпляр раннера
# ---------------------------------------------------------------------------

runner = PipelineRunner()
