"""
FastAPI-сервер для мультиагентной системы «Конвейер генерации контента».

Предоставляет:
- REST API для запуска пайплайна и получения статей
- SSE-поток для событий в реальном времени
- Интерактивный UI (HTML + CSS + JS)

Запуск:
    uvicorn swarm_content_pipeline.server:app --reload --port 8000
"""

import datetime
import html
import json
import os
import asyncio
import re
import sys
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from . import config
from .pipeline_runner import runner, PipelineLogger, PipelineEvent

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Multi-Agent Content Pipeline",
    description="Интерактивный UI для мультиагентной системы генерации контента",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")
OUTPUT_DIR = os.path.abspath(config.OUTPUT_DIR)

# Подключаем статику
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Jinja2 шаблоны
templates = Jinja2Templates(directory=TEMPLATES_DIR)


# ---------------------------------------------------------------------------
# Pydantic модели
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    topic: str = Field(..., min_length=1, max_length=500, description="Тема статьи")
    max_attempts: int = Field(default=3, ge=1, le=10, description="Макс. попыток доработки")


class CancelResponse(BaseModel):
    ok: bool
    message: str


class ReviewRequest(BaseModel):
    task_id: str
    decision: str = Field(..., pattern="^(approved|revision|rejected)$")
    comment: str = Field(default="", max_length=2000)
    edited_content: str = Field(default="", max_length=50000)


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _get_articles() -> list[dict]:
    """Возвращает список статей из output/."""
    if not os.path.isdir(OUTPUT_DIR):
        return []
    articles = []
    for fname in sorted(os.listdir(OUTPUT_DIR), reverse=True):
        if fname.endswith(".md") and fname != ".gitkeep":
            fpath = os.path.join(OUTPUT_DIR, fname)
            stat = os.stat(fpath)
            # Пробуем извлечь заголовок из первых строк файла
            title = _extract_title(fpath) or fname
            articles.append({
                "filename": fname,
                "title": title,
                "date": datetime.datetime.fromtimestamp(stat.st_mtime).strftime("%d.%m.%Y %H:%M"),
                "size": stat.st_size,
            })
    return articles


def _extract_title(fpath: str) -> Optional[str]:
    """Извлекает первый Markdown-заголовок H1 из файла."""
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("# ") and not line.startswith("##"):
                    return line[2:].strip()
    except Exception:
        pass
    return None


def _read_article(filename: str) -> Optional[str]:
    """Читает содержимое статьи."""
    # Санитайз: не даём выйти за пределы output/
    safe_name = os.path.basename(filename)
    fpath = os.path.join(OUTPUT_DIR, safe_name)
    print(f"[API] _read_article — filename={filename!r} safe_name={safe_name!r} full_path={fpath}", file=sys.stderr)
    print(f"[API] _read_article — OUTPUT_DIR={OUTPUT_DIR!r}", file=sys.stderr)
    if not os.path.isfile(fpath):
        print(f"[API] _read_article — Файл НЕ НАЙДЕН: {fpath}", file=sys.stderr)
        return None
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            content = f.read()
            print(f"[API] _read_article — Файл прочитан: {len(content)} символов", file=sys.stderr)
            return content
    except Exception as e:
        print(f"[API] _read_article — Ошибка чтения: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# SSE: Server-Sent Events
# ---------------------------------------------------------------------------

async def _event_generator(task_id: str):
    """
    Генерирует SSE-события для задачи.
    Читает из существующей thread-safe queue.Queue через run_in_executor.
    Очередь уже создана в PipelineRunner.run() — используем её.

    HEARTBEAT: если очередь пуста более 30 сек подряд, отправляется
    событие 'stall_warning' — UI покажет предупреждение о зависании.
    """
    import queue as _queue
    q: _queue.Queue = PipelineLogger.get_queue(task_id)
    if q is None:
        # Если очередь ещё не создана — ждём немного
        for _ in range(10):
            await asyncio.sleep(0.1)
            q = PipelineLogger.get_queue(task_id)
            if q is not None:
                break
        if q is None:
            yield f"data: {json.dumps({'event': 'error', 'stage': 'system', 'message': 'Очередь задачи не найдена', 'progress': 0})}\n\n"
            return
    loop = asyncio.get_event_loop()
    _empty_polls = 0       # счётчик пустых опросов для stall-detection
    _stall_threshold = 10  # 10 * 3с = 30 сек тишины -> stall_warning
    try:
        while True:
            # Читаем из thread-safe очереди через executor (неблокирующий для asyncio)
            try:
                event: PipelineEvent = await loop.run_in_executor(
                    None, lambda: q.get(timeout=3.0)
                )
                # Событие получено — сбрасываем счётчик
                _empty_polls = 0
                print(f"[SSE] Got event: event={event.event} result_path={event.result_path}", file=sys.stderr)
            except _queue.Empty:
                print(f"[SSE] Queue empty (timeout)", file=sys.stderr)
                # Таймаут — проверяем статус задачи
                task = runner.get_task(task_id)
                if task and task.status in ("completed", "error", "cancelled"):
                    print(f"[SSE] TaskInfo fallback: status={task.status} result_path={task.result_path}", file=sys.stderr)
                    if task.status == "completed" and task.result_path:
                        yield f"data: {json.dumps({'event': 'complete', 'stage': 'system', 'message': f'Статья сохранена: {task.result_path}', 'progress': 100, 'result_path': task.result_path, 'timestamp': datetime.datetime.now().isoformat()})}\n\n"
                    elif task.status == "error" and task.error:
                        yield f"data: {json.dumps({'event': 'error', 'stage': 'system', 'message': f'Ошибка: {task.error}', 'progress': 0, 'timestamp': datetime.datetime.now().isoformat()})}\n\n"
                    elif task.status == "cancelled":
                        yield f"data: {json.dumps({'event': 'cancelled', 'stage': 'system', 'message': 'Задача отменена', 'progress': 0, 'timestamp': datetime.datetime.now().isoformat()})}\n\n"
                    break

                # Stall detection: если долго нет событий — предупреждаем UI
                _empty_polls += 1
                if _empty_polls >= _stall_threshold:
                    print(f"[SSE] Stall warning: polls={_empty_polls}", file=sys.stderr)
                    _empty_polls = 0
                    yield f"data: {json.dumps({'event': 'stall_warning', 'stage': 'system', 'message': 'Пайплайн не отвечает более 30 сек. Возможно, LLM API завис или превышен таймаут.', 'progress': 0})}\n\n"
                continue

            # СНАЧАЛА проверяем финальные события, потом отправляем
            if event.event == "complete" and not event.result_path:
                print(f"[SSE] SKIP complete without result_path", file=sys.stderr)
                continue

            # Формируем SSE-сообщение
            data = json.dumps(event.as_dict(), ensure_ascii=False)
            yield f"data: {data}\n\n"

            # Если событие финальное — завершаем поток
            if event.event in ("error", "cancelled"):
                print(f"[SSE] Final event: {event.event}", file=sys.stderr)
                break
            if event.event == "complete" and event.result_path:
                print(f"[SSE] GOT complete WITH result_path={event.result_path} — breaking", file=sys.stderr)
                break

    except asyncio.CancelledError:
        pass
    finally:
        # Очищаем очередь после завершения SSE-потока
        PipelineLogger.unregister_queue(task_id)


# ---------------------------------------------------------------------------
# REST API маршруты
# ---------------------------------------------------------------------------

@app.get("/api/articles")
async def api_list_articles():
    """JSON-список статей."""
    return JSONResponse(_get_articles())


@app.get("/api/articles/{filename:path}")
async def api_get_article(filename: str):
    """JSON с содержимым статьи."""
    content = _read_article(filename)
    if content is None:
        raise HTTPException(status_code=404, detail="Статья не найдена")
    return PlainTextResponse(content, media_type="text/markdown; charset=utf-8")


@app.delete("/api/articles/{filename:path}")
async def api_delete_article(filename: str):
    """Удаляет файл статьи из output/."""
    safe_name = os.path.basename(filename)
    fpath = os.path.join(OUTPUT_DIR, safe_name)
    if not os.path.isfile(fpath):
        raise HTTPException(status_code=404, detail="Статья не найдена")
    try:
        os.remove(fpath)
        return {"ok": True, "filename": safe_name, "message": "Статья удалена"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка удаления: {e}")


@app.post("/api/run")
async def api_run_pipeline(req: RunRequest):
    """Запускает пайплайн. Возвращает task_id."""
    task_id = await runner.run(req.topic, req.max_attempts)
    return {"task_id": task_id, "topic": req.topic}


@app.get("/api/status/{task_id}")
async def api_get_status(task_id: str):
    """Статус выполнения задачи."""
    info = runner.get_task(task_id)
    if info is None:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    return {
        "task_id": info.task_id,
        "topic": info.topic,
        "status": info.status,
        "progress": info.progress,
        "result_path": info.result_path,
        "error": info.error,
        "rejection_reason": getattr(info, "rejection_reason", None),
        "created_at": info.created_at,
        "completed_at": info.completed_at,
    }


@app.get("/api/events/{task_id}")
async def api_sse_events(task_id: str):
    """
    SSE-поток событий задачи.
    Клиент подключается к этому endpoint и получает события в реальном времени.
    """
    task = runner.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Задача не найдена")

    return StreamingResponse(
        _event_generator(task_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/cancel/{task_id}")
async def api_cancel_task(task_id: str):
    """Отменяет выполнение задачи."""
    ok = runner.cancel(task_id)
    if ok:
        return CancelResponse(ok=True, message="Задача отменена")
    raise HTTPException(status_code=404, detail="Задача не найдена или уже завершена")


# ---------------------------------------------------------------------------
# Human Review API
# ---------------------------------------------------------------------------

@app.get("/api/review/status/{task_id}")
async def api_review_status(task_id: str):
    """Проверяет, ожидает ли задача решения пользователя, и возвращает контент для ревью."""
    if not PipelineLogger.is_pending_review(task_id):
        return {"pending": False, "content": None}
    content = PipelineLogger.get_review_content(task_id)
    return {"pending": True, "content": content}


@app.post("/api/review/submit")
async def api_review_submit(req: ReviewRequest):
    """Принимает решение пользователя по ревью статьи."""
    ok = PipelineLogger.submit_human_decision(
        req.task_id, req.decision, req.comment, req.edited_content
    )
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="Задача не найдена или не ожидает ревью"
        )
    return {"ok": True, "decision": req.decision, "message": "Решение принято"}


@app.get("/api/history")
async def api_history():
    """История запусков пайплайна."""
    tasks = runner.get_all_tasks()
    return [
        {
            "task_id": t.task_id,
            "topic": t.topic,
            "status": t.status,
            "progress": t.progress,
            "result_path": t.result_path,
            "error": t.error,
            "rejection_reason": getattr(t, "rejection_reason", None),
            "created_at": t.created_at,
            "completed_at": t.completed_at,
        }
        for t in sorted(tasks, key=lambda x: x.created_at, reverse=True)
    ]


# ---------------------------------------------------------------------------
# UI маршруты (HTML)
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def ui_index(request: Request):
    """Главная страница с UI пайплайна."""
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "title": "Multi-Agent Pipeline"},
    )


@app.get("/history", response_class=HTMLResponse)
async def ui_history(request: Request):
    """Страница истории статей."""
    articles = _get_articles()
    return templates.TemplateResponse(
        "history.html",
        {"request": request, "title": "История статей", "articles": articles},
    )


@app.get("/api/agents")
async def api_get_agents():
    """Возвращает информацию об агентах (имена, описания, системные промпты)."""
    from .agents import (
        RESEARCHER_INSTRUCTIONS,
        WRITER_INSTRUCTIONS,
        EDITOR_INSTRUCTIONS,
        PUBLISHER_INSTRUCTIONS,
    )
    return JSONResponse([
        {
            "name": "Researcher",
            "stage": "researcher",
            "title": "Researcher Agent",
            "description": "Поиск информации по теме",
            "color": "blue",
            "model_config_key": "RESEARCHER_MODEL",
            "instructions": RESEARCHER_INSTRUCTIONS.strip(),
        },
        {
            "name": "Writer",
            "stage": "writer",
            "title": "Writer Agent",
            "description": "Написание черновика статьи",
            "color": "green",
            "model_config_key": "WRITER_MODEL",
            "instructions": WRITER_INSTRUCTIONS.strip(),
        },
        {
            "name": "Editor",
            "stage": "editor",
            "title": "Editor Agent",
            "description": "Проверка качества и рецензирование",
            "color": "yellow",
            "model_config_key": "EDITOR_MODEL",
            "instructions": EDITOR_INSTRUCTIONS.strip(),
        },
        {
            "name": "Publisher",
            "stage": "publisher",
            "title": "Publisher Agent",
            "description": "Финализация и сохранение статьи",
            "color": "purple",
            "model_config_key": "PUBLISHER_MODEL",
            "instructions": PUBLISHER_INSTRUCTIONS.strip(),
        },
    ])


@app.get("/article/{filename:path}", response_class=HTMLResponse)
async def ui_article(request: Request, filename: str):
    """Страница просмотра статьи."""
    content = _read_article(filename)
    if content is None:
        raise HTTPException(status_code=404, detail="Статья не найдена")

    # Пробуем найти заголовок
    title = _extract_title(os.path.join(OUTPUT_DIR, os.path.basename(filename))) or filename

    # Рендерим Markdown в HTML с санитизацией (XSS-защита)
    rendered = _render_markdown_safe(content)

    return templates.TemplateResponse(
        "article.html",
        {
            "request": request,
            "title": title,
            "filename": filename,
            "content": rendered,
        },
    )


def _render_markdown_safe(text: str) -> str:
    """Рендерит Markdown в безопасный HTML (XSS-защита).

    Экранируем HTML-сущности ДО рендеринга Markdown, чтобы
    инлайн-HTML (например <script>) не прошёл в вывод.
    Markdown-синтаксис (**bold**, [link](url)) при этом работает
    корректно — Markdown-процессор сам конвертирует его в HTML-теги.
    """
    # Экранируем HTML-сущности: &, <, >, ", '
    text = html.escape(text)
    # Конвертируем Markdown в HTML
    try:
        import markdown
        html_content = markdown.markdown(text, extensions=['fenced_code', 'tables'])
    except ImportError:
        # fallback: конвертируем двойные переносы строк в параграфы
        lines = []
        in_paragraph = False
        for line in text.split('\n'):
            if line.strip() == '':
                if in_paragraph:
                    lines.append('</p>')
                    in_paragraph = False
            else:
                if not in_paragraph:
                    lines.append('<p>')
                    in_paragraph = True
                # Заголовки
                if line.startswith('### '):
                    if in_paragraph:
                        lines.append('</p>')
                        in_paragraph = False
                    lines.append(f'<h3>{html.escape(line[4:])}</h3>')
                elif line.startswith('## '):
                    if in_paragraph:
                        lines.append('</p>')
                        in_paragraph = False
                    lines.append(f'<h2>{html.escape(line[3:])}</h2>')
                elif line.startswith('# '):
                    if in_paragraph:
                        lines.append('</p>')
                        in_paragraph = False
                    lines.append(f'<h1>{html.escape(line[2:])}</h1>')
                else:
                    lines.append(html.escape(line))
        if in_paragraph:
            lines.append('</p>')
        html_content = '\n'.join(lines)
    return html_content


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.datetime.now().isoformat()}


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def main():
    """Запуск сервера через uvicorn."""
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    host = os.getenv("HOST", "0.0.0.0")
    print(f"FastAPI сервер запущен на http://{host}:{port}")
    print(f"Документация: http://localhost:{port}/docs")
    uvicorn.run(
        "swarm_content_pipeline.server:app",
        host=host,
        port=port,
        reload=os.getenv("RELOAD", "false").lower() == "true",
    )


if __name__ == "__main__":
    main()
