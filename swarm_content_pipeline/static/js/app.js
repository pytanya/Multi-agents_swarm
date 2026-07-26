/**
 * Multi-Agent Pipeline UI — Client App
 *
 * Функции:
 * - Управление формой запуска пайплайна
 * - SSE-подключение для событий в реальном времени
 * - Анимация шагов Pipeline Flow
 * - Консоль лога с цветовым кодированием
 * - Предпросмотр статьи
 * - История задач
 */

(function () {
    'use strict';

    // =========================================================================
    // Состояние приложения
    // =========================================================================
    const state = {
        taskId: null,
        eventSource: null,
        isRunning: false,
        currentStage: null,
        editorReviewText: null,
    };

    // =========================================================================
    // DOM-элементы
    // =========================================================================
    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);

    const el = {
        form: $('#pipeline-form'),
        topicInput: $('#topic-input'),
        runBtn: $('#run-btn'),
        cancelBtn: $('#cancel-btn'),
        errorMsg: $('#error-message'),
        pipelineContent: $('#pipeline-content'),
        consoleLog: $('#console-log'),
        previewContent: $('#preview-content'),
        previewActions: $('#preview-actions'),
        viewArticleBtn: $('#view-article-btn'),
        clearConsoleBtn: $('#clear-console-btn'),
        copyConsoleBtn: $('#copy-console-btn'),
        copyArticleBtn: $('#copy-article-btn'),
        revisionBadge: $('#revision-badge'),
        revisionCount: $('#revision-count'),
        tasksList: $('#tasks-list'),
    };

    // Review modal elements
    const reviewModal = document.getElementById('review-modal');
    const reviewEditor = document.getElementById('review-editor');
    const reviewPreview = document.getElementById('review-preview');
    const reviewApproveBtn = document.getElementById('review-approve-btn');
    const reviewRevisionBtn = document.getElementById('review-revision-btn');
    const reviewRejectBtn = document.getElementById('review-reject-btn');
    const reviewComment = document.getElementById('review-comment');
    const reviewCloseBtn = document.getElementById('review-close-btn');

    let _pendingReviewTaskId = null;
    let _debounceTimer = null;

    // =========================================================================
    // Human Review функции
    // =========================================================================

    /** Обновляет предпросмотр из textarea (debounced) */
    function updateReviewPreview() {
        if (!reviewEditor || !reviewPreview) return;
        const md = reviewEditor.value || '';
        if (typeof marked !== 'undefined' && marked.parse) {
            try {
                reviewPreview.innerHTML = marked.parse(md);
            } catch (e) {
                reviewPreview.innerHTML = '<p style="color:var(--accent-red)">[Ошибка рендеринга Markdown]</p>';
            }
        } else {
            // fallback если marked.js не загрузился
            reviewPreview.innerHTML = '<pre style="white-space:pre-wrap">' + md.replace(/</g, '<') + '</pre>';
        }
    }

    function showReviewModal(content, taskId) {
        _pendingReviewTaskId = taskId;
        if (reviewEditor) {
            // Ставим исходный markdown в textarea
            reviewEditor.value = content || '';
        }
        if (reviewComment) {
            reviewComment.value = '';
        }
        // Сразу обновляем предпросмотр
        updateReviewPreview();
        if (reviewModal) {
            reviewModal.style.display = 'flex';
        }
    }

    function hideReviewModal() {
        if (reviewModal) {
            reviewModal.style.display = 'none';
        }
        _pendingReviewTaskId = null;
    }

    async function submitReview(decision) {
        if (!_pendingReviewTaskId) return;

        const taskId = _pendingReviewTaskId;
        const comment = reviewComment ? reviewComment.value.trim() : '';
        // Берём отредактированный текст из textarea
        const editedContent = reviewEditor ? reviewEditor.value : '';

        // Валидация: revision требует комментарий
        if (decision === 'revision' && !comment) {
            alert('Пожалуйста, укажите что нужно исправить или добавить в статье.');
            return;
        }

        // Подтверждение для rejected (полная остановка)
        if (decision === 'rejected' && !confirm('Вы уверены? Статья будет окончательно отклонена и не попадёт в публикацию.')) {
            return;
        }

        try {
            const resp = await fetch('/api/review/submit', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ task_id: taskId, decision, comment, edited_content: editedContent }),
            });
            if (resp.ok) {
                hideReviewModal();
                switch (decision) {
                    case 'approved':
                        addLogLine('[Human Review] Статья одобрена!', 'system');
                        break;
                    case 'revision':
                        addLogLine(`[Human Review] Отправлено на доработку: "${comment}"`, 'system');
                        addLogLine('[Human Review] Запущен цикл доработки...', 'system');
                        incrementHumanRevision();
                        break;
                    case 'rejected':
                        addLogLine('[Human Review] Статья окончательно отклонена.', 'error');
                        break;
                }
            } else {
                const err = await resp.json();
                addLogLine(`[Human Review] Ошибка: ${err.detail || 'Не удалось отправить решение'}`, 'error');
            }
        } catch (e) {
            addLogLine(`[Human Review] Ошибка сети: ${e.message}`, 'error');
        }
    }

    const stepStatuses = {
        researcher: $('#status-researcher .status-dot'),
        writer: $('#status-writer .status-dot'),
        editor: $('#status-editor .status-dot'),
        human_review: $('#status-human_review .status-dot'),
        publisher: $('#status-publisher .status-dot'),
    };

    const progressBars = {
        researcher: $('#progress-researcher'),
        writer: $('#progress-writer'),
        editor: $('#progress-editor'),
        human_review: $('#progress-human_review'),
        publisher: $('#progress-publisher'),
    };

    const stepCards = {
        researcher: document.querySelector('.step-card[data-stage="researcher"]'),
        writer: document.querySelector('.step-card[data-stage="writer"]'),
        editor: document.querySelector('.step-card[data-stage="editor"]'),
        human_review: document.querySelector('.step-card[data-stage="human_review"]'),
        publisher: document.querySelector('.step-card[data-stage="publisher"]'),
    };

    // =========================================================================
    // Утилиты
    // =========================================================================
    function timestamp() {
        const now = new Date();
        return now.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    }

    function sanitizeText(text) {
        // Заменяем эмодзи и проблемные символы
        return text
            .replace(/\u2705/g, '[OK]')
            .replace(/\u274c/g, '[NO]')
            .replace(/\u26a0\ufe0f/g, '[WARN]')
            .replace(/\u26a0/g, '[WARN]')
            .replace(/\u{1f4a1}/gu, '[i]')
            .replace(/\u{1f4dd}/gu, '[i]')
            .replace(/\u2764/g, '(heart)');
    }

    // =========================================================================
    // Консоль лога
    // =========================================================================
    function addLogLine(message, stage = 'system') {
        const placeholder = el.consoleLog.querySelector('.console-placeholder');
        if (placeholder) {
            placeholder.remove();
        }

        const line = document.createElement('div');
        line.className = `console-line stage-${stage}`;

        const time = document.createElement('span');
        time.className = 'timestamp';
        time.textContent = `[${timestamp()}]`;
        line.appendChild(time);

        const text = document.createElement('span');
        text.textContent = sanitizeText(message);
        line.appendChild(text);

        el.consoleLog.appendChild(line);
        el.consoleLog.scrollTop = el.consoleLog.scrollHeight;
    }

    function clearConsole() {
        el.consoleLog.innerHTML = '<div class="console-placeholder">Консоль очищена</div>';
    }

    function copyConsole() {
        const lines = Array.from(el.consoleLog.querySelectorAll('.console-line'))
            .map(line => line.textContent)
            .join('\n');
        if (lines) {
            navigator.clipboard.writeText(lines).catch(() => {});
        }
    }

    // =========================================================================
    // Pipeline Flow — управление шагами
    // =========================================================================
    function resetSteps() {
        Object.values(stepCards).forEach(card => {
            if (card) {
                card.classList.remove('active', 'completed', 'error', 'cancelled');
            }
        });
        Object.values(stepStatuses).forEach(dot => {
            if (dot) {
                dot.className = 'status-dot pending';
            }
        });
        Object.values(progressBars).forEach(bar => {
            if (bar) {
                bar.style.width = '0%';
            }
        });
        el.revisionBadge.style.display = 'none';
        el.revisionCount.textContent = '0';
    }

    function setStepStatus(stage, status) {
        // Для human_review показываем карточку при начале
        if (stage === 'human_review') {
            const card = document.getElementById('human-review-card');
            if (card) {
                card.style.display = 'flex';
            }
        }

        const card = stepCards[stage];
        const dot = stepStatuses[stage];
        const bar = progressBars[stage];

        if (card) {
            card.classList.remove('active', 'completed', 'error', 'cancelled');
            if (status === 'running') card.classList.add('active');
            else if (status === 'completed') card.classList.add('completed');
            else if (status === 'error') card.classList.add('error');
            else if (status === 'cancelled') card.classList.add('cancelled');
        }

        if (dot) {
            dot.className = 'status-dot';
            if (status === 'running') dot.classList.add('running');
            else if (status === 'completed') dot.classList.add('success');
            else if (status === 'error') dot.classList.add('error');
            else if (status === 'cancelled') dot.className = 'status-dot pending';
            else dot.classList.add('pending');
        }

        if (bar && status === 'completed') {
            bar.style.width = '100%';
        }
    }

    function incrementRevision() {
        // Старый badge внутри карточки Editor
        const count = parseInt(el.revisionCount.textContent, 10) + 1;
        el.revisionCount.textContent = count;
        el.revisionBadge.style.display = 'flex';

        // Новый revision loop индикатор
        const loopCount = document.getElementById('revision-loop-count');
        if (loopCount) loopCount.textContent = count;
        const loop = document.getElementById('revision-loop');
        if (loop) loop.style.display = 'flex';
    }

    function showRevisionLoop(count) {
        const loop = document.getElementById('revision-loop');
        if (loop) loop.style.display = 'flex';
        const loopCount = document.getElementById('revision-loop-count');
        if (loopCount) loopCount.textContent = count || '1';
    }

    function hideRevisionLoop() {
        const loop = document.getElementById('revision-loop');
        if (loop) loop.style.display = 'none';
        const badge = document.getElementById('revision-badge');
        if (badge) badge.style.display = 'none';
    }

    function incrementHumanRevision() {
        const countEl = document.getElementById('human-revision-count');
        if (countEl) {
            const count = parseInt(countEl.textContent, 10) + 1;
            countEl.textContent = count;
        }
        const badge = document.getElementById('human-revision-badge');
        if (badge) badge.style.display = 'flex';
        const loop = document.getElementById('human-revision-loop');
        if (loop) loop.style.display = 'flex';
    }

    // =========================================================================
    // Предпросмотр статьи
    // =========================================================================
    function showPreview(content, resultPath) {
        // Диагностика: проверяем контейнер
        if (!el.previewContent) {
            console.error('[Preview] el.previewContent не найден в DOM!');
            addLogLine('[Error] Preview контейнер не найден в DOM', 'error');
            return;
        }
        if (typeof content !== 'string' || !content.trim()) {
            console.warn('[Preview] Получен пустой контент');
            addLogLine('[Warn] Предпросмотр: пустой контент', 'system');
            return;
        }

        const placeholder = el.previewContent.querySelector('.preview-placeholder');
        if (placeholder) {
            placeholder.remove();
        }

        // Конвертируем Markdown в HTML (грубо, для предпросмотра)
        const html = simpleMarkdown(content);

        el.previewContent.innerHTML = `<div class="markdown-body">${html}</div>`;
        el.previewContent.classList.add('show');
        el.previewActions.style.display = 'flex';

        addLogLine(`[Preview] Предпросмотр загружен: ${content.length} символов`, 'system');

        // Кнопка "Открыть"
        if (resultPath) {
            const filename = resultPath.split(/[\\/]/).pop();
            el.viewArticleBtn.onclick = () => {
                window.open(`/article/${filename}`, '_blank');
            };
            el.viewArticleBtn.style.display = 'flex';
        }
    }

    function simpleMarkdown(text) {
        if (!text) return '<p class="empty">(пусто)</p>';

        // Экранируем HTML
        let html = text
            .replace(/&/g, '&')
            .replace(/</g, '<')
            .replace(/>/g, '>');

        // Заголовки
        html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
        html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
        html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');

        // Жирный и курсив
        html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
        html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

        // Код
        html = html.replace(/`([^`]+)`/g, '<code>$1</code>');

        // Ссылки
        html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');

        // Горизонтальная линия
        html = html.replace(/^---$/gm, '<hr>');

        // Блокцитаты
        html = html.replace(/^> (.+)$/gm, '<blockquote>$1</blockquote>');

        // Списки
        html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
        html = html.replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>');

        // Параграфы
        const paragraphs = html.split('\n\n');
        html = paragraphs.map(p => {
            const trimmed = p.trim();
            if (!trimmed) return '';
            if (trimmed.startsWith('<h') || trimmed.startsWith('<ul') || trimmed.startsWith('<li') || trimmed.startsWith('<blockquote') || trimmed.startsWith('<hr')) {
                return trimmed;
            }
            return `<p>${trimmed.replace(/\n/g, '<br>')}</p>`;
        }).join('\n');

        return html;
    }

    // =========================================================================
    // SSE — Server-Sent Events
    // =========================================================================
    function connectSSE(taskId) {
        if (state.eventSource) {
            state.eventSource.close();
        }

        state.eventSource = new EventSource(`/api/events/${taskId}`);

        state.eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                handlePipelineEvent(data);
            } catch (e) {
                // ignore parse errors
            }
        };

        state.eventSource.onerror = () => {
            // SSE может переподключаться автоматически
            // Если задача уже завершена — закрываем
            checkTaskStatus(taskId);
        };
    }

    async function checkTaskStatus(taskId) {
        try {
            const resp = await fetch(`/api/status/${taskId}`);
            if (!resp.ok) return;
            const data = await resp.json();

            if (data.status === 'completed' || data.status === 'error' || data.status === 'cancelled') {
                if (state.eventSource) {
                    state.eventSource.close();
                    state.eventSource = null;
                }
                finishPipeline(data.status, data.result_path, data.error);
            }
        } catch (e) {
            // ignore
        }
    }

    // =========================================================================
    // Обработка событий пайплайна
    // =========================================================================
    function handlePipelineEvent(data) {
        const { event, stage, message, progress } = data;

        // Добавляем в консоль
        addLogLine(message, stage);

        if (event === 'stage') {
            // Начало нового этапа
            if (state.currentStage && state.currentStage !== stage) {
                setStepStatus(state.currentStage, 'completed');
            }

            // Если это writer и перед ним был editor — это ревизия
            if (stage === 'writer' && state.currentStage === 'editor') {
                incrementRevision();
            }

            state.currentStage = stage;
            setStepStatus(stage, 'running');

        } else if (event === 'revision') {
            // Событие ревизии (Editor отклонил, возврат к Writer)
            showRevisionLoop(data.progress ? Math.round(data.progress) : undefined);

        } else if (event === 'log') {
            // Обновляем прогресс если есть
            if (progress > 0 && stage && progressBars[stage]) {
                progressBars[stage].style.width = `${Math.min(progress, 95)}%`;
            }

        } else if (event === 'progress') {
            // Обновление прогресса
            if (stage && progressBars[stage]) {
                progressBars[stage].style.width = `${Math.min(progress, 95)}%`;
            }

        } else if (event === 'editor_review') {
            // Editor review — сохраняем текст замечаний
            if (data.review_text) {
                state.editorReviewText = data.review_text;
                // Показываем модальное окно с замечаниями редактора
                showEditorReviewModal(data.review_text);
            }

        } else if (event === 'human_review') {
            // Этап Human Review — сначала отмечаем предыдущий этап как завершённый
            if (state.currentStage && state.currentStage !== stage) {
                setStepStatus(state.currentStage, 'completed');
            }

            // Показываем модальное окно
            state.currentStage = stage;
            setStepStatus(stage, 'running');

            // Polling с exponential backoff для проверки pending review
            let retries = 0;
            const maxRetries = 30;
            const pollReview = () => {
                fetch(`/api/review/status/${state.taskId}`)
                    .then(r => r.json())
                    .then(data => {
                        if (data.pending && data.content) {
                            showReviewModal(data.content, state.taskId);
                        } else if (++retries < maxRetries) {
                            const delay = Math.min(1000 * Math.pow(1.5, retries), 10000);
                            setTimeout(pollReview, delay);
                        }
                    })
                    .catch(() => {
                        if (++retries < maxRetries) {
                            setTimeout(pollReview, 5000);
                        }
                    });
            };
            pollReview();

        } else if (event === 'human_revision') {
            // Событие ревизии Human Review
            incrementHumanRevision();

        } else if (event === 'complete') {
            finishPipeline('completed', data.result_path);

        } else if (event === 'error') {
            finishPipeline('error', null, message);

        } else if (event === 'cancelled') {
            finishPipeline('cancelled');
        }
    }

    function finishPipeline(status, resultPath, errorMsg) {
        state.isRunning = false;

        // Закрываем SSE
        if (state.eventSource) {
            state.eventSource.close();
            state.eventSource = null;
        }

        // Обновляем UI
        el.runBtn.style.display = 'flex';
        el.cancelBtn.style.display = 'none';
        el.topicInput.disabled = false;

        // Завершаем текущий шаг
        if (state.currentStage) {
            if (status === 'completed') {
                setStepStatus(state.currentStage, 'completed');
            } else if (status === 'error') {
                setStepStatus(state.currentStage, 'error');
            } else if (status === 'cancelled') {
                setStepStatus(stage, 'cancelled');
                // Отмечаем все шаги как cancelled
                Object.keys(stepCards).forEach(s => setStepStatus(s, 'cancelled'));
            }
        } else {
            // Если статус пришёл без currentStage (cancelled до запуска)
            if (status === 'cancelled') {
                Object.keys(stepCards).forEach(s => setStepStatus(s, 'cancelled'));
            }
        }

        if (status === 'completed' && resultPath) {
            // Загружаем предпросмотр
            const filename = resultPath.split(/[\\/]/).pop();
            fetch(`/api/articles/${filename}`)
                .then(r => {
                    if (!r.ok) throw new Error(`HTTP ${r.status}`);
                    return r.text();
                })
                .then(content => showPreview(content, resultPath))
                .catch(err => addLogLine(`[Error] Preview load failed: ${err.message}`, 'error'));
            addLogLine(`[Done] Статья сохранена: ${resultPath}`, 'complete');
        } else if (status === 'error') {
            if (el.errorMsg) {
                el.errorMsg.textContent = errorMsg || 'Произошла ошибка при выполнении пайплайна';
                el.errorMsg.style.display = 'block';
            }
            addLogLine(`[Error] ${errorMsg || 'Неизвестная ошибка'}`, 'error');
        } else if (status === 'cancelled') {
            addLogLine('[Cancelled] Выполнение отменено пользователем', 'system');
        }

        state.taskId = null;
        state.currentStage = null;
    }

    // =========================================================================
    // Запуск пайплайна
    // =========================================================================
    async function startPipeline(topic) {
        try {
            el.errorMsg.style.display = 'none';

            const resp = await fetch('/api/run', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ topic }),
            });

            if (!resp.ok) {
                const err = await resp.json();
                throw new Error(err.detail || `HTTP ${resp.status}`);
            }

            const data = await resp.json();
            state.taskId = data.task_id;
            state.isRunning = true;

            // Показываем UI пайплайна
            el.pipelineContent.style.display = 'block';
            el.runBtn.style.display = 'none';
            el.cancelBtn.style.display = 'flex';
            el.topicInput.disabled = true;

            // Сбрасываем шаги
            resetSteps();
            addLogLine(`[System] Запуск пайплайна для темы: "${topic}"`, 'system');

            // Подключаемся к SSE
            connectSSE(state.taskId);

        } catch (e) {
            el.errorMsg.textContent = `Ошибка запуска: ${e.message}`;
            el.errorMsg.style.display = 'block';
            state.isRunning = false;
            el.runBtn.style.display = 'flex';
            el.cancelBtn.style.display = 'none';
            el.topicInput.disabled = false;
        }
    }

    // =========================================================================
    // Отмена пайплайна
    // =========================================================================
    async function cancelPipeline() {
        if (!state.taskId) return;

        try {
            await fetch(`/api/cancel/${state.taskId}`, { method: 'POST' });
            addLogLine('[System] Отмена задачи...', 'system');
        } catch (e) {
            // ignore
        }
    }

    // =========================================================================
    // Editor Review Modal
    // =========================================================================
    function showEditorReviewModal(reviewText) {
        const modal = document.getElementById('editor-review-modal');
        const contentEl = document.getElementById('editor-review-content');
        if (!modal || !contentEl) return;

        // Конвертируем markdown в HTML
        contentEl.innerHTML = simpleMarkdown(reviewText || 'Нет замечаний');
        modal.style.display = 'flex';
    }

    function closeEditorReviewModal() {
        const modal = document.getElementById('editor-review-modal');
        if (modal) modal.style.display = 'none';
    }

    // =========================================================================
    // Agent Info Modal
    // =========================================================================
    let agentDataCache = null;

    async function loadAgentData() {
        if (agentDataCache) return agentDataCache;
        try {
            const resp = await fetch('/api/agents');
            if (!resp.ok) return [];
            agentDataCache = await resp.json();
            return agentDataCache;
        } catch (e) {
            return [];
        }
    }

    function openAgentModal(stage) {
        if (!agentDataCache) return;

        const agent = agentDataCache.find(a => a.stage === stage);
        if (!agent) return;

        const modal = document.getElementById('agent-modal');
        if (!modal) return;

        // Set icon
        const iconEl = document.getElementById('modal-icon');
        if (iconEl) iconEl.textContent = agent.name.charAt(0);

        // Set name
        const nameEl = document.getElementById('modal-agent-name');
        if (nameEl) nameEl.textContent = agent.title;

        // Set description
        const descEl = document.getElementById('modal-agent-desc');
        if (descEl) descEl.textContent = agent.description;

        // Set instructions
        const instrEl = document.getElementById('modal-agent-prompt');
        if (instrEl) instrEl.textContent = agent.instructions;

        // Set model
        const modelEl = document.getElementById('modal-model');
        if (modelEl) {
            const configKey = agent.model_config_key || '';
            modelEl.textContent = configKey ? configKey + ' (см. конфигурацию)' : 'Не указана';
        }

        // Set tools
        const toolsEl = document.getElementById('modal-tools');
        if (toolsEl) {
            const toolMap = {
                researcher: 'search_web, search_yandex, search_tavily',
                writer: 'Нет инструментов (чистая генерация текста)',
                editor: 'Нет инструментов (анализ текста)',
                publisher: 'save_article',
            };
            toolsEl.textContent = toolMap[stage] || '-';
        }

        // Apply stage class for icon color
        if (iconEl) {
            iconEl.className = 'modal-agent-icon';
        }

        modal.style.display = 'flex';
    }

    function closeAgentModal() {
        const modal = document.getElementById('agent-modal');
        if (modal) modal.style.display = 'none';
    }

    // =========================================================================
    // История задач
    // =========================================================================
    async function loadTasksHistory() {
        if (!el.tasksList) return;

        try {
            const resp = await fetch('/api/history');
            const tasks = await resp.json();

            if (tasks.length === 0) {
                el.tasksList.innerHTML = '<div class="tasks-placeholder">Нет запусков</div>';
                return;
            }

            el.tasksList.innerHTML = tasks.map(t => `
                <div class="task-item">
                    <span class="task-status-badge ${t.status}">${t.status}</span>
                    <span class="task-topic">${escapeHtml(t.topic)}</span>
                    <span class="task-time">${formatTime(t.created_at)}</span>
                </div>
            `).join('');
        } catch (e) {
            el.tasksList.innerHTML = '<div class="tasks-placeholder">Ошибка загрузки</div>';
        }
    }

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    function formatTime(iso) {
        if (!iso) return '';
        const d = new Date(iso);
        return d.toLocaleString('ru-RU', {
            day: '2-digit', month: '2-digit', year: 'numeric',
            hour: '2-digit', minute: '2-digit',
        });
    }

    // =========================================================================
    // Инициализация
    // =========================================================================
    function init() {
        // Форма запуска
        el.form.addEventListener('submit', (e) => {
            e.preventDefault();
            const topic = el.topicInput.value.trim();
            if (!topic) return;
            if (state.isRunning) return;
            startPipeline(topic);
        });

        // Кнопка отмены
        el.cancelBtn.addEventListener('click', cancelPipeline);

        // Очистка консоли
        el.clearConsoleBtn.addEventListener('click', clearConsole);

        // Копирование консоли
        el.copyConsoleBtn.addEventListener('click', copyConsole);

        // Копирование статьи
        if (el.copyArticleBtn) {
            el.copyArticleBtn.addEventListener('click', () => {
                const content = el.previewContent?.querySelector('.markdown-body');
                if (content) {
                    navigator.clipboard.writeText(content.textContent).catch(() => {});
                }
            });
        }

        // Клик по карточке агента — открываем модалку
        document.querySelectorAll('.step-card.clickable').forEach(card => {
            card.addEventListener('click', () => {
                const stage = card.dataset.agent;
                if (stage) openAgentModal(stage);
            });
        });

        // Клик по status-dot Editor — открываем модалку с замечаниями
        const editorDot = document.querySelector('#status-editor .status-dot');
        if (editorDot) {
            editorDot.addEventListener('click', (e) => {
                e.stopPropagation();
                if (state.editorReviewText) {
                    showEditorReviewModal(state.editorReviewText);
                }
            });
            // Делаем курсор pointer для кликабельности
            editorDot.style.cursor = 'pointer';
        }

        // Закрытие модалки
        const modalCloseBtn = document.getElementById('modal-close-btn');
        if (modalCloseBtn) {
            modalCloseBtn.addEventListener('click', closeAgentModal);
        }
        const modalOverlay = document.getElementById('agent-modal');
        if (modalOverlay) {
            modalOverlay.addEventListener('click', (e) => {
                if (e.target === modalOverlay) closeAgentModal();
            });
        }
        // Закрытие Editor Review модалки
        const editorModalCloseBtn = document.getElementById('editor-review-close-btn');
        if (editorModalCloseBtn) {
            editorModalCloseBtn.addEventListener('click', closeEditorReviewModal);
        }
        const editorModalOverlay = document.getElementById('editor-review-modal');
        if (editorModalOverlay) {
            editorModalOverlay.addEventListener('click', (e) => {
                if (e.target === editorModalOverlay) closeEditorReviewModal();
            });
        }

        // Закрытие по Escape (агентская модалка)
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                closeAgentModal();
                closeEditorReviewModal();
                if (reviewModal && reviewModal.style.display === 'flex') {
                    // Не закрываем review modal по Escape — решение обязательно
                }
            }
        });

        // Human Review: live preview при вводе
        if (reviewEditor) {
            reviewEditor.addEventListener('input', () => {
                // Используем requestAnimationFrame для плавности
                if (_debounceTimer) cancelAnimationFrame(_debounceTimer);
                _debounceTimer = requestAnimationFrame(updateReviewPreview);
            });
        }

        // Human Review: кнопка закрытия модалки
        if (reviewCloseBtn) {
            reviewCloseBtn.addEventListener('click', hideReviewModal);
        }

        // Human Review: Approve
        if (reviewApproveBtn) {
            reviewApproveBtn.addEventListener('click', () => submitReview('approved'));
        }

        // Human Review: Revision (доработка)
        if (reviewRevisionBtn) {
            reviewRevisionBtn.addEventListener('click', () => submitReview('revision'));
        }

        // Human Review: Reject (полная остановка)
        if (reviewRejectBtn) {
            reviewRejectBtn.addEventListener('click', () => submitReview('rejected'));
        }

        // Загрузка данных об агентах (кешируются)
        loadAgentData();

        // Загрузка истории задач на странице /history
        if (el.tasksList) {
            loadTasksHistory();
        }

        // Обработка Enter в поле ввода
        el.topicInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                el.form.dispatchEvent(new Event('submit'));
            }
        });

        console.log('[App] Multi-Agent Pipeline UI initialized');
    }

    // Запуск после загрузки DOM
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();
