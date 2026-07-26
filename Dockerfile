FROM python:3.12-slim

WORKDIR /app

# Устанавливаем зависимости
COPY swarm_content_pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект
COPY . .

# Создаём пустую .env для Docker (реальные ключи — через environment)
RUN touch .env

# Порт
EXPOSE 8080

# Запуск FastAPI сервера
CMD ["uvicorn", "swarm_content_pipeline.server:app", "--host", "0.0.0.0", "--port", "8080"]
