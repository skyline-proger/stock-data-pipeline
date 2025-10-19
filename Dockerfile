# 1. Используем официальный Python-образ
FROM python:3.13-slim

# 2. Устанавливаем зависимости системы (для psycopg2)
RUN apt-get update && apt-get install -y libpq-dev gcc

# 3. Копируем файлы проекта
WORKDIR /app
COPY . /app

# 4. Устанавливаем Python-зависимости
RUN pip install --no-cache-dir -r requirements.txt

# 5. Задаем команду по умолчанию
CMD ["python", "main.py"]
