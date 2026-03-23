FROM python:3.11-slim

WORKDIR /app

# Получаем GID группы docker с хоста при сборке (или ставим стандартный 999)
ARG DOCKER_GID=986

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scaler.py .

# Создаем группу docker и добавляем пользователя
RUN groupadd -g ${DOCKER_GID} docker && \
    useradd -m -u 1000 -g docker scaler && \
    chown -R scaler:docker /app

USER scaler

CMD ["python", "scaler.py"]