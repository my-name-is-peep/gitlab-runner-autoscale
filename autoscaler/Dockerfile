FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scaler.py .

RUN useradd -m -u 1000 scaler && chown -R scaler:scaler /app
USER scaler

CMD ["python", "scaler.py"]