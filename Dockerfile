FROM python:3.12.7-slim

WORKDIR /app

COPY requirements.txt .

RUN python3 -m --no-cache-dir pip install -r requirements.txt

COPY src/ .

CMD ["python", "main.py"]
