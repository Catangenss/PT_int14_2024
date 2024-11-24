FROM python:3.11-slim

RUN apt-get update && apt-get install -y sqlite3

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py /app/main.py
COPY tests.py /app/tests.py
COPY pt_int14.db /app/pt_int14.db

WORKDIR /app

ENTRYPOINT ["python3", "main.py"]
