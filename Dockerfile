FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python

COPY app.py .

CMD ["python", "-u", "app.py"]