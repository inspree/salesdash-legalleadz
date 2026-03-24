FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cache bust: 2026-03-24-v8
ENV PORT=8090
EXPOSE 8090

CMD ["python", "billing_server.py"]
