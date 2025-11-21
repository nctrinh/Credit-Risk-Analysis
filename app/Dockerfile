FROM python:3.7-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard_app.py .
COPY chart_utils.py .
COPY templates/ templates/

EXPOSE 5000

CMD ["python", "dashboard_app.py"]