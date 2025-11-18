FROM python:3.7-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard_app.py .
COPY template/ template/

EXPOSE 5000

CMD ["python", "dashboard_app.py"]