import socketio
from fastapi import FastAPI, Request, Query
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
import json
import threading
from datetime import datetime
from collections import deque
import time
import logging
import asyncio
import numpy as np
import pandas as pd
from statistics import mean, stdev

# Import chart utilities
try:
    from chart_utils import (
        plot_loan_amount_distribution,
        plot_risk_score_distribution,
        plot_grade_distribution,
    )
except ImportError:
    print("Warning: chart_utils not found. Chart functions will not be available.")
    # Fallback functions nếu chart_utils không tồn tại
    def plot_loan_amount_distribution(df): return {"labels": [], "data": []}
    def plot_risk_score_distribution(df): return {"labels": [], "data": []}
    def plot_grade_distribution(df): return {"labels": [], "data": []}

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FastAPI_App")

# --- Cấu hình biến toàn cục ---
# In-memory storage
predictions_history = deque(maxlen=10000)  # Tăng từ 1000 lên 10000
latest_predictions = deque(maxlen=5)
stats = {
    "total_predictions": 0,
    "good_loans": 0,
    "bad_loans": 0,
    "avg_risk_score": 0.0,
    "total_loan_amount": 0.0
}

# Kafka Connection Settings
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092', 'kafka:9092', 'localhost:9092']
KAFKA_TOPIC = 'loanPredictions'
MAX_RETRIES = 60
RETRY_DELAY = 5

# Thread control
consumer_running = False
consumer_thread = None

# --- Setup Socket.IO ---
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')

# --- Kafka Consumer Logic ---
def create_kafka_consumer():
    """Tạo Kafka consumer với retry logic"""
    for server in KAFKA_BOOTSTRAP_SERVERS:
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Connecting to Kafka at {server} (attempt {attempt + 1}/{MAX_RETRIES})...")
                consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=[server],
                    auto_offset_reset='latest',
                    group_id='dashboard_consumers',
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    request_timeout_ms=30000,
                    session_timeout_ms=10000,
                    heartbeat_interval_ms=3000,
                    api_version_auto_timeout_ms=10000
                )
                logger.info(f"Kafka consumer connected successfully to {server}!")
                return consumer
            except (NoBrokersAvailable, KafkaError, Exception) as e:
                logger.warning(f"Error connecting to {server}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
    return None

def kafka_consumer_worker():
    """Worker thread để consume Kafka messages"""
    global consumer_running, stats
    logger.info("Starting Kafka consumer worker...")
    
    consumer = create_kafka_consumer()
    if consumer is None:
        logger.error("Failed to create Kafka consumer.")
        return

    try:
        while consumer_running:
            try:
                msg_pack = consumer.poll(timeout_ms=1000)
                
                for tp, messages in msg_pack.items():
                    for message in messages:
                        if not consumer_running: break
                        
                        data = message.value
                        process_message(data)
                        
            except Exception as e:
                logger.error(f"Error in consumption loop: {e}")
                time.sleep(1)
    finally:
        consumer.close()
        logger.info("Kafka consumer stopped")

def process_message(data):
    """Xử lý data và emit socket"""
    global stats
    try:
        # Add timestamp
        data['timestamp'] = datetime.now().isoformat()
        data['timestamp_display'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Add to history
        predictions_history.append(data)
        latest_predictions.append(data)
        
        # Update stats
        stats["total_predictions"] += 1
        if data.get("label") == "Good":
            stats["good_loans"] += 1
        elif data.get("label") == "Bad":
            stats["bad_loans"] += 1
        
        if data.get("risk_score") is not None:
            total = stats["total_predictions"]
            old_avg = stats["avg_risk_score"]
            stats["avg_risk_score"] = (old_avg * (total - 1) + data["risk_score"]) / total
        
        if data.get("loan_amnt"):
            stats["total_loan_amount"] += data["loan_amnt"]
        
        logger.info(f"New prediction: ID={data.get('id')}, Label={data.get('label')}")

        # Broadcast to clients via SocketIO
        asyncio.run(sio.emit('new_prediction', data, namespace='/'))
        asyncio.run(sio.emit('stats_update', stats, namespace='/'))

    except Exception as e:
        logger.error(f"Error processing message: {e}")

# --- Chart Data Calculation Functions ---

def get_loan_amount_distribution(bins=10):
    """Tính phân phối số tiền vay"""
    if not predictions_history:
        return {"labels": [], "data": []}
    
    amounts = [p.get("loan_amnt", 0) for p in predictions_history if p.get("loan_amnt")]
    if not amounts:
        return {"labels": [], "data": []}
    
    min_val, max_val = min(amounts), max(amounts)
    bin_edges = np.linspace(min_val, max_val, bins + 1)
    hist, _ = np.histogram(amounts, bins=bin_edges)
    
    labels = [f"${int(bin_edges[i]):,}-${int(bin_edges[i+1]):,}" for i in range(len(bin_edges)-1)]
    
    return {
        "labels": labels,
        "data": [int(h) for h in hist]
    }

def get_risk_score_distribution(bins=10):
    """Tính phân phối điểm rủi ro"""
    if not predictions_history:
        return {"labels": [], "data": []}
    
    scores = [p.get("risk_score", 0) for p in predictions_history if p.get("risk_score") is not None]
    if not scores:
        return {"labels": [], "data": []}
    
    hist, bin_edges = np.histogram(scores, bins=bins, range=(0, 1))
    labels = [f"{bin_edges[i]:.1f}-{bin_edges[i+1]:.1f}" for i in range(len(bin_edges)-1)]
    
    return {
        "labels": labels,
        "data": [int(h) for h in hist]
    }

def get_label_distribution():
    """Tính phân phối Good/Bad loans"""
    total = stats["total_predictions"]
    if total == 0:
        return {"labels": [], "data": []}
    
    return {
        "labels": ["Good Loans", "Bad Loans"],
        "data": [stats["good_loans"], stats["bad_loans"]],
        "colors": ["#10b981", "#ef4444"]
    }

def get_grade_distribution():
    """Tính phân phối theo grade (A, B, C, ...)"""
    if not predictions_history:
        return {"labels": [], "data": []}
    
    grades = {}
    for p in predictions_history:
        grade = p.get("grade", "N/A")
        grades[grade] = grades.get(grade, 0) + 1
    
    sorted_grades = sorted(grades.items())
    return {
        "labels": [g[0] for g in sorted_grades],
        "data": [g[1] for g in sorted_grades]
    }

def get_repayment_efficiency_distribution():
    """Tính hiệu quả hoàn trả"""
    if not predictions_history:
        return {"labels": [], "data": []}
    
    efficiencies = []
    for p in predictions_history:
        total_pymnt = p.get("total_pymnt", 1)
        total_rec_prncp = p.get("total_rec_prncp", 0)
        if total_pymnt > 0:
            eff = total_rec_prncp / total_pymnt
            efficiencies.append(round(eff, 1))
    
    if not efficiencies:
        return {"labels": [], "data": []}
    
    efficiency_counts = {}
    for eff in efficiencies:
        efficiency_counts[eff] = efficiency_counts.get(eff, 0) + 1
    
    sorted_eff = sorted(efficiency_counts.items())
    return {
        "labels": [str(e[0]) for e in sorted_eff],
        "data": [e[1] for e in sorted_eff]
    }

def get_status_distribution():
    """Tính phân phối trạng thái khoản vay"""
    if not predictions_history:
        return {"labels": [], "data": []}
    
    statuses = {}
    for p in predictions_history:
        status = p.get("status", "N/A")
        statuses[status] = statuses.get(status, 0) + 1
    
    sorted_status = sorted(statuses.items(), key=lambda x: x[1], reverse=True)
    return {
        "labels": [s[0] for s in sorted_status],
        "data": [s[1] for s in sorted_status]
    }

# --- Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global consumer_running, consumer_thread
    consumer_running = True
    consumer_thread = threading.Thread(target=kafka_consumer_worker, daemon=True)
    consumer_thread.start()
    logger.info("Kafka consumer thread started via Lifespan")
    
    yield
    
    # Shutdown
    logger.info("Shutting down, stopping consumer...")
    consumer_running = False
    if consumer_thread:
        consumer_thread.join(timeout=5)
    logger.info("Cleanup complete")

# --- Setup FastAPI ---
app = FastAPI(lifespan=lifespan)

# Wrap FastAPI app bằng SocketIO app
socket_app = socketio.ASGIApp(sio, app)

# Templates
templates = Jinja2Templates(directory="templates")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API Endpoints ---

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "kafka_connected": consumer_thread is not None and consumer_thread.is_alive(),
        "total_predictions": stats["total_predictions"]
    }

@app.get("/api/predictions")
async def get_predictions(
    page: int = 1,
    limit: int = 50,
    label: str = None,
    sort_by: str = 'timestamp',
    sort_order: str = 'desc'
):
    """API với pagination"""
    data = list(predictions_history)
    
    # Filter
    if label and label != 'all':
        data = [p for p in data if p.get('label') == label]
    
    # Sort
    reverse = (sort_order == 'desc')
    if sort_by == 'timestamp':
        data.sort(key=lambda x: x.get('timestamp', ''), reverse=reverse)
    elif sort_by == 'risk_score':
        data.sort(key=lambda x: x.get('risk_score', 0), reverse=reverse)
    elif sort_by == 'loan_amnt':
        data.sort(key=lambda x: x.get('loan_amnt', 0), reverse=reverse)
    elif sort_by == 'id':
        data.sort(key=lambda x: x.get('id', 0), reverse=reverse)
    
    # Pagination
    total = len(data)
    skip = (page - 1) * limit
    data = data[skip:skip + limit]
    
    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total + limit - 1) // limit,
        "predictions": data
    }

@app.get("/api/latest")
async def get_latest():
    return {"predictions": list(latest_predictions)}

@app.get("/api/stats")
async def get_stats():
    return stats

# --- Chart Data Endpoints ---

@app.get("/api/charts/loan-distribution")
async def chart_loan_distribution():
    return get_loan_amount_distribution()

@app.get("/api/charts/label-distribution")
async def chart_label_distribution():
    return get_label_distribution()

@app.get("/api/charts/grade-distribution")
async def chart_grade_distribution():
    return get_grade_distribution()

# --- Socket.IO Events ---

@sio.event
async def connect(sid, environ):
    logger.info(f'Client connected: {sid}')
    await sio.emit('stats_update', stats, to=sid)
    await sio.emit('latest_predictions', list(latest_predictions), to=sid)

@sio.event
async def disconnect(sid):
    logger.info(f'Client disconnected: {sid}')

@sio.event
async def request_history(sid, data):
    limit = data.get('limit', 50)
    history = list(predictions_history)[:limit]
    await sio.emit('history_data', history, to=sid)

# --- Entry Point ---
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI dashboard server on http://0.0.0.0:5000")
    uvicorn.run(socket_app, host="0.0.0.0", port=5000)