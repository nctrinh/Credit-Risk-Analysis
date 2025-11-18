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

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FastAPI_App")

# --- Cấu hình biến toàn cục ---
# In-memory storage
predictions_history = deque(maxlen=1000)
latest_predictions = deque(maxlen=10)
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
# Tạo Async Server cho SocketIO
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')

# --- Kafka Consumer Logic (Giữ nguyên logic cũ) ---
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
        # Tạo event loop riêng cho thread này để emit socketio nếu cần (tùy chọn), 
        # nhưng với python-socketio asgi, ta dùng method 'emit' thread-safe của nó.
        while consumer_running:
            try:
                # Poll messages (thay vì loop for message in consumer để kiểm soát vòng lặp tốt hơn)
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
        # Lưu ý: Hàm emit của python-socketio là async, nhưng ta đang ở trong thread đồng bộ.
        # Ta dùng phương thức emit chạy background task hoặc loop.call_soon_threadsafe.
        # Cách đơn giản nhất với python-socketio 5.x+:
        asyncio.run(sio.emit('new_prediction', data, namespace='/'))
        asyncio.run(sio.emit('stats_update', stats, namespace='/'))

    except Exception as e:
        logger.error(f"Error processing message: {e}")

# --- Lifespan Manager (Startup/Shutdown) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Chạy Kafka Consumer Thread
    global consumer_running, consumer_thread
    consumer_running = True
    consumer_thread = threading.Thread(target=kafka_consumer_worker, daemon=True)
    consumer_thread.start()
    logger.info("Kafka consumer thread started via Lifespan")
    
    yield # Ứng dụng chạy tại đây
    
    # Shutdown: Dọn dẹp
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

# CORS (FastAPI middleware)
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
    limit: int = 100,
    label: str = None,
    sort_by: str = 'timestamp',
    sort_order: str = 'desc'
):
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
    
    data = data[:limit]
    return {
        "total": len(predictions_history),
        "filtered": len(data),
        "predictions": data
    }

@app.get("/api/latest")
async def get_latest():
    return {"predictions": list(latest_predictions)}

@app.get("/api/stats")
async def get_stats():
    return stats

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
    # Chạy socket_app thay vì app
    logger.info("Starting FastAPI dashboard server on http://0.0.0.0:5000")
    uvicorn.run(socket_app, host="0.0.0.0", port=5000)