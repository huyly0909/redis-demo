import asyncio
import json
import os
import sys
import threading
import time
import uuid
from pathlib import Path

from flask import Flask, request, jsonify
from flask_cors import CORS
from loguru import logger
from pydantic import BaseModel, ValidationError
from redis import Redis
from redis import asyncio as aioredis

from consts import RedisQueue
from utils import TarGz

# Configure loguru for better Docker logging
logger.remove()  # Remove default handler
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="DEBUG",
    enqueue=True  # Thread-safe logging
)

app = Flask(__name__)
CORS(app)

# Disable Flask's default logging to avoid duplicate logs
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Redis clients
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = int(os.getenv('REDIS_PORT', '6379'))
# Sync client for Flask routes
redis_client = Redis(host=redis_host, port=redis_port, decode_responses=True)

# Directories
HEALTH_DATA_DIR = Path("health_data")
RESULTS_DIR = Path("results")
HEALTH_DATA_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)


class TaskPayload(BaseModel):
    id: str
    data: dict
    attempt: int = 0
    ts: float = time.time()


class PushTaskRequest(BaseModel):
    num: int


@app.route('/health', methods=['GET'])
def health_check():
    """Basic health check for the API"""
    try:
        redis_client.ping()
        return jsonify({
            "status": "healthy", 
            "redis": "connected",
            "background_thread": background_thread.is_alive() if 'background_thread' in globals() else False
        }), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


@app.route('/api/test/consumers', methods=['GET'])
def test_consumers():
    """Test endpoint to check if background consumers are working"""
    try:
        # Check if health data files exist
        health_files = list(HEALTH_DATA_DIR.glob("*.json"))
        result_files = list(RESULTS_DIR.glob("*.json"))
        
        return jsonify({
            "health_consumer": {
                "health_files_count": len(health_files),
                "latest_health_files": [f.name for f in health_files[:5]]
            },
            "result_consumer": {
                "result_files_count": len(result_files),
                "latest_result_files": [f.name for f in result_files[:5]]
            },
            "background_thread_alive": background_thread.is_alive() if 'background_thread' in globals() else False
        }), 200
    except Exception as e:
        logger.error(f"[API] Error in test consumers endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/api/push', methods=['POST'])
def push_task():
    """Push a task to the appropriate queue based on Python version"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
            
        # Validate input
        task_request = PushTaskRequest(**data)
        
        queue = RedisQueue.TASK
        
        # Create archive (if data file exists)
        archive = None
        data_file_path = Path("data/data.json")
        if data_file_path.exists():
            with open(data_file_path, 'rb') as f:
                input_file = f.read()
            archive = TarGz.compress_files(
                files=[("data.json", input_file)], 
                archive_name="data_archive", 
                log_prefix="API"
            )
        
        # Create task payload
        task_id = str(uuid.uuid4())
        payload = TaskPayload(
            id=task_id,
            data={
                "num": task_request.num,
                "archive": archive.model_dump() if archive else None,
            }
        )
        
        # Push to Redis queue
        redis_client.rpush(queue, payload.model_dump_json())

        logger.info(f"[API] Pushed task {task_id} to queue {queue}")
        
        return jsonify({
            "task_id": task_id,
            "queue": queue,
            "status": "queued"
        }), 201
        
    except ValidationError as e:
        return jsonify({"error": "Invalid input", "details": str(e)}), 400
    except Exception as e:
        logger.error(f"[API] Error pushing task: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/api/workers/health', methods=['GET'])
def get_workers_health():
    """Get health status of all workers"""
    try:
        # Read all health data files
        health_data = {}
        
        for health_file in HEALTH_DATA_DIR.glob("*.json"):
            worker_name = health_file.stem
            try:
                with open(health_file, 'r') as f:
                    worker_health = json.load(f)
                health_data[worker_name] = worker_health
            except Exception as e:
                logger.warning(f"[API] Could not read health data for {worker_name}: {e}")
                health_data[worker_name] = {"error": f"Could not read health data: {e}"}
        
        return jsonify({
            "workers": health_data,
            "total_workers": len(health_data),
            "timestamp": time.time()
        }), 200
        
    except Exception as e:
        logger.error(f"[API] Error getting worker health: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/api/results/<task_id>', methods=['GET'])
def get_task_result(task_id):
    """Get result for a specific task ID"""
    try:
        result_file = RESULTS_DIR / f"{task_id}.json"
        
        if not result_file.exists():
            return jsonify({"error": "Task result not found"}), 404
            
        with open(result_file, 'r') as f:
            result_data = json.load(f)
            
        return jsonify(result_data), 200
        
    except Exception as e:
        logger.error(f"[API] Error getting task result: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/api/results', methods=['GET'])
def list_results():
    """List all available task results"""
    try:
        results = []
        for result_file in RESULTS_DIR.glob("*.json"):
            task_id = result_file.stem
            try:
                with open(result_file, 'r') as f:
                    result_data = json.load(f)
                results.append({
                    "task_id": task_id,
                    "timestamp": result_data.get("ts_done"),
                    "error": result_data.get("error"),
                    "has_error": result_data.get("error") is not None
                })
            except Exception as e:
                logger.warning(f"[API] Could not read result for {task_id}: {e}")
                
        # Sort by timestamp (newest first)
        results.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        
        return jsonify({
            "results": results,
            "total_results": len(results)
        }), 200
        
    except Exception as e:
        logger.error(f"[API] Error listing results: {e}")
        return jsonify({"error": "Internal server error"}), 500


async def _async_consume_health_checks():
    """Async task to consume health check data from Redis"""
    logger.info("[HEALTH-CONSUMER] Starting health check consumer")
    
    # Create async Redis client for this task
    async_redis = aioredis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    try:
        # Test Redis connection
        await async_redis.ping()
        logger.info("[HEALTH-CONSUMER] Redis connection established")
        logger.debug("[HEALTH-CONSUMER] Waiting for health check data...")
        ps = async_redis.pubsub()
        await ps.subscribe(RedisQueue.WORKER_HEALTH_CHECK)
        async for msg in ps.listen():
            print(f"[API] got {RedisQueue.WORKER_HEALTH_CHECK}:", msg)
            if msg["type"] == "message":
                health_data_json = msg["data"]
                health_data = json.loads(health_data_json)

                worker_name = health_data.get("worker_name", "unknown")
                health_file = HEALTH_DATA_DIR / f"{worker_name}.json"

                # Write to file
                with open(health_file, 'w') as f:
                    json.dump(health_data, f, indent=2)

                logger.info(f"[HEALTH-CONSUMER] Updated health data for {worker_name}")
    except Exception as e:
        logger.error(f"[HEALTH-CONSUMER] Fatal error in health consumer: {e}")
    finally:
        logger.info("[HEALTH-CONSUMER] Closing Redis connection")
        await async_redis.aclose()


async def _async_consume_task_results():
    """Async task to consume task results from Redis"""
    logger.info("[RESULT-CONSUMER] Starting task result consumer")
    
    # Create async Redis client for this task
    async_redis = aioredis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    try:
        # Test Redis connection
        await async_redis.ping()
        logger.info("[RESULT-CONSUMER] Redis connection established")
        logger.debug("[RESULT-CONSUMER] Waiting for task results...")
        while True:
            try:
                # Block and wait for task results
                result = await async_redis.brpop([RedisQueue.TASK_RESULT])
                logger.debug("[RESULT-CONSUMER] Received task result from Redis")
                if result:
                    _, result_data_json = result
                    result_data = json.loads(result_data_json)
                    
                    task_id = result_data.get("id", "unknown")
                    result_file = RESULTS_DIR / f"{task_id}.json"
                    
                    # Write result to file
                    with open(result_file, 'w') as f:
                        json.dump(result_data, f, indent=2)
                        
                    logger.info(f"[RESULT-CONSUMER] Saved result for task {task_id}")
                else:
                    logger.debug("[RESULT-CONSUMER] No task results received (timeout)")
                    
            except Exception as e:
                logger.error(f"[RESULT-CONSUMER] Error consuming task results: {e}")
                await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"[RESULT-CONSUMER] Fatal error in result consumer: {e}")
    finally:
        logger.info("[RESULT-CONSUMER] Closing Redis connection")
        await async_redis.aclose()


async def start_server():
    app.run(host='0.0.0.0', port=5000, debug=True)

async def run_background_tasks():
    await asyncio.gather(
        _async_consume_health_checks(),
        _async_consume_task_results()
    )

# Global variable to track background thread
background_thread = None


if __name__ == '__main__':
    background_thread = threading.Thread(target=asyncio.run, args=(run_background_tasks(),), daemon=True)
    background_thread.start()
    app.run(host='0.0.0.0', port=5000, debug=True)
    
