import asyncio
import json
import os
import platform
import random
import psutil
import socket
import sys
import time
from typing import Tuple, Type, Self
from loguru import logger

from pydantic import BaseModel, ValidationError
from redis import asyncio as aioredis

from consts import RedisQueue, REDIS_MAX_RETRIES


# Define which exceptions should trigger a retry
class TemporaryError(Exception): ...


class DownstreamTimeout(Exception): ...


RETRYABLE_EXC: Tuple[Type[Exception], ...] = (TemporaryError, DownstreamTimeout)

# Global state for tracking processing tasks
PROCESSING_TASKS = set()


class Job(BaseModel):
    id: str
    data: dict
    max_attempts: int = 3
    attempt: int = 0
    ts: float = time.time()
    ts_retry: float | None = None


class JobResult(Job):
    ts_done: float = time.time()
    error: str | None = None

    @classmethod
    def of(cls, *, job: Job, data: dict | None = None, error: str | None = None) -> Self:
        return cls(
            id=job.id,
            data=data or {},
            attempt=job.attempt,
            ts=job.ts,
            ts_retry=job.ts_retry,
            ts_done=time.time(),
            error=error
        )


class WorkerHealth(BaseModel):
    worker_name: str
    python_version: str
    timestamp: float
    health_status: str  # "healthy", "busy", "error"
    cpu_cores: int
    cpu_percent: float
    memory_total: int  # in bytes
    memory_used: int   # in bytes
    memory_percent: float
    disk_total: int    # in bytes
    disk_used: int     # in bytes
    disk_percent: float
    is_busy: bool
    processing_tasks: list[str]  # list of task IDs currently processing
    hostname: str


# ---------------- Health Check Functions ----------------
def get_system_metrics() -> dict:
    """Get current system metrics"""
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    return {
        "cpu_cores": psutil.cpu_count(),
        "cpu_percent": psutil.cpu_percent(interval=1),
        "memory_total": memory.total,
        "memory_used": memory.used,
        "memory_percent": memory.percent,
        "disk_total": disk.total,
        "disk_used": disk.used,
        "disk_percent": (disk.used / disk.total) * 100,
        "hostname": socket.gethostname()
    }


async def send_health_check(r: aioredis.Redis, worker_name: str):
    """Send health check data to Redis"""
    try:
        metrics = get_system_metrics()
        health = WorkerHealth(
            worker_name=worker_name,
            python_version=f"{sys.version_info.major}.{sys.version_info.minor}",
            timestamp=time.time(),
            health_status="busy" if PROCESSING_TASKS else "healthy",
            is_busy=bool(PROCESSING_TASKS),
            processing_tasks=list(PROCESSING_TASKS),
            **metrics
        )
        await r.publish(RedisQueue.WORKER_HEALTH_CHECK, health.model_dump_json())
    except Exception as e:
        logger.error(f"[{worker_name}] Health check failed: {e}")


async def health_check_worker(worker_name: str):
    """Background task to send health checks every 10 seconds"""
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', '6379'))
    r = aioredis.Redis(host=redis_host, port=redis_port)
    logger.info(f"[{worker_name}] Health check worker starting...")
    while True:
        await send_health_check(r, worker_name)
        await asyncio.sleep(10)


# ---------------- Your Business Logic ----------------
async def process_job(job: Job) -> JobResult:
    """
    Do work here. Raise TemporaryError/DownstreamTimeout to trigger retry.
    Anything else is treated as fatal (goes to DEAD).
    """
    # Demo logic: fail first 2 times if data == 5
    if job.data['num'] == 5 and job.attempt < 2:
        raise TemporaryError("Process transient failure: 5")

    if job.data['num'] == 6:
        raise TemporaryError("Process transient failure: 6")

    if job.data['num'] == 7:
        raise ValueError("Value error: 7")

    # Processing
    return JobResult.of(
        job=job,
        data={
            "num_x2": job.data['num'] * 2,
            "archive": job.data['archive'] if 'archive' in job.data else None
        }
    )


async def worker(name):
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', '6379'))
    r = aioredis.Redis(host=redis_host, port=redis_port)
    logger.info(f"[{name}] Worker starting on Python {sys.version_info.major}.{sys.version_info.minor}, consuming from {RedisQueue.TASK}")
    
    while True:
        raw: str = await r.brpoplpush(RedisQueue.TASK, RedisQueue.TASK_PROCESSING, timeout=60*60)
        if not raw:
            logger.info(f"[{name}] No tasks found")
            continue
        try:
            job = Job.model_validate_json(raw)
        except ValidationError as e:
            await r.lrem(RedisQueue.TASK_PROCESSING, 1, raw)
            logger.info(f"[{name}] Skip - Invalid job format: {e}")
            continue

        # Track that we're processing this job
        PROCESSING_TASKS.add(job.id)
        
        try:
            result = await process_job(job)
        except RETRYABLE_EXC as e:
            # Retryable failure
            await ack_and_requeue(r, raw, job, name, retry_reason=str(e))
        except Exception as e:
            # Non-retryable failure → dead letter
            await ack_to_dead(r, raw, job, name, fatal_reason=str(e))
        else:
            # Success → ack
            await ack_success(r, raw, result, name)
        finally:
            # Remove from processing tasks
            PROCESSING_TASKS.discard(job.id)


# ---------------- Ack Helpers ----------------
async def ack_success(r: aioredis.Redis, raw: str | bytes, job: JobResult, worker_name: str):
    # Remove from PROCESSING
    pipe = r.pipeline(transaction=True)
    _ = await pipe.lrem(RedisQueue.TASK_PROCESSING, 1, raw)
    logger.info(f"[{worker_name}] ACK success id={job.id}")
    await pipe.rpush(RedisQueue.TASK_RESULT, job.model_dump_json())
    await pipe.execute()


async def ack_and_requeue(r: aioredis.Redis, raw: str | bytes, job: Job, worker_name: str, retry_reason: str):
    # Remove from PROCESSING + requeue (atomic via MULTI/EXEC)
    job.attempt += 1
    pipe = r.pipeline(transaction=True)
    await pipe.lrem(RedisQueue.TASK_PROCESSING, 1, raw)
    if job.attempt <= REDIS_MAX_RETRIES:
        job.ts_retry = time.time()
        # requeue with new raw
        await pipe.rpush(RedisQueue.TASK, job.model_dump_json())
    else:
        failed_result = JobResult.of(
            job=job,
            error=retry_reason,
        )
        # push failed result to Result queue
        await pipe.rpush(RedisQueue.TASK_RESULT, failed_result.model_dump_json())
    removed_processing, _ = await pipe.execute()
    status = "RETRY" if job.attempt <= REDIS_MAX_RETRIES else "FAILED"
    logger.info(
        f"[{worker_name}] {status} id={job.id} attempt={job.attempt} reason={retry_reason} removed_processing={bool(removed_processing)}")


async def ack_to_dead(r: aioredis.Redis, raw: str | bytes, job: Job, worker_name: str, fatal_reason: str):
    pipe = r.pipeline(transaction=True)
    await pipe.lrem(RedisQueue.TASK_PROCESSING, 1, raw)
    result = JobResult.of(
        job=job,
        error=fatal_reason,
    )
    # push failed result to Result queue
    await pipe.rpush(RedisQueue.TASK_RESULT, result.model_dump_json())
    removed_processing, _ = await pipe.execute()
    logger.info(
        f"[{worker_name}] FATAL id={job.id} moved to DEAD removed_processing={bool(removed_processing)} reason={fatal_reason}")


STALE_AFTER = 5


def need_requeue(job: Job) -> bool:
    now = time.time()
    if job.ts_retry:
        return now - job.ts_retry > STALE_AFTER
    return now - job.ts > STALE_AFTER * 10


# ---------------- Reaper (optional but recommended) ----------------
async def reaper(worker_name: str):
    """
    Periodically scan PROCESSING for stuck jobs (worker crashed mid-processing),
    and requeue them if they have not exceeded REDIS_MAX_RETRIES.
    """
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', '6379'))
    r = aioredis.Redis(host=redis_host, port=redis_port)
    logger.info(f"[{worker_name}] Starting reaper...")
    while True:
        items: list[str | bytes] = await r.lrange(RedisQueue.TASK_PROCESSING, 0, -1)
        invalid_jobs = []
        for raw in items:
            try:
                job = Job.model_validate_json(raw)
            except ValidationError as e:
                logger.info(f"[Reaper] Skip... Invalid job format: {e}")
                invalid_jobs.append(raw)
                continue

            if need_requeue(job):
                # timed out: treat as retryable
                await ack_and_requeue(r, raw.decode("utf-8"), job, worker_name="reaper",
                                      retry_reason="[stale] Requeued by reaper")

        # Kill invalid/exception/cannot execute jobs
        if invalid_jobs:
            pipe = await r.pipeline(transaction=True)
            for invalid_job in invalid_jobs:
                await pipe.lrem(RedisQueue.TASK_PROCESSING, 1, invalid_job)
                logger.info(f"[Reaper] Remove invalid job: {invalid_job[:20]}...")
            await pipe.execute()
        # Sleep for a while before next scan
        await asyncio.sleep(STALE_AFTER // 2 or 1)


# ---------------- Demo Runner ----------------
async def main():
    worker_name = os.getenv("WORKER_NAME", f"worker-{random.randint(1000, 9999)}")
    tasks = [
        worker(worker_name),
        health_check_worker(worker_name),
    ]
    if os.getenv("ENABLE_REAPER", False):
        tasks.append(reaper(worker_name))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
