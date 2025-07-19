import asyncio
import time
from typing import Tuple, Type, Self

from pydantic import BaseModel, ValidationError
from redis import asyncio as aioredis

from consts import RedisQueue, REDIS_MAX_RETRIES


# Define which exceptions should trigger a retry
class TemporaryError(Exception): ...


class DownstreamTimeout(Exception): ...


RETRYABLE_EXC: Tuple[Type[Exception], ...] = (TemporaryError, DownstreamTimeout)


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
            ts_done=time.time(),
            error=error
        )


# ---------------- Your Business Logic ----------------
async def process_job(job: Job) -> JobResult:
    """
    Do work here. Raise TemporaryError/DownstreamTimeout to trigger retry.
    Anything else is treated as fatal (goes to DEAD).
    """
    # Demo logic: fail first 2 times if data == 5
    if job.data['num'] == 5 and job.attempt < 2:
        raise TemporaryError("Process transient failure")

    # Processing
    return JobResult.of(
        job=job,
        data={
            "num_x2": job.data['num'] * 2,
            "archive": job.data['archive'] if 'archive' in job.data else None
        }
    )


# ---------------- Worker ----------------
async def worker(name):
    r = aioredis.Redis()
    while True:
        raw: str = await r.brpoplpush(RedisQueue.TASK, RedisQueue.TASK_PROCESSING)
        try:
            job = Job.model_validate_json(raw)
        except ValidationError as e:
            await r.lrem(RedisQueue.TASK_PROCESSING, 1, raw)
            print(f"[{name}] Skip - Invalid job format: {e}")
            continue

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


# ---------------- Ack Helpers ----------------
async def ack_success(r: aioredis.Redis, raw: str | bytes, job: JobResult, worker_name: str):
    # Remove from PROCESSING
    pipe = r.pipeline(transaction=True)
    _ = await pipe.lrem(RedisQueue.TASK_PROCESSING, 1, raw)
    print(f"[{worker_name}] ACK success id={job.id}")
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
    print(
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
    print(
        f"[{worker_name}] FATAL id={job.id} moved to DEAD removed_processing={bool(removed_processing)} reason={fatal_reason}")


STALE_AFTER = 5


def need_requeue(job: Job) -> bool:
    now = time.time()
    if job.ts_retry:
        return now - job.ts_retry > STALE_AFTER
    return now - job.ts > STALE_AFTER * 10


# ---------------- Reaper (optional but recommended) ----------------
async def reaper():
    """
    Periodically scan PROCESSING for stuck jobs (worker crashed mid-processing),
    and requeue them if they have not exceeded REDIS_MAX_RETRIES.
    """
    r = aioredis.Redis()
    while True:
        items: list[str | bytes] = await r.lrange(RedisQueue.TASK_PROCESSING, 0, -1)
        invalid_jobs = []
        for raw in items:
            try:
                job = Job.model_validate_json(raw)
            except ValidationError as e:
                print(f"[Reaper] Skip... Invalid job format: {e}")
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
                print(f"[Reaper] Remove invalid job: {invalid_job[:20]}...")
            await pipe.execute()
        # Sleep for a while before next scan
        await asyncio.sleep(STALE_AFTER // 2 or 1)


# ---------------- Demo Runner ----------------
async def main():
    await asyncio.gather(
        worker("w1"),
        worker("w2"),
        reaper()
    )


if __name__ == "__main__":
    asyncio.run(main())
