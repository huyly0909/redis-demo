from __future__ import annotations

import asyncio
import time
import uuid

from pydantic import BaseModel
from redis import asyncio as aioredis

from consts import RedisQueue
from utils import TarGz
from loguru import logger


class TaskPayload(BaseModel):
    id: str
    data: dict
    attempt: int = 2
    ts: float = time.time()


# ---------------- Push ----------------
async def push(data):
    r = aioredis.Redis()
    with open("data/data.json", 'rb') as video4MB:
        input_file = video4MB.read()
    archive = TarGz.compress_files(
        files=[("data.json", input_file)], archive_name="video_archive", log_prefix="producer"
    )
    payload = TaskPayload(
        id=str(uuid.uuid4()),
        data={
            "archive": archive.model_dump(),
            "num": data,
        },
    )
    await r.rpush(RedisQueue.TASK, payload.model_dump_json())
    logger.info(f"[Producer] Pushed task to queue: {payload.id}")
    await r.aclose()


# ---------------- Demo Runner ----------------
async def demo():
    for n in [1, 5, 2, 5, 3]:
        await push(n)


if __name__ == "__main__":
    asyncio.run(demo())
