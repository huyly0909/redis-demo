import asyncio
import json

from loguru import logger
from redis import asyncio as aioredis

from consts import RedisQueue
from utils import TarGz


# ---------------- Worker ----------------
async def result():
    r = aioredis.Redis()
    while True:
        res: bytes
        _, res = await r.brpop([RedisQueue.TASK_RESULT])  # queue_name, result bytes
        logger.info(f"[Consumer] Get result: {res[:50]}...")
        json_result = json.loads(res)
        data = json_result['data']
        if archive := data.get('archive'):
            TarGz.extract(archive['content'], archive['codec'], save_dir="results", log_prefix="consumer")


# ---------------- Demo Runner ----------------
async def main():
    await asyncio.gather(
        result(),
    )


if __name__ == "__main__":
    asyncio.run(main())
