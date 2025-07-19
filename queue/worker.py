import asyncio
from redis import asyncio as aioredis

QUEUE = "events_q"
PROCESSING = "events_q_processing"

async def worker(name):
    r = aioredis.Redis()
    while True:
        # Atomically pop from QUEUE tail, push to PROCESSING head; block forever
        item = await r.brpoplpush(QUEUE, PROCESSING, timeout=0)
        if item is None:
            continue  # shouldn't happen with timeout=0
        try:
            print(f"[{name}] got {item}")
            await asyncio.sleep(3)  # Simulate processing time
            # ... process ...
            # Remove from PROCESSING after success
            await r.lrem(PROCESSING, 1, item)
        except Exception as ex:
            print(f"[{name}] error processing {item}: {ex}")

async def main():
    tasks = [
        worker("worker1"),  # ➊ run worker in background
        worker("worker2"),  # ➊ run worker in background
    ]
    await asyncio.gather(*tasks)  # ➏ wait for both to finish

if __name__ == "__main__":
    asyncio.run(main())  # ➐ run the main function
