import asyncio

from redis import asyncio as aioredis

QUEUE = "events_q"
PROCESSING = "events_q_processing"


async def push(n):
    r = aioredis.Redis()
    await r.rpush(QUEUE, n)
    await r.aclose()


async def main():
    tasks = [
        push('[10, 11, 12, 13]'),
        push(20),
        push(30),
        push(30),
    ]
    await asyncio.gather(*tasks)  # ➏ wait for both to finish


if __name__ == "__main__":
    asyncio.run(main())  # ➐ run the main function
