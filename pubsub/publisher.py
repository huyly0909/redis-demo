import asyncio

from redis import asyncio as aioredis


async def publisher():
    r = aioredis.Redis()
    i = 0
    while True:
        await asyncio.sleep(1)  # ➋ timer uses same loop
        await r.publish("events", f"tick {i}")  # ➌ non-blocking
        i += 1

async def publisher2():
    r = aioredis.Redis()
    i = 0
    while True:
        await asyncio.sleep(3)
        await r.publish("logs", f"message {i}")  # ➌ non-blocking
        i += 1

async def main():
    tasks = [
        publisher(),
        publisher2(),
    ]
    await asyncio.gather(*tasks)  # ➏ wait for both to finish


if __name__ == "__main__":
    asyncio.run(main())  # ➐ run the main function
