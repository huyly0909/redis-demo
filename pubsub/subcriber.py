import asyncio
from redis import asyncio as aioredis


async def subscriber():
    r = aioredis.Redis()
    ps = r.pubsub()
    await ps.subscribe("events")
    async for msg in ps.listen():  # ➊ yields automatically
        if msg["type"] == "message":
            print("[s1] got:", msg["data"])

async def subscriber2():
    r = aioredis.Redis()
    ps = r.pubsub()
    await ps.subscribe("events")
    async for msg in ps.listen():  # ➊ yields automatically
        if msg["type"] == "message":
            print("[s2] got:", msg["data"])


async def subscriber3():
    r = aioredis.Redis()
    ps = r.pubsub()
    await ps.subscribe("logs")
    async for msg in ps.listen():  # ➊ yields automatically
        if msg["type"] == "message":
            print("[s2] got:", msg["data"])


async def main():
    tasks = [
        subscriber(),  # ➍ run subscriber in background
        subscriber2(),
        subscriber3()
    ]
    await asyncio.gather(*tasks)  # ➏ wait for both to finish


if __name__ == "__main__":
    asyncio.run(main())  # ➐ run the main function
