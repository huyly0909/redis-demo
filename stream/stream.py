import asyncio
from redis import asyncio as aioredis
from redis.exceptions import ResponseError

STREAM = "events_stream"
GROUP = "events_group"

async def ensure_group(r):
    """Create the consumer group if it doesn't exist."""
    try:
        # id="$" means start delivering only new entries added *after* group creation.
        await r.xgroup_create(name=STREAM, groupname=GROUP, id="$", mkstream=True)
    except ResponseError as e:
        # BUSYGROUP = already exists; safe to ignore
        if "BUSYGROUP" not in str(e):
            raise

async def producer():
    r = aioredis.Redis()
    # Normally you would reuse the client; keeping it simple for demo
    for i in range(5):
        # print(
            await r.xadd(STREAM, {"n": i})
        # )
    await r.aclose()

async def worker(consumer_name):
    r = aioredis.Redis()
    await ensure_group(r)
    while True:
        # Read 1 new message for this consumer (">" = new messages not yet delivered)
        resp = await r.xreadgroup(
            groupname=GROUP,
            consumername=consumer_name,
            streams={STREAM: ">"},
            count=1,
            block=5000,  # ms; wait up to 5s for new entries
        )
        if not resp:
            continue  # timeout; loop again

        # resp structure: [(stream_name, [(msg_id, {field: val, ...}), ...])]
        for _stream, entries in resp:
            for msg_id, fields in entries:
                try:
                    print(f"[{consumer_name}] processing {msg_id} {fields}")
                    # ... do work ...
                    await asyncio.sleep(3)  # Simulate processing time
                    await r.xack(STREAM, GROUP, msg_id)
                except Exception as exc:
                    # don't XACK on failure; message stays pending for recovery
                    print(f"[{consumer_name}] error {exc}; leaving pending")

async def main():
    await asyncio.gather(worker("s1"), worker("s2"), producer())

if __name__ == "__main__":
    asyncio.run(main())