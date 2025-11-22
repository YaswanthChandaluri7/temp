from fastapi import APIRouter, Body
import os
import httpx
import redis

router = APIRouter()

REDIS_HOST = os.getenv('REDIS_HOST', 'cache')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))

HIGH = int(os.getenv('REPLICATION_THRESHOLD_HIGH', '10'))
LOW = int(os.getenv('REPLICATION_THRESHOLD_LOW', '2'))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
WEBHOOK = os.getenv('WEBHOOK_URL', 'http://webserver:8000')


@router.post("/access/update")
async def access_update(payload: dict = Body(...)):
    """
    Called by webserver whenever a photo is viewed.
    Tracks access count in Redis.
    Triggers replicate_up or replicate_down to webserver.
    """
    photo_id = payload["photo_id"]
    key = f"access:{photo_id}"

    count = r.incr(key)
    r.expire(key, 3600)  # 1 hour access window

    async with httpx.AsyncClient() as client:
        if count >= HIGH:
            await client.post(
                f"{WEBHOOK}/internal/replication/trigger",
                json={
                    "photo_id": photo_id,
                    "action": "replicate_up"
                }
            )

        elif count <= LOW:
            await client.post(
                f"{WEBHOOK}/internal/replication/trigger",
                json={
                    "photo_id": photo_id,
                    "action": "replicate_down"
                }
            )

    return {"status": "ok", "count": count}


@router.post("/replication/trigger")
async def manual_trigger(payload: dict = Body(...)):
    """
    Manually trigger replication operation.
    """
    async with httpx.AsyncClient() as client:
        await client.post(f"{WEBHOOK}/internal/replication/trigger", json=payload)

    return {"status": "triggered"}
