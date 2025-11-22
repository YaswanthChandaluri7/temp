from fastapi import APIRouter, Body, UploadFile, File, HTTPException
import os
import httpx
import base64
import redis

router = APIRouter()

DIR_SVC = os.getenv('DIR_SVC','http://directory-service:8001')
STORE_SVC = os.getenv('STORE_SVC','http://store-service:8002')
RM_SVC = os.getenv('RM_SVC','http://replication-manager:8003')
REDIS_HOST = os.getenv('REDIS_HOST','cache')
REDIS_PORT = int(os.getenv('REDIS_PORT','6379'))

r = redis.Redis(host=REDIS_HOST,port=REDIS_PORT,decode_responses=True)

@router.post('/upload')
async def upload(payload: dict = Body(...)):
    # 1. RM update
    async with httpx.AsyncClient() as client:
        await client.post(f"{RM_SVC}/access/update", json={"photo_id":"upload-temp","access_type":"upload"})
    # 2. Directory allocate
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{DIR_SVC}/directory/upload", json={"photo_size": payload['photo_size']})
        j = resp.json()
    photo_id = j['photo_id']
    replicas = j['replica_locations']
    # 3. write to stores
    write_results = []
    async with httpx.AsyncClient() as client:
        for rloc in replicas:
            body = {"photo_id": photo_id, "volume_id": rloc['volume'], "photo_data": payload['data'], "cookie":"c"}
            w = await client.post(f"{STORE_SVC}/store/write", json=body)
            write_results.append(w.json())
    # 4. confirm directory
    async with httpx.AsyncClient() as client:
        await client.post(f"{DIR_SVC}/directory/upload/confirm", json={"photo_id":photo_id,"replicas":replicas})
    return {"photo_id":photo_id,"replicas":replicas,"status":"uploaded"}

@router.get('/photo/{photo_id}')
async def fetch(photo_id: str):
    # 1. RM update
    async with httpx.AsyncClient() as client:
        await client.post(f"{RM_SVC}/access/update", json={"photo_id":photo_id,"access_type":"fetch"})
    # 2. Directory fetch
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{DIR_SVC}/directory/fetch/{photo_id}")
        if resp.status_code != 200:
            raise HTTPException(status_code=404, detail='not found')
        entry = resp.json()
    # 3. try cache
    cached = r.get(f"photo:{photo_id}")
    if cached:
        return {"photo_id":photo_id,"data":cached,"source":"cache"}
    # 4. read from first replica
    replicas = entry['replicas']
    # For simplicity, ask store service for the id
    async with httpx.AsyncClient() as client:
        store_resp = await client.get(f"{STORE_SVC}/store/read/{photo_id}")
        if store_resp.status_code != 200:
            raise HTTPException(status_code=404, detail='not found in store')
        data = store_resp.json()['data']
    
    # cache result
    r.set(f"photo:{photo_id}", data, ex=300)

    #inform replicate manager
    async with httpx.AsyncClient() as client:
        await client.post(
            "http://replication-manager:8003/access/update",
            json={"photo_id": photo_id}
        )

    return {"photo_id":photo_id,"data":data,"source":"store"}

@router.delete('/photo/{photo_id}')
async def delete(photo_id: str):
    async with httpx.AsyncClient() as client:
        await client.post(f"{RM_SVC}/access/update", json={"photo_id":photo_id,"access_type":"delete"})
        # directory returns replicas
        resp = await client.delete(f"{DIR_SVC}/directory/delete/{photo_id}")
        if resp.status_code != 200:
            raise HTTPException(status_code=404, detail='not found')
        entry = resp.json()
        replicas = entry['replicas']
        # ask store to delete
        for rloc in replicas:
            await client.post(f"{STORE_SVC}/store/delete/{photo_id}")
        # confirm directory delete
        await client.post(f"{DIR_SVC}/directory/delete/confirm", json={"photo_id":photo_id})
    # drop cache
    r.delete(f"photo:{photo_id}")
    return {"photo_id":photo_id,"status":"deleted"}


# @router.post("/internal/replication/trigger")
# async def handle_replication(payload: dict = Body(...)):
@router.post("/internal/replication/trigger")
async def handle_replication(payload: dict = Body(...)):
    """
    Handle replication triggered by replication-manager.
    Webserver is responsible for actual replication logic.
    """
    photo_id = payload["photo_id"]
    action = payload["action"]

    async with httpx.AsyncClient() as client:
        # 1️⃣ Get current and free locations from directory
        resp = await client.get(f"{DIR_SVC}/directory/fetch/{photo_id}")
        if resp.status_code != 200:
            return {"status": "error", "reason": "photo not found"}
        entry = resp.json()
        replicas = entry["replicas"]

        if action == "replicate_up":
            # Directory should provide a new free location
            resp_free = await client.post(f"{DIR_SVC}/directory/get_free_locations", json={"count": 1})
            new_locations = resp_free.json().get("locations", [])

            if not new_locations:
                return {"status": "error", "reason": "no free locations"}

            # Fetch from any current replica
            source = replicas[0]
            store_resp = await client.get(f"{STORE_SVC}/store/read/{photo_id}")
            if store_resp.status_code != 200:
                return {"status": "error", "reason": "failed reading from store"}
            data = store_resp.json()["data"]

            # Write to new locations
            write_results = []
            for loc in new_locations:
                body = {"photo_id": photo_id, "volume_id": loc['volume'], "photo_data": data, "cookie": "c"}
                w = await client.post(f"{STORE_SVC}/store/write", json=body)
                write_results.append(w.json())

            # Notify directory of new replicas
            await client.post(f"{DIR_SVC}/directory/add_replicas", json={"photo_id": photo_id, "replicas": new_locations})

            return {"status": "replicate_up_done", "new_replicas": new_locations}

        elif action == "replicate_down":
            # Remove half replicas
            resp_remove = await client.post(f"{DIR_SVC}/directory/remove_replicas", json={"photo_id": photo_id})
            remove_info = resp_remove.json()
            to_remove = remove_info.get("replicas", [])

            # Delete from store
            for rloc in to_remove:
                await client.post(f"{STORE_SVC}/store/delete/{photo_id}", json={"volume_id": rloc['volume']})

            return {"status": "replicate_down_done", "removed_replicas": to_remove}

    return {"status": "unknown_action"}
