from fastapi import APIRouter, Body, HTTPException
from .engine import StoreEngine

router = APIRouter()
engine = StoreEngine()

@router.post('/store/write')
async def store_write(payload: dict = Body(...)):
    return engine.write(payload)

@router.get('/store/read/{photo_id}')
async def store_read(photo_id: str):
    data = engine.read(photo_id)
    if not data:
        raise HTTPException(status_code=404, detail='not found')
    return data

@router.post('/store/delete/{photo_id}')
async def store_delete(photo_id: str):
    engine.mark_deleted(photo_id)
    return {"status":"marked_deleted"}

@router.post('/store/compact')
async def store_compact():
    engine.compact()
    return {"status":"compaction_started"}