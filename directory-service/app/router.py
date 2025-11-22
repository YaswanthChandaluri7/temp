from fastapi import APIRouter, Body
from .metadata import DirectoryMeta
from fastapi import HTTPException

router = APIRouter()
meta = DirectoryMeta()

@router.post('/directory/upload')
async def directory_upload(payload: dict = Body(...)):
    size = payload.get('photo_size')
    return meta.alloc_replicas(size)

@router.post('/directory/upload/confirm')
async def upload_confirm(payload: dict = Body(...)):
    photo_id = payload['photo_id']
    replicas = payload['replicas']
    meta.confirm_upload(photo_id, replicas)
    return {"status":"metadata_saved"}

@router.get('/directory/fetch/{photo_id}')
async def directory_fetch(photo_id: str):
    entry = meta.get(photo_id)
    if not entry:
        raise HTTPException(status_code=404, detail='not found')
    return entry

@router.delete('/directory/delete/{photo_id}')
async def directory_delete(photo_id: str):
    entry = meta.mark_delete(photo_id)
    if not entry:
        raise HTTPException(status_code=404, detail='not found')
    return entry

@router.post('/directory/delete/confirm')
async def delete_confirm(payload: dict = Body(...)):
    photo_id = payload['photo_id']
    meta.confirm_delete(photo_id)
    return {"status":"metadata_deleted"}

@router.post("/directory/get_free_locations")
async def get_free_locations(payload: dict = Body(...)):
    """
    Return free volumes for replication.
    """
    count = payload.get("count", 1)
    return meta.get_free_locations(count)

@router.post("/directory/add_replicas")
async def add_replicas(payload: dict = Body(...)):
    photo_id = payload['photo_id']
    new_replicas = payload['replicas']
    meta.add_replicas(photo_id, new_replicas)
    return {"status": "added"}

@router.post("/directory/remove_replicas")
async def remove_replicas(payload: dict = Body(...)):
    photo_id = payload['photo_id']
    # Directory decides which replicas to remove
    removed = meta.remove_half_replicas(photo_id)
    return {"status": "removed", "replicas": removed}