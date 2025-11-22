from fastapi import FastAPI
from .router import router
app = FastAPI(title='Replication Manager')
app.include_router(router)