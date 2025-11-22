from fastapi import FastAPI, HTTPException
from .router import router

app = FastAPI(title="Directory Service")
app.include_router(router)