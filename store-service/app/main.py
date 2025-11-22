from fastapi import FastAPI
from .router import router

app = FastAPI(title='Store Service')
app.include_router(router)