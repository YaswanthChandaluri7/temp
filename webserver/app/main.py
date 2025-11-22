from fastapi import FastAPI
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from .router import router

app = FastAPI(title="Web Server")
app.include_router(router)

# Serve static folder
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Redirect root to index.html
@app.get("/")
def read_root():
    return RedirectResponse(url="/static/index.html")