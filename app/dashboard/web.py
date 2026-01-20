from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from app.storage.dedup import NewsStorage
import os

app = FastAPI()
storage = NewsStorage()


templates = Jinja2Templates(directory="app/dashboard/templates")

@app.get("/", response_class=HTMLResponse)
async def read_item(request: Request):
    news = storage.get_recent_news(limit=100)
    return templates.TemplateResponse("index.html", {"request": request, "news": news})

@app.get("/api/news")
async def get_news():
    return storage.get_recent_news(limit=100)

@app.get("/api/status")
async def get_status():
    return {
        "status": "online",
        "queues": {
            "relevance": storage.get_queue_length("relevance"),
            "extraction": storage.get_queue_length("extraction")
        },
        "model": os.getenv("GEMINI_MODEL", "unknown")
    }
