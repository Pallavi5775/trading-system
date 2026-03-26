# app/main.py

from fastapi import FastAPI
from api.routes import router

app = FastAPI(title="Market Data Service")

app.include_router(router)