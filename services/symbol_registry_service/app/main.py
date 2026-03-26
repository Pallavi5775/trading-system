
from fastapi import FastAPI
from app.routes import router
from app.models import Base
from app.database import engine

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Symbol Registry Service")

app.include_router(router)


