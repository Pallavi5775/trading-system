# app/storage/postgres.py


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker





DATABASE_URL = "postgresql+psycopg2://avnadmin:AVNS_SPx5mGZsHfWLTBIzEGM@pg-39985733-pallavidapriya75-97f0.h.aivencloud.com:12783/defaultdb?sslmode=require"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)