# app/storage/postgres.py

from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("postgresql+psycopg2://avnadmin:AVNS_SPx5mGZsHfWLTBIzEGM@pg-39985733-pallavidapriya75-97f0.h.aivencloud.com:12783/defaultdb?sslmode=require")

def save_latest(df):
    df.to_sql("latest_market_data", engine, if_exists="replace", index=False)