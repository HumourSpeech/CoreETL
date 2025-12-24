from app.logging.logger import logging
from app.exception.exception import CoreETL
import sys

from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.db import init_db
from app.api import routes
from app.ingestion.pipeline import run_etl
import threading
import time

# automatically start ETL on startup
def start_etl_loop():
    logging.info("ETL thread started, waiting for DB to be ready...")
    # Wait a bit for DB to be ready
    time.sleep(5)
    logging.info("Starting initial ETL run...")
    try:
        run_etl()
    except Exception as e:
        logging.info(f"Initial ETL failed: {e}")
        raise CoreETL(e, sys)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_db()
    # Run ETL in a separate thread on startup so it doesn't block API
    thread = threading.Thread(target=start_etl_loop)
    thread.daemon = True
    thread.start()
    yield
    # Shutdown

app = FastAPI(title="CoreETL Backend", lifespan=lifespan)

app.include_router(routes.router)

@app.get("/")
def root():
    return {"message": "Welcome to CoreETL API. Go to /docs for Swagger UI."}
