from fastapi import FastAPI
from app.db import db_ping

app = FastAPI(title="Twitter Sentiment API", version="0.1.0")

@app.get("/health")
def health():
    ok = db_ping()
    return {"status": "ok" if ok else "degraded", "db": ok}
