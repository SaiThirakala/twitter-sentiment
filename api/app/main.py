from fastapi import FastAPI, Query
from app.db import db_ping, engine
from app.crud import insert_tweet, list_tweets
from app.ingest import fetch_mock_tweets

app = FastAPI(title="Twitter Sentiment API", version="0.1.0")

@app.get("/health")
def health():
    ok = db_ping()
    return {"status": "ok" if ok else "degraded", "db": ok}

@app.post("/ingest")
def ingest(query: str = Query(..., min_length=1), n: int = Query(10, ge=1, le=100)):
    tweets = fetch_mock_tweets(query=query, n=n)
    inserted_ids = []
    for t in tweets:
        new_id = insert_tweet(
            engine,
            query=query,
            text_content=t["text"],
            created_at=t.get("created_at"),
            raw_json=t.get("raw_json"),
        )
        inserted_ids.append(new_id)
    return {"query": query, "requested": n, "inserted": len(inserted_ids), "ids": inserted_ids}

@app.get("/tweets")
def get_tweets(query: str | None = None, limit: int = 50):
    limit = max(1, min(limit, 200))
    items = list_tweets(engine, query=query, limit=limit)
    return {"count": len(items), "items": items}