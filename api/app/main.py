from fastapi import FastAPI, Query
from app.db import db_ping, engine
from app.crud import insert_tweet, insert_sentiment, list_tweets_with_latest_sentiment
from app.ingest import fetch_mock_tweets
from app.sentiment import predict_sentiment
from sqlalchemy import text as sql_text

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
def get_tweets(query: str | None = None, limit: int = 50, model_name: str | None = None):
    limit = max(1, min(limit, 200))
    items = list_tweets_with_latest_sentiment(engine, query=query, limit=limit, model_name=model_name)
    return {"count": len(items), "items": items}

@app.post("/predict")
def predict(text: str):
    return predict_sentiment(text)

@app.post("/score-latest")
def score_latest(query: str | None = None, limit: int = 50) -> dict:
    limit = max(1, min(limit, 200))
    model_name = "cardiffnlp/twitter-roberta-base-sentiment"

    # Get tweet IDs that match query and have no sentiment yet, optional
    sql = """
        SELECT t.id, t.text
        FROM tweets t
        LEFT JOIN tweet_sentiment s
        ON s.tweet_id = t.id AND s.model_name = :model_name
        WHERE s.id IS NULL
    """
    params = {"limit": limit, "model_name": model_name}
    if query:
        sql += " AND t.query = :query"
        params["query"] = query
    sql += " ORDER BY t.inserted_at DESC LIMIT :limit"

    with engine.connect() as conn:
        rows = conn.execute(sql_text(sql), params).mappings().all()

    inserted = 0
    for r in rows:
        pred = predict_sentiment(r["text"])
        insert_sentiment(
            engine,
            tweet_id=int(r["id"]),
            model_name=pred["model_name"],
            label=pred["label"],
            score=pred["score"],
        )
        inserted += 1

    return {"scored": inserted}