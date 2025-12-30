from fastapi import FastAPI, Query
from app.db import db_ping, engine
from app.crud import insert_tweet, insert_sentiment, list_tweets_with_latest_sentiment, get_sentiment_stats
from app.ingest import fetch_mock_tweets
from app.sentiment import predict_sentiment
from sqlalchemy import text as sql_text
from app.ingest_dataset import fetch_dataset_tweets

app = FastAPI(title="Twitter Sentiment API", version="0.1.0")
dataset_path = "/app/data/Twitter_Data.csv"

@app.get("/health")
def health():
    ok = db_ping()
    return {"status": "ok" if ok else "degraded", "db": ok}

@app.post("/ingest")
def ingest(
    query: str = Query(...),
    n: int = Query(10),
    source: str = Query("mock"),
    dataset_path: str | None = None,
):
    if source == "dataset":
        if not dataset_path:
            raise ValueError("dataset_path is required when source=dataset")

        tweets = fetch_dataset_tweets(
            path=dataset_path,
            limit=n,
        )
    else:
        tweets = fetch_mock_tweets(query=query, n=n)

    inserted_ids = []
    for t in tweets:
        new_id = insert_tweet(
            engine,
            query=query,
            text_content=t["text"],
            created_at=t["created_at"],
            raw_json=t["raw_json"],
        )
        inserted_ids.append(new_id)

    return {
        "source": source,
        "query": query,
        "requested": n,
        "inserted": len(inserted_ids),
        "ids": inserted_ids,
    }

@app.get("/tweets")
def get_tweets(query: str | None = None, limit: int = 50, model_name: str | None = None):
    limit = max(1, min(limit, 200))
    items = list_tweets_with_latest_sentiment(engine, query=query, limit=limit, model_name=model_name)
    return {"count": len(items), "items": items}

@app.post("/predict")
def predict(text: str):
    return predict_sentiment(text)

@app.post("/score-latest")
def score_latest(
    query: str | None = None, 
    limit: int = 50,
    since_id: int | None = None,
    since_time: str | None = None
) -> dict:
    limit = max(1, min(limit, 200))
    model_name = "cardiffnlp/twitter-roberta-base-sentiment"
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

    if since_id:
        sql += " AND t.id > :since_id"
        params["since_id"] = since_id

    if since_time:
        sql += " AND t.inserted_at > :since_time"
        params["since_time"] = since_time

    sql += " ORDER BY t.inserted_at ASC LIMIT :limit"

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

@app.get("/stats")
def stats(query: str | None = None, model_name: str = Query(...)):
    return get_sentiment_stats(engine, query=query, model_name=model_name)
