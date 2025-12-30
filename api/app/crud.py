from __future__ import annotations

from typing import Any
from sqlalchemy import text, bindparam
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSONB


def insert_tweet(engine: Engine, *, query: str, text_content: str, created_at: Any | None = None, raw_json: dict | None = None) -> int:
    """
    Insert a single tweet into the database and return its generated ID.

    Parameters
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine used to connect to teh Postgres database.
    query : str
        The topic associated with the tweet.
    text_content : str
        The raw text content of the tweet.
    created_at : Any | None, optional
        Timestamp indicated when the tweet was orignally created. 
        This value can be None if the source does not provide any timestamp.
        The database accepts timezone-aware timestamps.
    raw_json : dict | None, optional
        Optional raw JSON payload from the datasource. Stored for some potential 
        future use.
        
    Returns
    int
        The primary key ID of the newly inserted tweet.
    """
    sql = text("""
        INSERT INTO tweets (query, text, created_at, raw_json)
        VALUES (:query, :text, :created_at, :raw_json)
        RETURNING id;
    """).bindparams(bindparam("raw_json", type_=JSONB))
    with engine.begin() as conn:
        new_id = conn.execute(
            sql,
            {"query": query, "text": text_content, "created_at": created_at, "raw_json": raw_json},
        ).scalar_one()
    return int(new_id) 

def list_tweets(engine: Engine, *, query: str | None = None, limit: int = 50) -> list[dict]:
    """
    Retrieve the most recent tweets stored in the database. The tweets 
    recieved can be optionally filtered by a query.

    Parameters
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine used to connect to the Postgres database.
    query : str | None, optional
        Optional, used to filter the tweets so that only tweets
        inserted with this query value will be returned.
    limit : int, default=50
        Maximum number of tweets to return.

    Returns 
        list[dict]
            A list of dictionaries, where each dictionary represents a tweet
            row with the following keys:
            - id
            - query
            - text
            - created_at
            - inserted_at
    """
    sql = """
        SELECT id, query, text, created_at, inserted_at
        FROM tweets
    """
    params = {}
    if query:
        sql += " WHERE query = :query"
        params["query"] = query
    sql += " ORDER BY inserted_at DESC LIMIT :limit"
    params["limit"] = limit

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]

def insert_sentiment(engine: Engine, *, tweet_id: int, model_name: str, label: str, score: float) -> int:
    """
    Insert a sentinment prediction for a tweet into the database.

    This function persists the output of a sentinment model for a given tweet to the 
    Postrgres database. Each prediction is stored in a separate row, so each tweet can be
    scored several times, either by a different model or by the same model at a different
    points in time. 

    Parameters
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine used to connect to the Postgres database.
    tweet_id : int
        The ID of the tweet that the sentiment predction is being performed. The ID must reference
        an existing tweet in the 'tweets' table in the Postgres database.
    model_name : str
        Identifier for the NLP classification model used to generate the sentinment prediction.
    label : str
        Predicted sentinment label (NEGATIVE, NEUTRAL, or POSITIVE).
    score : float
        Probability value associated with the predicted label.
    
    Returns
    int
        The primary key ID of the newly inserted sentinment prediction.
    """
    sql = text("""
        INSERT INTO tweet_sentiment (tweet_id, model_name, label, score)
        VALUES (:tweet_id, :model_name, :label, :score)
        RETURNING id;
    """)
    with engine.begin() as conn:
        new_id = conn.execute(sql, {
            "tweet_id": tweet_id,
            "model_name": model_name,
            "label": label,
            "score": score,
        }).scalar_one()
    return int(new_id)

def list_tweets_with_latest_sentiment(
    engine: Engine,
    *,
    query: str | None = None,
    limit: int = 50,
    model_name: str | None = None,
) -> list[dict]:
    """
    Return tweets with the most recent sentiment prediction.

    For each tweet, this function returns the tweet metadata along with the 
    latest available sentinment prediction. These predictions can be filtered by
    model name. If model name is not specified, the most recent predictions
    from any model are returned.

    Parameters
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine used to connect to the Postgres database.
    query : str | None, optional
        If provided, restricts tweets returned to those ingested with
        the given query/topic. If None, tweets from all queries are returned.
    limit : int | default=50
        Maximum number of tweets to return. Results are ordered by
        insertion time (most recent first).
    model_name : str | None, optional
        If provided, only sentiment predictions generated by this model
        are considered when selecting the latest sentiment for each tweet.
    
    Returns
    list[dict]
        A list of dictionaries, where each dictionary represents a tweet
        joined with its latest sentiment prediction. The result includes:
        - tweet metadata (id, query, text, timestamps)
        - sentiment metadata (model_name, label, score, predicted_at)
    """
    base_sql = """
    SELECT
      t.id,
      t.query,
      t.text,
      t.created_at,
      t.inserted_at,
      s.model_name,
      s.label,
      s.score,
      s.predicted_at
    FROM tweets t
    LEFT JOIN LATERAL (
      SELECT model_name, label, score, predicted_at
      FROM tweet_sentiment
      WHERE tweet_id = t.id
    """

    params: dict = {"limit": limit}

    if model_name:
        base_sql += " AND model_name = :model_name"
        params["model_name"] = model_name

    base_sql += """
      ORDER BY predicted_at DESC
      LIMIT 1
    ) s ON TRUE
    """

    if query:
        base_sql += " WHERE t.query = :query"
        params["query"] = query

    base_sql += " ORDER BY t.inserted_at DESC LIMIT :limit"

    with engine.connect() as conn:
        rows = conn.execute(text(base_sql), params).mappings().all()

    return [dict(r) for r in rows]