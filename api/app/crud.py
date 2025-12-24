from __future__ import annotations

from typing import Any
from sqlalchemy import text, bindparam
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSONB

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
def insert_tweet(engine: Engine, *, query: str, text_content: str, created_at: Any | None = None, raw_json: dict | None = None) -> int:
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
def list_tweets(engine: Engine, *, query: str | None = None, limit: int = 50) -> list[dict]:
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
