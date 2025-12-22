from __future__ import annotations

from typing import Any
from sqlalchemy import text
from sqlalchemy.engine import Engine

def insert_tweet(engine: Engine, *, query: str, text_content: str, created_at: Any | None = None, raw_json: dict | None = None) -> int:
    """
    Inserts a tweet row and returns the new id.
    created_at can be None and raw_json is optional.
    """
    sql = text("""
        INSERT INTO tweets (query, text, created_at, raw_json)
        VALUES (:query, :text, :created_at, :raw_json)
        RETURNING id;
    """)
    with engine.begin as conn:
        new_id = conn.execute(
            sql,
            {"query": query, "text": text_content, "created_at": created_at, "raw_json": raw_json},
        ).scalar_one()
    return int(new_id)

