import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv("DATABASE_URL", "")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is not set")

engine: Engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def db_ping() -> bool:
    """Return True if DB is reachable and can run a trivial query."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
