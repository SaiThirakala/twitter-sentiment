from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Text, BigInteger, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB

class Base(DeclarativeBase):
    pass

class Tweet(Base):
    __tablename__ = "tweets"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    inserted_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
