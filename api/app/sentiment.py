from __future__ import annotations

from functools import lru_cache
from transformers import pipeline

MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english"

@lru_cache(maxsize=1)
def get_sentiment_pipeline():
    # Loads once per process, then cached.
    return pipeline("sentiment-analysis", model=MODEL_NAME)

def predict_sentiment(text: str) -> dict:
    """
    Returns dict with keys: label, score, model_name.
    """
    clf = get_sentiment_pipeline()
    out = clf(text[:512])[0]  # safety truncation for long inputs
    return {"label": out["label"], "score": float(out["score"]), "model_name": MODEL_NAME}
