from __future__ import annotations

from functools import lru_cache
from transformers import pipeline

MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment"

LABEL_MAP = {
    "LABEL_0": "NEGATIVE",
    "LABEL_1": "NEUTRAL",
    "LABEL_2": "POSITIVE",
}

@lru_cache(maxsize=1)
def get_sentiment_pipeline():
    # Loads once per process, then cached.
    return pipeline("text-classification", model=MODEL_NAME)

def predict_sentiment(text: str) -> dict:
    """
    Returns dict with keys: label, score, model_name.
    """
    clf = get_sentiment_pipeline()
    out = clf(text[:512])[0]  # safety truncation for long inputs
    raw_label = out["label"]
    return {
        "label": LABEL_MAP.get(raw_label, raw_label),
        "score": float(out["score"]),
        "model_name": MODEL_NAME,
        "raw_label": raw_label,
    }
