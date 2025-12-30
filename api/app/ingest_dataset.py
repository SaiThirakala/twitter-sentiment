import csv
from datetime import datetime, timezone

LABEL_MAP = {
    -1: "NEGATIVE",
    0: "NEUTRAL",
    1: "POSITIVE",
}

def fetch_dataset_tweets(
    path: str,
    *,
    limit: int=50,
) -> list[dict]:
    """
    Load tweets from a labeled tweet dataset.

    Expected columns:
    - tweet (or text)
    - label (1, 0, -1)
    """
    tweets = []
    now = datetime.now(timezone.utc)

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if len(tweets) >= limit:
                break
            
            text = (row.get("clean_text") or "").strip()
            label_raw = row.get("category")

            if not text or label_raw is None:
                continue
            
            try:
                label_int = int(label_raw)
            except ValueError:
                continue

            if label_int not in LABEL_MAP:
                continue

            tweets.append({
                "text": text,
                "created_at": now,
                "raw_json": {
                    "dataset_label": LABEL_MAP[label_int],
                    "original_label": label_raw,
                    "source": "twitter-sentinment-dataset",
                },
            })
    
    return tweets