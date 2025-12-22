from __future__ import annotations

import random
from datetime import datetime, timezone

"""
Stand-in for Twitter API. Randomly generates a dict of tweets of varying sentiment.

Parameters
query : str
    The topic associated with the tweet.
n : int
    The number of tweets to be generated.

Returns
list[dict]

"""
def fetch_mock_tweets(query: str, n: int=10) -> list[dict]:
    templates = [
        f"I love {query} — this is awesome!",
        f"{query} is overrated tbh.",
        f"Neutral take: {query} exists.",
        f"Hot take: {query} is changing everything.",
        f"Not sure how I feel about {query} yet.",
        f"{query} is honestly terrible today.",
        f"Big fan of {query}.",
        f"{query} news is wild.",
        f"Why is everyone talking about {query}?",
        f"{query} just made my day.",
    ]
    now = datetime.now(timezone.utc)
    tweets = []
    for _ in range(n):
        text = random.choice(templates)
        tweets.append(
            {
                "text": text,
                "created_at": now,
                "raw_json": {"source": "mock", "query": query, "text": text}
            }
        )
    return tweets
