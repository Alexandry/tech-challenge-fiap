import time
from typing import Any, Optional
from app.db.mongo import db

CACHE_TTL_SECONDS = 3600  # 1 hora

def get_cache(key: str) -> Optional[Any]:
    cache = db.cache.find_one({"_id": key})
    if cache and (time.time() - cache["timestamp"] < CACHE_TTL_SECONDS):
        return cache["data"]
    return None

def set_cache(key: str, data: Any):
    db.cache.update_one(
        {"_id": key},
        {"$set": {"data": data, "timestamp": time.time()}},
        upsert=True
    )