import redis
from src.config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_USERNAME, REDIS_PASSWORD
)

def get_redis_client() -> redis.Redis:
    """Retorna una instancia de cliente Redis configurada."""
    return redis.Redis(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        db=REDIS_DB, 
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
