import os

SQLALCHEMY_DATABASE_URI = os.getenv(
    "SUPERSET_METADATA_DB",
    "postgresql://postgres:postgres@db:5432/superset_db"
)

SECRET_KEY = os.getenv(
    "SUPERSET_SECRET_KEY",
    "7_this_is_a_professional_and_secure_key_8_!"
)

BABEL_DEFAULT_LOCALE = "es"

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

def redis_url(db: int) -> str:
    return f"redis://{REDIS_HOST}:{REDIS_PORT}/{db}"

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_results",
    "CACHE_REDIS_URL": redis_url(0),
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_filter_state",
    "CACHE_REDIS_URL": redis_url(1),
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_explore_form_data",
    "CACHE_REDIS_URL": redis_url(2),
}

MAPBOX_API_KEY = ""
ENABLE_CHUNKED_DATA_TRANSFER = True