import pymysql
import redis



class BaseStorage:
    def __init__(
        self,
        mysql_conf: dict,
        redis_conf: dict,
        redis_prefix: str
    ):
        # MySQL
        self.db = pymysql.connect(
            charset="utf8mb4",
            autocommit=True,
            **mysql_conf
        )
        self.cursor = self.db.cursor()

        # Redis
        self.redis = redis.Redis(
            decode_responses=True,
            **redis_conf
        )

        self.redis_prefix = redis_prefix

    # ---------- Redis 去重 ----------
    def redis_is_duplicate(self, unique_key: str, expire_days=60) -> bool:
        redis_key = f"{self.redis_prefix}:{unique_key}"
        is_new = self.redis.setnx(redis_key, 1)
        if is_new:
            self.redis.expire(redis_key, expire_days * 86400)
        return not is_new
