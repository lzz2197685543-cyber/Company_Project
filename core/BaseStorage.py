import pymysql
import redis



class BaseStorage:
    def __init__(
        self,
        mysql_conf: dict,
        redis_conf: dict,
        redis_prefix: str,

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
    def redis_is_duplicate_permanent(self, unique_key: str) -> bool:
        """永不过期的去重检查"""
        redis_key = f"{self.redis_prefix}:{unique_key}"
        is_new = self.redis.setnx(redis_key, 1)  # 设置成功返回1，失败返回0
        # 不调用 expire()，键就永远不会过期
        return not is_new  # True表示重复，False表示新key
