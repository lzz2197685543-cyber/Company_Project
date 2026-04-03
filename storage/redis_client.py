import redis


class RedisClient:
    def __init__(self):
        self.client = redis.Redis(
            host='localhost',
            port=6379,
            db=1,
            decode_responses=True
        )

    def batch_filter_exists(self, key, values: list):
        """
        批量去重（正确版本）
        """
        pipe = self.client.pipeline()

        for v in values:
            pipe.sadd(key, v)   # ✅ 用 sadd

        results = pipe.execute()

        # 👉 sadd 返回：
        # 1 = 新数据
        # 0 = 已存在

        return [v for v, r in zip(values, results) if r == 1]