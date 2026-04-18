# storage/redis_client.py
import redis


class RedisClient:
    def __init__(self):
        self.client = redis.Redis(
            host='r-bp1ogeji1wtu8f6ed7pd.redis.rds.aliyuncs.com',
            port=6379,
            db=1,
            password='Lxz123456',
            decode_responses=True
        )

    def batch_filter_exists(self, key, values: list):
        """
        批量去重：添加并返回新数据
        返回: 不存在的元素列表
        """
        if not values:
            return []

        pipe = self.client.pipeline()
        for v in values:
            pipe.sadd(key, v)
        results = pipe.execute()

        # sadd 返回: 1=新数据, 0=已存在
        return [v for v, r in zip(values, results) if r == 1]

    def batch_add(self, key, values: list):
        """
        批量添加元素到集合
        """
        if not values:
            return 0

        pipe = self.client.pipeline()
        for v in values:
            pipe.sadd(key, v)
        results = pipe.execute()

        # 返回成功添加的数量
        return sum(1 for r in results if r == 1)