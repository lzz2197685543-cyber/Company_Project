# storage/product_dao.py
from storage.base_dao import BaseDAO
from storage.redis_client import RedisClient
from typing import List, Dict, Optional
from utils.logger import get_logger


class ProductDAO(BaseDAO):

    def __init__(self,job):
        super().__init__(job)
        self.redis_client = RedisClient()


    # ---------------- 批量插入 ----------------
    def insert_products(self,products: list):
        if not products:
            return

        # 👉 1. Redis去重（核心）
        goods_ids=[p.goods_id for p in products]

        new_ids=self.redis_client.batch_filter_exists("temu:goods_ids",goods_ids)

        if not new_ids:
            self.logger.info(f'没有新数据不插入')
            return

        # 👉 2.过滤出未重复数据
        new_products=[p for p in products if p.goods_id in new_ids]

        self.logger.info(f'未重复的数据有：{len(new_products)}条数据')

        sql = """
        INSERT INTO products_auto 
        (source, goods_id, name, category,sub_category ,month_sale,status, shop_id)
        VALUES (%s, %s, %s, %s, %s, %s,%s,%s)
        ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        category = VALUES(category),
        sub_category = VALUES(sub_category),
        updated_at = CURRENT_TIMESTAMP
        """

        data = [
            (
                p.source,
                p.goods_id,
                p.name,
                p.category,
                p.sub_category,
                p.month_sale,
                p.status,
                p.shop_id
            )
            for p in new_products
        ]
        # 使用连接池的批量执行
        affected_rows=self.executemany(sql,data)
        # 标记已插入的商品到Redis
        self.redis_client.batch_add("temu:goods_ids", new_ids)

        return affected_rows

    # ---------------- 获取待处理数据（加锁版本） ----------------
    def fetch_pending(self, limit: int = 50) -> List[Dict]:
        """获取待处理数据并加锁"""
        with self.get_cursor() as cursor:
            # 开启事务
            cursor.execute("START TRANSACTION")

            try:
                # 查询待处理数据并加锁
                select_sql = """
                SELECT * FROM products_auto
                WHERE status = 'pending'
                ORDER BY created_at DESC
                LIMIT %s
                FOR UPDATE
                """
                cursor.execute(select_sql, (limit,))
                rows = cursor.fetchall()

                if not rows:
                    cursor.execute("COMMIT")
                    return []

                # 标记为 processing
                ids = [row['id'] for row in rows]
                placeholders = ','.join(['%s'] * len(ids))
                update_sql = f"""
                UPDATE products_auto
                SET status = 'processing'
                WHERE id IN ({placeholders})
                """
                cursor.execute(update_sql, ids)

                cursor.execute("COMMIT")
                return rows

            except Exception as e:
                cursor.execute("ROLLBACK")
                self.logger.error(f"fetch_pending 失败: {e}")
                raise

    # ---------------- 更新状态 ----------------
    def update_status(self, goods_id: str, status: str) -> bool:
        """更新商品状态"""
        sql = """
        UPDATE products_auto
        SET status = %s
        WHERE goods_id = %s
        """
        try:
            affected_rows = self.execute(sql, (status, goods_id))
            return affected_rows > 0
        except Exception as e:
            self.logger.error(f"更新状态失败: {e}")
            return False

    # ---------------- 标记失败（带重试） ----------------
    def mark_failed(self, goods_id: str) -> bool:
        """标记失败并处理重试逻辑"""
        sql = """
        UPDATE products_auto
        SET 
            retry_count = retry_count + 1,
            status = CASE 
                WHEN retry_count >= 3 THEN 'failed'
                ELSE 'pending'
            END
        WHERE goods_id = %s
        """
        try:
            affected_rows = self.execute(sql, (goods_id,))

            # 获取更新后的状态
            check_sql = "SELECT status, retry_count FROM products_auto WHERE goods_id = %s"
            result = self.fetchone(check_sql, (goods_id,))

            if result:
                self.logger.info(
                    f"商品 {goods_id} 重试次数: {result['retry_count']}, "
                    f"状态: {result['status']}"
                )

            return affected_rows > 0

        except Exception as e:
            self.logger.error(f"标记失败时出错: {e}")
            return False

    # ---------------- 批量更新状态 ----------------
    def batch_update_status(self, goods_ids: List[str], status: str) -> int:
        """批量更新商品状态"""
        if not goods_ids:
            return 0

        placeholders = ','.join(['%s'] * len(goods_ids))
        sql = f"""
        UPDATE products_auto
        SET status = %s
        WHERE goods_id IN ({placeholders})
        """

        params = [status] + goods_ids
        return self.execute(sql, tuple(params))

    # ---------------- 获取成功的商品 ----------------
    def get_successful_goods(self, goods_ids: List[str]) -> List[str]:
        """筛选出状态为 collected 的商品"""
        if not goods_ids:
            return []

        placeholders = ','.join(['%s'] * len(goods_ids))
        sql = f"""
        SELECT goods_id FROM products_auto 
        WHERE goods_id IN ({placeholders}) 
        AND status = 'collected'
        """

        results = self.fetchall(sql, tuple(goods_ids))
        return [row['goods_id'] for row in results]

    # ---------------- 获取统计信息 ----------------
    def get_statistics(self) -> Dict[str, int]:
        """获取商品状态统计"""
        sql = """
        SELECT 
            status,
            COUNT(*) as count
        FROM products_auto
        GROUP BY status
        """

        results = self.fetchall(sql)
        stats = {}
        for row in results:
            stats[row['status']] = row['count']

        return stats

    # ---------------- 清理过期数据 ----------------
    def clean_expired_data(self, days: int = 30) -> int:
        """清理超过指定天数的失败数据"""
        sql = """
        DELETE FROM products_auto
        WHERE status = 'failed'
        AND updated_at < DATE_SUB(NOW(), INTERVAL %s DAY)
        """

        return self.execute(sql, (days,))