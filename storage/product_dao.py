from storage.db import Database
from storage.redis_client import RedisClient

db = Database()
redis_client = RedisClient()


class ProductDAO:

    # ---------------- 批量插入 ----------------
    @staticmethod
    def insert_products(products: list,logger):
        if not products:
            return

        # 👉 1. Redis去重（核心）
        goods_ids=[p.goods_id for p in products]

        new_ids=redis_client.batch_filter_exists("temu:goods_ids",goods_ids)

        if not new_ids:
            logger.info(f'没有新数据不插入')
            return

        # 👉 2.过滤出未重复数据
        new_products=[p for p in products if p.goods_id in new_ids]

        logger.info(f'未重复的数据有：{len(new_products)}条数据')
        print(f'未重复的数据有：{len(new_products)}条数据')

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

        db.executemany(sql, data)

    # ---------------- 获取待处理数据（加锁版本） ----------------
    @staticmethod
    def fetch_pending(limit=50):
        """
        取数据 + 标记为 processing（防止重复消费）
        """
        conn = db.get_conn()
        cursor = conn.cursor()

        try:
            # 1️⃣ 查
            cursor.execute("""
                SELECT * FROM products_auto
WHERE status = 'pending'
ORDER BY created_at DESC
LIMIT %s
FOR UPDATE
            """, (limit,))
            rows = cursor.fetchall()

            if not rows:
                return []

            ids = [row['id'] for row in rows]

            # 2️⃣ 标记为 processing
            format_ids = ','.join(['%s'] * len(ids))
            cursor.execute(f"""
                UPDATE products_auto
                SET status = 'processing'
                WHERE id IN ({format_ids})
            """, ids)

            conn.commit()
            return rows

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            conn.close()

    # ---------------- 更新状态 ----------------
    @staticmethod
    def update_status(goods_id, status):
        sql = """
        UPDATE products_auto
        SET status = %s
        WHERE goods_id = %s
        """
        db.execute(sql, (status, goods_id))

    @staticmethod
    def fetch_collected(limit=20):
        conn = db.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT * FROM products_auto
                WHERE status = 'collected'
                ORDER BY created_at ASC
                LIMIT %s
                FOR UPDATE
            """, (limit,))

            rows = cursor.fetchall()

            if not rows:
                return []

            ids = [row['id'] for row in rows]

            format_ids = ','.join(['%s'] * len(ids))
            cursor.execute(f"""
                UPDATE products_auto
                SET status = 'publishing'
                WHERE id IN ({format_ids})
            """, ids)

            conn.commit()
            return rows

        finally:
            conn.close()

    # ---------------- 标记成功 ----------------
    @staticmethod
    def mark_success(goods_id):
        ProductDAO.update_status(goods_id, "success")

    # ---------------- 标记失败（带重试） ----------------
    @staticmethod
    def mark_failed(goods_id):
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
        db.execute(sql, (goods_id,))