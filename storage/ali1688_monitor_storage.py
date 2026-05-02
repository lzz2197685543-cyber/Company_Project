import pymysql
from datetime import date


class Ali1688MonitorStorage:

    def __init__(self):
        self.conn = pymysql.connect(
            host='rm-bp186omby3lautfn0no.mysql.rds.aliyuncs.com',
            port=3306,
            user='root_lxz',
            password='Lxz123456',
            database='py_spider',
            charset="utf8mb4"
        )

        # 自动建表
        self.create_tables()

    # -------------------------
    # 创建数据表
    # -------------------------
    def create_tables(self):

        product_table_sql = """
        CREATE TABLE IF NOT EXISTS ali1688_product_monitor (
            id INT AUTO_INCREMENT PRIMARY KEY,
            
            offerid VARCHAR(255) NOT NULL,
            url VARCHAR(255),

            publish_time VARCHAR(50),
            category VARCHAR(200),
            repurchase_rate VARCHAR(20),

            day_order_count INT,
            day_sale_quantity INT,
            day_sales_volume DECIMAL(12,2),

            order_30d INT,
            sales_volume_30d DECIMAL(12,2),
            sale_quantity_30d INT,

            sku_count INT,

            total_order_count INT,
            total_sale_count INT,


            shop_name VARCHAR(200),
            
            bookedCount7dGrowthRate DECIMAL(12,2),

            category_changed VARCHAR(10),
            count_changed VARCHAR(10),

            create_time DATE
        )
        """

        sku_table_sql = """
        CREATE TABLE IF NOT EXISTS ali1688_sku_monitor (
            id INT AUTO_INCREMENT PRIMARY KEY,

            url VARCHAR(255),
            skuid VARCHAR(100),

            sku_name VARCHAR(200),
            page_price DECIMAL(10,2),

            sku_count INT,

            price_changed VARCHAR(10),
            name_changed VARCHAR(10),
            count_changed VARCHAR(10),

            create_time DATE
        )
        """

        with self.conn.cursor() as cursor:
            cursor.execute(product_table_sql)
            cursor.execute(sku_table_sql)

        self.conn.commit()

    # -------------------------
    # 获取上一次商品数据（与最近一次非当天的记录对比）
    # -------------------------
    def get_last_product(self, url):
        sql = """
        SELECT *
        FROM ali1688_product_monitor
        WHERE url=%s
        AND DATE(create_time) < CURDATE()
        ORDER BY create_time DESC
        LIMIT 1
        """

        with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql, (url,))
            result = cursor.fetchone()

        return result

    # -------------------------
    # 获取上一次SKU（与最近一次非当天的记录对比）
    # -------------------------
    def get_last_sku(self, url):
        sql = """
        SELECT *
        FROM ali1688_sku_monitor
        WHERE url=%s
        AND DATE(create_time) < CURDATE()
        ORDER BY create_time DESC
        """

        with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql, (url,))
            result = cursor.fetchall()

        return result

    # -------------------------
    # 保存商品数据
    # -------------------------
    def save_product(self, product):

        sql = """
        INSERT INTO ali1688_product_monitor (
            offerid,
            url,
            publish_time,
            category,
            repurchase_rate,

            day_order_count,
            day_sale_quantity,
            day_sales_volume,

            order_30d,
            sales_volume_30d,
            sale_quantity_30d,

            sku_count,

            total_order_count,
            total_sale_count,

            shop_name,
            bookedCount7dGrowthRate,
            category_changed,
            count_changed,
            create_time
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        values = (
            product['offerid'],
            product["url"],
            product["publish_time"],
            product["category"],
            product["repurchase_rate"],
            product["day_order_count"],
            product["day_sale_quantity"],
            product["day_sales_volume"],
            product["order_30d"],
            product["sales_volume_30d"],
            product["sale_quantity_30d"],
            product["sku_count"],
            product["total_order_count"],
            product["total_sale_count"],
            product["shop_name"],
            product["bookedCount7dGrowthRate"],
            product["category_changed"],
            product['count_changed'],
            date.today()
        )

        with self.conn.cursor() as cursor:
            cursor.execute(sql, values)

        self.conn.commit()

    # -------------------------
    # 保存SKU
    # -------------------------
    def save_sku(self, sku):

        sql = """
        INSERT INTO ali1688_sku_monitor (
            url,
            skuid,
            sku_name,
            page_price,
            sku_count,

            price_changed,
            name_changed,
            

            create_time
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """

        values = (
            sku["url"],
            sku["skuid"],
            sku["sku_name"],
            sku["page_price"],
            sku["sku_count"],
            sku["price_changed"],
            sku["name_changed"],
            date.today()
        )

        with self.conn.cursor() as cursor:
            cursor.execute(sql, values)

        self.conn.commit()

    def get_today_changed_products(self):
        """
        获取今天有变化的商品（类目变化或SKU数量变化）- 去重
        """
        sql = """
        SELECT DISTINCT p1.*, 
               p2.sku_count as old_sku_count,
               p2.category as old_category
        FROM ali1688_product_monitor p1
        LEFT JOIN ali1688_product_monitor p2 
            ON p1.url = p2.url 
            AND DATE(p2.create_time) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        WHERE DATE(p1.create_time) = CURDATE()
          AND (p1.category_changed = '是' OR p1.count_changed = '是')
        ORDER BY p1.create_time DESC
        """
        # 注意：这里添加了 DISTINCT
        with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        return result

    def get_today_changed_skus(self):
        """
        获取今天有变化的SKU（价格变化或名称变化）- 去重
        """
        sql = """
        SELECT DISTINCT s1.*,
               s2.sku_name as old_sku_name,
               s2.page_price as old_price
        FROM ali1688_sku_monitor s1
        LEFT JOIN ali1688_sku_monitor s2 
            ON s1.url = s2.url 
            AND s1.skuid = s2.skuid
            AND DATE(s2.create_time) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        WHERE DATE(s1.create_time) = CURDATE()
          AND (s1.price_changed = '是' OR s1.name_changed = '是')
        ORDER BY s1.create_time DESC
        """
        # 注意：这里添加了 DISTINCT
        with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        return result

    def get_today_all_products(self):
        """获取今天爬取的所有商品数据"""
        try:
            sql = """
            SELECT *
            FROM ali1688_product_monitor
            WHERE DATE(create_time) = CURDATE()
            ORDER BY create_time DESC
            """

            with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql)
                products = cursor.fetchall()

            return products
        except Exception as e:
            print(f"获取今天所有商品数据失败: {e}")
            return []

    def get_today_all_skus(self):
        """获取今天爬取的所有SKU数据"""
        try:
            sql = """
            SELECT *
            FROM ali1688_sku_monitor
            WHERE DATE(create_time) = CURDATE()
            ORDER BY create_time DESC
            """

            with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql)
                skus = cursor.fetchall()

            return skus
        except Exception as e:
            print(f"获取今天所有SKU数据失败: {e}")
            return []