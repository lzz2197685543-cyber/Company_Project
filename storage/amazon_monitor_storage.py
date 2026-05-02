import pymysql
from utils.logger import get_logger


class AmazonMonitorStorage:

    def __init__(self, job):
        self.logger = get_logger(job)

        self.conn = pymysql.connect(
            host='rm-bp186omby3lautfn0no.mysql.rds.aliyuncs.com',
            port=3306,
            user='root_lxz',
            password='Lxz123456',
            database='py_spider',
            charset="utf8mb4"
        )

        # 初始化时创建表
        self.create_table()

    def create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS amazon_product_monitor (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,

            asin VARCHAR(20) NOT NULL,
            product_name VARCHAR(500),
            product_url VARCHAR(1000),

            crawl_date DATE,

            price DECIMAL(10,2),
            coupon VARCHAR(50),
            sales INT,

            bsr_rank INT,
            sub_rank INT,

            all_keywords INT,
            natural_keywords INT,
            ads_keywords INT,
            recommend_keywords INT,

            rating DECIMAL(3,2),
            review_count INT,

            title VARCHAR(1000),

            title_changed VARCHAR(10),
            
            subcategorie_name VARCHAR(1000),
            
            subcategorie_changed VARCHAR(10),

            img_url VARCHAR(1000),
            
            price_changed VARCHAR(10),
            coupon_changed VARCHAR(10),

            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            INDEX idx_asin (asin),
            INDEX idx_crawl_date (crawl_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        with self.conn.cursor() as cursor:
            cursor.execute(sql)

        self.conn.commit()

    def save(self, data):
        sql = """
        INSERT INTO amazon_product_monitor
        (asin,product_name,product_url,crawl_date,price,coupon,sales,
        bsr_rank,sub_rank,all_keywords,natural_keywords,ads_keywords,
        recommend_keywords,rating,review_count,title,title_changed,subcategorie_name,subcategorie_changed,coupon_changed,price_changed,img_url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        # 处理可能为空的数值类型字段
        def clean_value(value):
            """将空字符串转换为 None，保留0作为有效值"""
            if value == '' or value == 'None' or value is None:
                return None
            return value

        with self.conn.cursor() as cursor:
            cursor.execute(sql, (
                data["asin"],
                data["product_name"],
                data["url"],
                data["date"],
                clean_value(data.get("price")),  # 修改这里
                data.get("coupon"),  # coupon 是字符串，可以保留空字符串
                clean_value(data.get("sales")),  # 修改这里
                clean_value(data.get("bsr_rank")),  # 修改这里
                clean_value(data.get("sub_rank")),  # 修改这里
                clean_value(data.get("all_keywords")),  # 修改这里
                clean_value(data.get("natural_keywords")),  # 修改这里
                clean_value(data.get("ads_keywords")),  # 修改这里
                clean_value(data.get("recommend_keywords")),  # 修改这里
                clean_value(data.get("rating")),  # 修改这里
                clean_value(data.get("reviews")),  # 修改这里
                data.get("title"),
                data.get("title_changed"),
                data.get("subcategorie_name"),
                data.get('subcategorie_changed'),
                data.get('coupon_changed'),
                data.get("price_changed"),
                data.get("img_url")
            ))

        self.conn.commit()

    def get_yesterday_data(self, asin):
        sql = """
        SELECT price,title,subcategorie_name,coupon
        FROM amazon_product_monitor
        WHERE asin=%s
        ORDER BY crawl_date DESC
        LIMIT 1
        """

        with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql, (asin,))
            return cursor.fetchone()