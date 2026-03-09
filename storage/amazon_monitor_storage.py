import pymysql
from utils.logger import get_logger


class AmazonMonitorStorage:

    def __init__(self, job):
        self.logger = get_logger(job)

        self.conn = pymysql.connect(
            host="127.0.0.1",
            user="root",
            password="1234",
            database="py_spider",
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

            img_url VARCHAR(1000),

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
        recommend_keywords,rating,review_count,title,title_changed,img_url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        with self.conn.cursor() as cursor:
            cursor.execute(sql, (
                data["asin"],
                data["product_name"],
                data["url"],
                data["date"],
                data["price"],
                data["coupon"],
                data["sales"],
                data["bsr_rank"],
                data["sub_rank"],
                data["all_keywords"],
                data["natural_keywords"],
                data["ads_keywords"],
                data["recommend_keywords"],
                data["rating"],
                data["reviews"],
                data["title"],
                data["title_changed"],
                data["img_url"]
            ))

        self.conn.commit()

    def get_yesterday_data(self, asin):
        sql = """
        SELECT price,title
        FROM amazon_product_monitor
        WHERE asin=%s
        ORDER BY crawl_date DESC
        LIMIT 1
        """

        with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql, (asin,))
            return cursor.fetchone()