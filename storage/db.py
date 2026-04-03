import pymysql


class Database:
    def __init__(self):
        self.config = {
            "host": "127.0.0.1",
            "user": "root",
            "password": "1234",
            "database": "py_spider",
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor  # ✅ 返回字典（很重要）
        }

    def get_conn(self):
        return pymysql.connect(**self.config)

    # ================= 初始化 =================
    def init_db(self):
        """初始化数据库表"""
        conn = self.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS products_auto (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                source VARCHAR(255),
                goods_id VARCHAR(50) NOT NULL UNIQUE,
                name TEXT,
                category VARCHAR(100),
                status VARCHAR(20) DEFAULT 'pending',
                shop_id VARCHAR(50),
                retry_count INT DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                INDEX idx_status (status),
                INDEX idx_category (category)
            )
            """)
            conn.commit()
        finally:
            conn.close()

    def executemany(self, sql, data):
        conn = self.get_conn()
        cursor = conn.cursor()

        try:
            cursor.executemany(sql, data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("❌ SQL执行失败:", e)
            print("❌ SQL:", sql)
            print("❌ 数据示例:", data[:2])  # 打印前两条就够了
            raise e
        finally:
            conn.close()


    # ================= 通用执行 =================
    def execute(self, sql, params=None):
        """通用执行（INSERT/UPDATE/DELETE）"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
        finally:
            conn.close()

    def query(self, sql, params=None):
        """查询（返回列表）"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            conn.close()

    def query_one(self, sql, params=None):
        """查询单条"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            return cursor.fetchone()
        finally:
            conn.close()