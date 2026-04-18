# storage/db_pool.py
import pymysql
from dbutils.pooled_db import PooledDB
from config.settings import DB_CONFIG
import threading
from utils.logger import get_logger


class DatabasePool:
    """数据库连接池管理器（单例模式）"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls,job=None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self,job=None):
        if self._initialized:
            return

        self.logger = get_logger(job)
        self._initialize_pool()
        self._init_tables()  # ✅ 初始化表结构
        self._initialized = True

    def _initialize_pool(self):
        """初始化连接池"""
        try:
            # 从配置文件获取数据库配置
            db_config = {
                'host': DB_CONFIG.get('host', 'localhost'),
                'port': DB_CONFIG.get('port', 3306),
                'user': DB_CONFIG.get('user', 'root'),
                'password': DB_CONFIG.get('password', '1234'),
                'database': DB_CONFIG.get('database', 'py_spider'),
                'charset': DB_CONFIG.get('charset', 'utf8mb4'),
                'cursorclass': pymysql.cursors.DictCursor,  # 返回字典格式
                'autocommit': True
            }

            self.pool = PooledDB(
                creator=pymysql,
                maxconnections=20,  # 最大连接数
                mincached=5,  # 初始化时创建的空闲连接数
                maxcached=10,  # 最大空闲连接数
                maxshared=0,  # 最大共享连接数（0表示不共享）
                blocking=True,  # 连接池满时是否阻塞等待
                maxusage=None,  # 单个连接最大复用次数
                setsession=[],  # 开始会话时执行的SQL
                ping=1,  # ping检查连接是否有效
                **db_config
            )

            self.logger.info("✅ 数据库连接池初始化成功")

        except Exception as e:
            self.logger.error(f"❌ 数据库连接池初始化失败: {e}")
            raise

    def _init_tables(self):
        """初始化数据库表结构"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # 创建商品表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS products_auto (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source VARCHAR(255),
    goods_id VARCHAR(50) NOT NULL UNIQUE,
    name TEXT,
    category VARCHAR(100),
    sub_category VARCHAR(100),
    month_sale VARCHAR(100),
    status VARCHAR(20) DEFAULT 'pending',
    processing_at DATETIME NULL,                       -- 新增字段，位于 status 之后
    shop_id VARCHAR(50),
    worker_id VARCHAR(50),
    retry_count INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_status (status),
    INDEX idx_goods_id (goods_id),
    INDEX idx_category (category),
    INDEX idx_processing_at (processing_at)            -- 新增索引
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            conn.commit()
            cursor.close()
            conn.close()

            self.logger.info("✅ 数据库表初始化成功")

        except Exception as e:
            self.logger.error(f"❌ 数据库表初始化失败: {e}")
            raise

    def get_connection(self):
        """获取数据库连接"""
        try:
            return self.pool.connection()
        except Exception as e:
            self.logger.error(f"获取数据库连接失败: {e}")
            raise

    def close_all(self):
        """关闭所有连接（一般不需要手动调用）"""
        try:
            if hasattr(self.pool, 'close'):
                self.pool.close()
                self.logger.info("数据库连接池已关闭")
        except Exception as e:
            self.logger.error(f"关闭数据库连接池失败: {e}")

    def get_pool_status(self):
        """获取连接池状态"""
        try:
            return {
                'max_connections': getattr(self.pool, '_maxconnections', 0),
                'min_cached': getattr(self.pool, '_mincached', 0),
                'max_cached': getattr(self.pool, '_maxcached', 0),
            }
        except:
            return {}