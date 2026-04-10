# storage/base_dao.py
from contextlib import contextmanager
from storage.db_pool import DatabasePool
from utils.logger import get_logger
import time
from typing import Optional, List, Dict, Any


class BaseDAO:
    """数据库操作基类，提供连接池管理和通用方法"""

    def __init__(self,job):
        self.db_pool = DatabasePool(job)
        self.logger = get_logger(job)

    @contextmanager
    def get_cursor(self, commit=True):
        """获取数据库游标的上下文管理器"""
        conn = None
        cursor = None
        start_time = time.time()

        try:
            # 从连接池获取连接
            conn = self.db_pool.get_connection()
            cursor = conn.cursor()

            yield cursor

            if commit:
                conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"数据库操作失败: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

            # 记录慢查询
            elapsed = time.time() - start_time
            if elapsed > 1.0:  # 超过1秒记录警告
                self.logger.warning(f"慢查询耗时: {elapsed:.2f}秒")

    def execute(self, sql: str, params: tuple = None) -> int:
        """执行SQL语句"""
        with self.get_cursor() as cursor:
            affected_rows = cursor.execute(sql, params)
            return affected_rows

    def executemany(self, sql: str, params_list: List[tuple]) -> int:
        """批量执行SQL语句"""
        with self.get_cursor() as cursor:
            affected_rows = cursor.executemany(sql, params_list)
            return affected_rows

    def fetchone(self, sql: str, params: tuple = None) -> Optional[Dict]:
        """查询单条记录"""
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def fetchall(self, sql: str, params: tuple = None) -> List[Dict]:
        """查询多条记录"""
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def fetch_page(self, sql: str, params: tuple = None,
                   page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        """分页查询"""
        # 计算总数
        count_sql = f"SELECT COUNT(*) as total FROM ({sql}) as t"
        total_result = self.fetchone(count_sql, params)
        total = total_result['total'] if total_result else 0

        # 分页查询
        offset = (page - 1) * page_size
        page_sql = f"{sql} LIMIT {page_size} OFFSET {offset}"
        data = self.fetchall(page_sql, params)

        return {
            'total': total,
            'page': page,
            'page_size': page_size,
            'total_pages': (total + page_size - 1) // page_size,
            'data': data
        }

    def transaction(self, operations: List[tuple]):
        """事务操作
        operations: [(sql, params), ...]
        """
        with self.get_cursor(commit=False) as cursor:
            for sql, params in operations:
                cursor.execute(sql, params)
            # 上下文管理器会自动提交