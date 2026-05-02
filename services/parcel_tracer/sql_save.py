from core.BaseStorage import BaseStorage
from typing import List, Dict
import time
import hashlib
import json


class StockInStorage(BaseStorage):
    def create_tables(self):
        sql = """
                CREATE TABLE IF NOT EXISTS smt_purchase_stock_record (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    crawl_time_ms BIGINT NOT NULL,
                    shop_name VARCHAR(100) NOT NULL,
                    purchase_order_sn VARCHAR(64) NOT NULL,
                    deliver_quantity INT DEFAULT 0,
                    receive_quantity INT DEFAULT 0,
                    listing_quantity INT DEFAULT 0,
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_purchase_order_sn (purchase_order_sn),
                    INDEX idx_shop_name (shop_name),
                    INDEX idx_crawl_time (crawl_time_ms)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
        try:
            self.cursor.execute(sql)
            self.logger.info("✅ 表创建成功或已存在")
        except Exception as e:
            self.logger.error(f"❌ 创建表失败: {e}")

    def batch_insert(self, items: List[Dict]):
        if not items:
            self.logger.info("⚠️ 没有要插入的数据")
            return
        try:
            sql = """
                   INSERT INTO smt_purchase_stock_record (
                       crawl_time_ms,
                       shop_name,
                       purchase_order_sn,
                       deliver_quantity,
                       receive_quantity,
                       listing_quantity
                   ) VALUES (%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE
                       deliver_quantity = VALUES(deliver_quantity),
                       receive_quantity = VALUES(receive_quantity),
                       listing_quantity = VALUES(listing_quantity),
                       crawl_time_ms = VALUES(crawl_time_ms);
                   """

            values = [
                (
                    int(i["数据爬取日期"]),
                    str(i["店铺"]),
                    str(i["备货单号"]),
                    int(i["发货数量"]),
                    int(i["总收货数量"]),  # 注意这里从 "入库数量" 获取值
                    int(i['总上架数量'])
                )
                for i in items
            ]

            affected_rows = self.cursor.executemany(sql, values)
            self.logger.info(f"✅ 成功插入/更新 {affected_rows} 条数据")

        except Exception as e:
            self.logger.error(f"❌ 批量插入失败: {e}")

    def filter_new_items(self, items: List[Dict]) -> List[Dict]:
        new_items = []
        for i in items:
            if not self.redis_is_duplicate_permanent(i["备货单号"]):
                new_items.append(i)
                self.logger.info(f"📝 新增订单: {i['备货单号']}")
        self.logger.info(f"📊 去重后新增 {len(new_items)} 条数据")
        return new_items