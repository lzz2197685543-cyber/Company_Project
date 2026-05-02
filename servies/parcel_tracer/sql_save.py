from core.BaseStorage import BaseStorage
from typing import List,Dict
import time
import hashlib
import json


class StockInStorage(BaseStorage):
    def create_tables(self):
        sql = """
                CREATE TABLE IF NOT EXISTS shein_purchase_stock_record (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    crawl_time_ms BIGINT NOT NULL,
                    shop_name VARCHAR(100) NOT NULL,
                    purchase_order_sn VARCHAR(64) NOT NULL,
                    deliver_quantity INT DEFAULT 0,
                    receive_quantity INT DEFAULT 0,
                    deliver_time_ms BIGINT NULL,
                    receive_time_ms BIGINT NULL,
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_purchase_order_sn (purchase_order_sn)
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
                   INSERT INTO shein_purchase_stock_record (
                       crawl_time_ms,
                       shop_name,
                       purchase_order_sn,
                       deliver_quantity,
                       receive_quantity,
                       deliver_time_ms,
                       receive_time_ms
                   ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE
                       deliver_quantity = VALUES(deliver_quantity),
                       receive_quantity = VALUES(receive_quantity),
                       deliver_time_ms = VALUES(deliver_time_ms),
                       receive_time_ms = VALUES(receive_time_ms),
                       crawl_time_ms = VALUES(crawl_time_ms);
                   """

            values = [
                (
                    int(i["数据爬取日期"]),
                    str(i["店铺"]),
                    str(i["订单号"]),
                    int(i["送货数量"]),
                    int(i["上架数量"]),
                    i["发货时间"],
                    i["上架时间"]
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
            if not self.redis_is_duplicate_permanent(i["订单号"]):
                new_items.append(i)
                self.logger.info(f"📝 新增订单: {i['订单号']}")
        self.logger.info(f"📊 去重后新增 {len(new_items)} 条数据")
        return new_items


    def detect_abnormal(self, items: List[Dict]) -> List[Dict]:
        return [
            i for i in items
            if int(i["送货数量"]) != int(i["上架数量"])
        ]

    def alarm_abnormal(self, abnormal_items: List[Dict], logger):
        for i in abnormal_items:
            logger.warning(
                f"🚨 入库异常 | 店铺={i['店铺']} | 订单号={i['订单号']} "
                f"| 送货数量={i['送货数量']} | 上架数量={i['上架数量']}"
            )


def trace_fingerprint(traces:list)->str:
    """
    物流轨迹指纹（顺序敏感）
    """
    raw=json.dumps(traces,ensure_ascii=False)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

class DeliveryNoteStorage(BaseStorage):

    def create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS shein_delivery_note_record (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            crawl_time_ms BIGINT NOT NULL,
            shop_name VARCHAR(100) NOT NULL,
            purchase_order_sn VARCHAR(100) NOT NULL,
            order_status VARCHAR(100),
            delivery_time VARCHAR(50),
            receipt_time VARCHAR(50),
            logistics_trajectory TEXT,
            mark_status VARCHAR(50),
            mark_reason VARCHAR(200),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        try:
            self.cursor.execute(sql)
            self.logger.info("✅ 表创建成功或已存在")
        except Exception as e:
            self.logger.error(f"❌ 创建表失败: {e}")

    def batch_insert(self, items):
        if not items:
            self.logger.info("⚠️ 没有要插入的数据")
            return

        sql = """
        INSERT INTO shein_delivery_note_record(
            crawl_time_ms,
            shop_name,
            purchase_order_sn,
            order_status,
            delivery_time,
            receipt_time,
            logistics_trajectory,
            mark_status,
            mark_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            order_status=VALUES(order_status),
            receipt_time=VALUES(receipt_time),
            logistics_trajectory=VALUES(logistics_trajectory),
            mark_status=VALUES(mark_status),
            mark_reason=VALUES(mark_reason)
        """

        try:
            values = [
                (i["数据抓取日期"],
                 i['店铺'],
                 i["订单号"],
                 i['订单状态'],
                 i['发货时间'],
                 i['收货时间'],
                 json.dumps(i['物流轨迹'], ensure_ascii=False),  # 确保轨迹是 JSON 字符串
                 i['标记状态'],
                 i["标记原因"])
                for i in items
            ]

            affected_rows = self.cursor.executemany(sql, values)
            self.logger.info(f"✅ 成功插入/更新 {affected_rows} 条数据")

        except Exception as e:
            self.logger.error(f"❌ 批量插入失败: {e}")


    def build_dedup_key(self,item:dict)->str:
        trace_hash=trace_fingerprint(item.get('物流轨迹',[]))
        status=item.get('标记状态',"UNKNOWN")
        order_sn = item["订单号"]

        return f"{self.redis_prefix}:{order_sn}:{status}:{trace_hash}"

    def is_new(self,item:dict)->bool:
        key=self.build_dedup_key(item)
        return self.redis.set(key, 1, nx=True)  # 移除ex参数 # True  - 设置成功（键原来不存在）

    def filter_new_items(self, items: list) -> list:
        new_items = []
        for item in items:
            if self.is_new(item):
                new_items.append(item)

                self.logger.info(f"📝 新增订单: {item['订单号']}")
        self.logger.info(f"📊 去重后新增 {len(new_items)} 条数据")
        return new_items

    def detect_abnormal(self,items):
        return [i for i in items if i['标记状态']!="正常"]








