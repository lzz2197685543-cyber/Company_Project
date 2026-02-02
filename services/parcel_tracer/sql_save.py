from core.BaseStorage import BaseStorage
from typing import List, Dict
import time
import hashlib
import json

class StockInStorage(BaseStorage):

    def create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS temu_purchase_stock_record (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            crawl_time_ms BIGINT NOT NULL,
            shop_name VARCHAR(100) NOT NULL,
            purchase_order_sn VARCHAR(64) NOT NULL,
            purchase_create_time_ms BIGINT,
            deliver_quantity INT DEFAULT 0,
            receive_quantity INT DEFAULT 0,
            deliver_time_ms BIGINT,
            receive_time_ms BIGINT,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_purchase_order_sn (purchase_order_sn)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        self.cursor.execute(sql)

    def batch_insert(self, items: List[Dict]):
        if not items:
            return

        sql = """
        INSERT INTO temu_purchase_stock_record (
            crawl_time_ms,
            shop_name,
            purchase_order_sn,
            purchase_create_time_ms,
            deliver_quantity,
            receive_quantity,
            deliver_time_ms,
            receive_time_ms
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            deliver_quantity = VALUES(deliver_quantity),
            receive_quantity = VALUES(receive_quantity),
            deliver_time_ms = VALUES(deliver_time_ms),
            receive_time_ms = VALUES(receive_time_ms),
            crawl_time_ms = VALUES(crawl_time_ms);
        """

        values = [
            (
                i["数据抓取时间"],
                i["店铺"],
                i["备货单号"],
                i["备货单创建时间"],
                i["送货数"],
                i["入库数"],
                i["交接时间"],
                i["收货时间"]
            )
            for i in items
        ]

        self.cursor.executemany(sql, values)

    def filter_new_items(self, items: List[Dict]) -> List[Dict]:
        new_items = []
        for i in items:
            if not self.redis_is_duplicate_permanent(i["备货单号"]):
                new_items.append(i)
        return new_items

    def detect_abnormal(self, items: List[Dict]) -> List[Dict]:
        return [
            i for i in items
            if int(i["送货数"]) != int(i["入库数"])
        ]

    def alarm_abnormal(self, abnormal_items: List[Dict], logger):
        for i in abnormal_items:
            logger.warning(
                f"🚨 入库异常 | 店铺={i['店铺']} | 备货单={i['备货单号']} "
                f"| 送货={i['送货数']} | 入库={i['入库数']}"
            )

    def build_abnormal_message(self, abnormal_items, shop_name):
        if not abnormal_items:
            return None

        lines = []
        lines.append("🚨【Temu 入库异常报警】")
        lines.append(f"店铺：{shop_name}")
        lines.append(f"异常单数：{len(abnormal_items)}\n")

        for idx, i in enumerate(abnormal_items, start=1):
            diff = int(i["入库数"]) - int(i["送货数"])
            sign = "+" if diff > 0 else ""

            lines.append(
                f"{idx}️⃣ 备货单：{i['备货单号']}\n"
                f"   送货：{i['送货数']}  入库：{i['入库数']}  ❌ 差：{sign}{diff}"
            )

        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        lines.append(f"\n⏰ 抓取时间：{now}")

        return "\n".join(lines)


def trace_fingerprint(traces: list) -> str:
    """
    物流轨迹指纹（顺序敏感）
    """
    raw = json.dumps(traces, ensure_ascii=False)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


class DeliveryNoteStorage(BaseStorage):

    def create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS temu_delivery_note_record (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    crawl_time_ms BIGINT NOT NULL,
    shop_name VARCHAR(100) NOT NULL,
    purchase_order_sn VARCHAR(64) NOT NULL,
    logistics_no VARCHAR(128),
    package_status VARCHAR(50),
    delivery_method VARCHAR(50),
    expect_pickup_time_ms BIGINT,
    mark_status VARCHAR(50),
    mark_reason VARCHAR(255),
    logistics_traces TEXT,
    trace_hash CHAR(32) NOT NULL,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_order_status_trace (purchase_order_sn, mark_status, trace_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        """
        self.cursor.execute(sql)

    def batch_insert(self, items):
        if not items:
            return

        sql = """
        INSERT INTO temu_delivery_note_record (
            crawl_time_ms,
    shop_name,
    purchase_order_sn,
    logistics_no,
    package_status,
    delivery_method,
    expect_pickup_time_ms,
    mark_status,
    mark_reason,
    logistics_traces,
    trace_hash
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            package_status = VALUES(package_status),
            mark_status = VALUES(mark_status),
            mark_reason = VALUES(mark_reason),
            logistics_traces = VALUES(logistics_traces),
            crawl_time_ms = VALUES(crawl_time_ms);
        """

        values = [
            (i["数据抓取时间"],
             i["店铺"],
             i["备货单号"],
             i["物流单号"],
             i["包裹状态"],
             i["发货方式"],
             i.get("预约取货时间"),
             i["标记状态"],
             i.get("标记原因", ""),
             "\n".join(i["物流轨迹"]),
             trace_fingerprint(i["物流轨迹"])
             )
            for i in items
        ]

        self.cursor.executemany(sql, values)

    def build_dedup_key(self, item: dict) -> str:
        trace_hash = trace_fingerprint(item.get("物流轨迹", []))
        status = item.get("标记状态", "UNKNOWN")
        order_sn = item["备货单号"]

        return f"{self.redis_prefix}:{order_sn}:{status}:{trace_hash}"

    def is_new(self, item: dict) -> bool:
        """检查是否是新数据（永不过期）"""
        key = self.build_dedup_key(item)
        return self.redis.set(key, 1, nx=True)  # 移除ex参数

    def filter_new_items(self, items: list) -> list:
        new_items = []

        for item in items:
            if self.is_new(item):
                new_items.append(item)

        return new_items

    def detect_abnormal(self, items):
        return [i for i in items if i["标记状态"] != "正常"]

    def alarm_abnormal(self, abnormal_items: List[Dict], logger):
        for i in abnormal_items:
            logger.warning(
                f"🚨 入库异常 | 店铺={i['店铺']} "
                f" 备货单号：{i['备货单号']}\n"
                f" 状态：{i['标记状态']}\n"
                f" 异常原因：{i.get('标记原因', '')}"
            )

    def build_delivery_abnormal_message(self, abnormal_items, shop_name):
        if not abnormal_items:
            return None

        lines = []
        lines.append("🚨【Temu 发货单异常报警】")
        lines.append(f"店铺：{shop_name}")
        lines.append(f"异常包裹数：{len(abnormal_items)}\n")

        for idx, i in enumerate(abnormal_items, 1):
            lines.append(
                f" {idx} ：备货单号：{i['备货单号']}\n"
                f" 状态：{i['标记状态']}\n"
                f" 异常原因：{i.get('标记原因', '')}"
            )

        now = time.strftime("%Y-%m-%d %H:%M:%S")
        lines.append(f"\n⏰ 抓取时间：{now}")

        return "\n".join(lines)
