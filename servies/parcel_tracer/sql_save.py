from core.BaseStorage import BaseStorage
from typing import List,Dict
import time
import hashlib
import json

import time
from datetime import datetime

class StockInStorage(BaseStorage):
    def create_tables(self):
        sql = """
                CREATE TABLE IF NOT EXISTS shopee_purchase_stock_record (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    crawl_time_ms BIGINT NOT NULL,
                    shop_name VARCHAR(100) NOT NULL,
                    purchase_order_sn VARCHAR(64) NOT NULL,
                    deliver_quantity INT DEFAULT 0,
                    receive_quantity INT DEFAULT 0,
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
                   INSERT INTO shopee_purchase_stock_record (
                       crawl_time_ms,
                       shop_name,
                       purchase_order_sn,
                       deliver_quantity,
                       receive_quantity,
                       receive_time_ms
                   ) VALUES (%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE
                       deliver_quantity = VALUES(deliver_quantity),
                       receive_quantity = VALUES(receive_quantity),
                       receive_time_ms = VALUES(receive_time_ms),
                       crawl_time_ms = VALUES(crawl_time_ms);
                   """

            values = [
                (
                    int(i["数据爬取日期"]),
                    str(i["店铺"]),
                    str(i["入库ID"]),
                    int(i["送货数"]),
                    int(i["入库数"]),
                    self._convert_to_timestamp(i["实际入库时间"]),  # 转换时间字符串为时间戳
                )
                for i in items
            ]

            affected_rows = self.cursor.executemany(sql, values)
            self.logger.info(f"✅ 成功插入/更新 {affected_rows} 条数据")

        except Exception as e:
            self.logger.error(f"❌ 批量插入失败: {e}")
            # 添加详细错误日志以便调试
            for idx, item in enumerate(items):
                self.logger.error(f"问题数据行 {idx}: {item}")

    def _convert_to_timestamp(self, time_str):
        """将时间字符串转换为毫秒级时间戳"""
        if not time_str or time_str == '':
            return None

        try:
            # 如果是已经是数字（时间戳），直接返回
            if isinstance(time_str, (int, float)):
                return int(time_str)

            # 如果是字符串，尝试转换
            if isinstance(time_str, str):
                # 处理 '2025-12-19 18:12:20' 格式
                dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                # 转换为毫秒时间戳
                timestamp_ms = int(dt.timestamp() * 1000)
                return timestamp_ms

        except Exception as e:
            self.logger.warning(f"时间转换失败: {time_str}, 错误: {e}")
            return None

    def filter_new_items(self, items: List[Dict]) -> List[Dict]:
        new_items = []
        for i in items:
            if not self.redis_is_duplicate_permanent(i["入库ID"]):
                new_items.append(i)
                self.logger.info(f"📝 新增订单: {i['入库ID']}")
        self.logger.info(f"📊 去重后新增 {len(new_items)} 条数据")
        return new_items

    def detect_abnormal(self, items: List[Dict]) -> List[Dict]:
        return [
            i for i in items
            if int(i["送货数"]) != int(i["入库数"])
        ]

    def alarm_abnormal(self, abnormal_items: List[Dict], logger):
        for i in abnormal_items:
            logger.warning(
                f"🚨 入库异常 | 店铺={i['店铺']} | 入库ID={i['入库ID']} "
                f"| 送货数量={i['送货数']} | 入库数量={i['入库数']}"
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
        CREATE TABLE IF NOT EXISTS shopee_delivery_note_record (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            crawl_time_ms BIGINT NOT NULL COMMENT '抓取时间戳',
            shop_name VARCHAR(100) NOT NULL COMMENT '店铺名称',
            inbound_id VARCHAR(100) COMMENT '入库ID',
            inbound_qty INT COMMENT '到货数量',
            logistics_trajectory TEXT COMMENT '物流轨迹(JSON格式)',
            mark_status VARCHAR(50) DEFAULT '正常' COMMENT '标记状态',
            mark_reason VARCHAR(200) COMMENT '标记原因',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
            INDEX idx_shop_name (shop_name),
            INDEX idx_inbound_id (inbound_id),
            INDEX idx_mark_status (mark_status),
            INDEX idx_crawl_time_ms (crawl_time_ms)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发货单记录表';
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
        INSERT INTO shopee_delivery_note_record(
            crawl_time_ms,
            shop_name,
            inbound_id,
            inbound_qty,
            logistics_trajectory,
            mark_status,
            mark_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            inbound_qty = VALUES(inbound_qty),
            logistics_trajectory = VALUES(logistics_trajectory),
            mark_status = VALUES(mark_status),
            mark_reason = VALUES(mark_reason),
            updated_at = CURRENT_TIMESTAMP
        """

        try:
            values = []
            for item in items:
                # 处理物流轨迹，确保是JSON字符串
                logistics_trajectory = item.get('物流轨迹', [])
                if isinstance(logistics_trajectory, (list, dict)):
                    logistics_trajectory = json.dumps(logistics_trajectory, ensure_ascii=False)
                elif logistics_trajectory is None:
                    logistics_trajectory = '[]'

                values.append((
                    item.get("数据抓取日期", int(time.time() * 1000)),
                    item.get('店铺', ''),
                    item.get("入库ID", ''),
                    item.get("到货数量", 0),
                    logistics_trajectory,
                    item.get('标记状态', '正常'),
                    item.get("标记原因", '')
                ))

            affected_rows = self.cursor.executemany(sql, values)
            self.logger.info(f"✅ 成功插入/更新 {len(values)} 条数据")

        except Exception as e:
            self.logger.error(f"❌ 批量插入失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def build_dedup_key(self, item: dict) -> str:
        """
        构建去重键：入库ID + 标记状态 + 物流轨迹指纹
        """
        trace_hash = trace_fingerprint(item.get('物流轨迹', []))
        status = item.get('标记状态', "正常")
        inbound_id = item.get("入库ID", "")

        return f"{self.redis_prefix}:{inbound_id}:{status}:{trace_hash}"

    def is_new(self,item:dict)->bool:
        key=self.build_dedup_key(item)
        return self.redis.set(key, 1, nx=True)  # 移除ex参数 # True  - 设置成功（键原来不存在）

    def filter_new_items(self, items: list) -> list:
        new_items = []
        for item in items:
            if self.is_new(item):
                new_items.append(item)
                self.logger.info(f"📝 新增入库记录: {item.get('入库ID', '')}")
        self.logger.info(f"📊 去重后新增 {len(new_items)} 条数据")
        return new_items

    def detect_abnormal(self,items):
        return [i for i in items if i['标记状态']!="正常"]


class DeliveryNoteStoragel(BaseStorage):

    def create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS shopee_delivery_note_record12 (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            crawl_time_ms BIGINT NOT NULL COMMENT '抓取时间戳',
            shop_name VARCHAR(100) NOT NULL COMMENT '店铺名称',
            inbound_id VARCHAR(100) COMMENT '入库ID',
            create_time DATETIME COMMENT '创建时间',
            order_status VARCHAR(50) COMMENT '订单状态',
            logistics_status VARCHAR(50) COMMENT '物流状态',
            logistics_trajectory TEXT COMMENT '物流轨迹(JSON格式)',
            mark_status VARCHAR(50) DEFAULT '正常' COMMENT '标记状态',
            mark_reason VARCHAR(200) COMMENT '标记原因',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
            INDEX idx_shop_name (shop_name),
            INDEX idx_inbound_id (inbound_id),
            INDEX idx_order_status (order_status),
            INDEX idx_logistics_status (logistics_status),
            INDEX idx_mark_status (mark_status),
            INDEX idx_crawl_time_ms (crawl_time_ms),
            INDEX idx_create_time (create_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发货单记录表';
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
        INSERT INTO shopee_delivery_note_record12(
            crawl_time_ms,
            shop_name,
            inbound_id,
            create_time,
            order_status,
            logistics_status,
            logistics_trajectory,
            mark_status,
            mark_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            create_time = VALUES(create_time),
            order_status = VALUES(order_status),
            logistics_status = VALUES(logistics_status),
            logistics_trajectory = VALUES(logistics_trajectory),
            mark_status = VALUES(mark_status),
            mark_reason = VALUES(mark_reason),
            updated_at = CURRENT_TIMESTAMP
        """

        try:
            values = []
            for item in items:
                # 处理物流轨迹，确保是JSON字符串
                logistics_trajectory = item.get('物流轨迹', [])
                if isinstance(logistics_trajectory, (list, dict)):
                    logistics_trajectory = json.dumps(logistics_trajectory, ensure_ascii=False)
                elif logistics_trajectory is None:
                    logistics_trajectory = '[]'

                values.append((
                    item.get("数据抓取日期", int(time.time() * 1000)),
                    item.get('店铺', ''),
                    item.get("入库ID", ''),
                    item.get("创建时间", None),
                    item.get("订单状态", ''),
                    item.get("物流状态", ''),
                    logistics_trajectory,
                    item.get('标记状态', '正常'),
                    item.get('标记原因', '')
                ))

            affected_rows = self.cursor.executemany(sql, values)
            self.logger.info(f"✅ 成功插入/更新 {len(values)} 条数据")

        except Exception as e:
            self.logger.error(f"❌ 批量插入失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def build_dedup_key(self, item: dict) -> str:
        """
        构建去重键：入库ID + 订单状态 + 物流状态 + 物流轨迹指纹
        """
        trace_hash = trace_fingerprint(item.get('物流轨迹', []))
        order_status = item.get('订单状态', "")
        logistics_status = item.get('物流状态', "")
        inbound_id = item.get("入库ID", "")

        return f"{self.redis_prefix}:{inbound_id}:{order_status}:{logistics_status}:{trace_hash}"

    def is_new(self,item:dict)->bool:
        key=self.build_dedup_key(item)
        return self.redis.set(key, 1, nx=True)  # True - 设置成功（键原来不存在）

    def filter_new_items(self, items: list) -> list:
        new_items = []
        for item in items:
            if self.is_new(item):
                new_items.append(item)
                self.logger.info(f"📝 新增入库记录: {item.get('入库ID', '')}")
        self.logger.info(f"📊 去重后新增 {len(new_items)} 条数据")
        return new_items

    def detect_abnormal(self,items):
        return [i for i in items if i['标记状态']!="正常"]





