import requests
import time
from utils.cookie_manager import CookieManager
from utils.logger import get_logger
import asyncio
import pymysql
import hashlib
import redis
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from utils.dingtalk_bot import ding_bot_send

"""违规记录——数据获取"""

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


class Temu_ViolationRecored:
    def __init__(self, shop_name):
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.logger = get_logger(f"temu_violation_recored")

        self.redis_client = redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True
        )

        self.db = pymysql.connect(host='localhost',
                                  user='root',
                                  password='1234',
                                  database='py_spider'
                                  )  # 数据库名字

        # 使用cursor()方法获取操作游标
        self.cursor = self.db.cursor()

        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.url = 'https://agentseller.temu.com/mms/api/andes/punish/seller/listPunishRecord'

    """获取爬取数据当天0点到24点的时间戳"""
    def get_day_range_ms(self,target_date: date | None = None):
        if target_date is None:
            target_date = date.today()

        start_dt = datetime.combine(target_date, time.min, tzinfo=BEIJING_TZ)
        end_dt = start_dt + timedelta(days=1)

        return (
            int(start_dt.timestamp() * 1000),
            int(end_dt.timestamp() * 1000)
        )

    def get_info(self, cookies, shop_id):
        self.headers['mallid'] = str(shop_id)

        start_ms, end_ms = self.get_day_range_ms()

        json_data = {
            'pageSize': 100,
            'pageNo': 1,
            'startDateFrom': start_ms,
            'startDateTo': end_ms,
            'punishTabCode': 2,
        }

        try:
            response = requests.post(
                url=self.url,
                cookies=cookies,
                headers=self.headers,
                json=json_data,
                timeout=10,
            )
            print(response.text[:200])
            # self.logger.info(response.text)

            # ❗ 不管 status code，先尝试解析 JSON
            try:
                data = response.json()
            except ValueError:
                self.logger.error(f"❌ 响应不是 JSON: {response.text[:200]}")
                ding_bot_send('me', f"{self.shop_name}❌ ❌ 响应不是 JSON: {response.text[:100]}")
                return None

            # 打印非 200 但有业务错误的情况
            if response.status_code != 200:
                self.logger.error(f'状态不是200：{response.text[:200]}')
                self.logger.error(f"⚠️ HTTP异常: {response.status_code}")
                return data

            return data

        except requests.exceptions.Timeout:
            self.logger.error("❌ 请求超时")
            ding_bot_send('me',f"{self.shop_name}❌ 请求超时")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 请求异常: {e}")
            ding_bot_send('me', f"{self.shop_name}❌ 请求异常: {e}")
        return None

    def safe_datetime_from_ms(self, ts_ms):
        """
        毫秒时间戳 → datetime
        无效值返回 None（MySQL DATETIME 可接受）
        """
        try:
            if not ts_ms or int(ts_ms) <= 0:
                return None
            return datetime.fromtimestamp(int(ts_ms) / 1000)
        except Exception:
            return None

    def parse_data(self, json_data):
        records = json_data.get("sale", {}).get("list", [])

        if not records:
            self.logger.info("暂无违规数据")
            return

        for i in records:
            try:
                item = {
                    '店铺': self.shop_name,
                    '违规编号': i.get('punishSn'),
                    '违规类型': i.get('punishTypeDesc'),
                    '违规颗粒': i.get('qualityEventPunishDimensionDesc'),
                    '备货单号': i.get('subPurchaseOrderSn', ''),
                    'SKU': i.get('productSkcId', ''),
                    '违规发起时间': self.safe_datetime_from_ms(i.get('violationStartTime')),
                    '违规金额': float(i.get('punishAmount', 0))/100,
                    '进度': i.get('punishStatusDesc'),
                    '数据抓取时间': datetime.now(),
                }

                # ✅ 判断是否需要更新
                if not self.need_update(item):
                    self.logger.info(
                        f"[跳过] 违规编号 {item['违规编号']} 无变化"
                    )
                    continue

                self.save_record(item)
                # self.logger.info(
                #     f"[更新] 违规编号 {item['违规编号']} 状态={item['进度']}"
                # )

            except Exception as e:
                self.logger.error(f"解析/保存数据失败: {e}")

    # ================= 去重 =================
    def need_update(self, item: dict) -> bool:
        """
        判断该违规记录是否发生变化
        """
        key = f"temu:violation:{self.shop_name}:{item['违规编号']}"

        # 只对关键字段做 hash
        hash_source = f"{item['违规编号']}|{item['进度']}|{item['违规金额']}"
        new_hash = hashlib.md5(hash_source.encode("utf-8")).hexdigest()

        old_hash = self.redis_client.get(key)

        # 第一次出现
        if not old_hash:
            self.redis_client.set(key, new_hash)
            return True

        # 发生变化
        if old_hash != new_hash:
            self.redis_client.set(key, new_hash)
            return True

        # 完全没变化
        return False

    """创建数据表"""
    def create_table(self):
        sql = """
            CREATE TABLE IF NOT EXISTS temu_violation_record (
                id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键',
                店铺 VARCHAR(64) NOT NULL COMMENT '店铺名称',
                违规编号 VARCHAR(64) NOT NULL COMMENT '处罚单号',
                违规类型 VARCHAR(100) COMMENT '处罚类型',
                违规颗粒 VARCHAR(100) COMMENT '处罚维度',
                备货单号 VARCHAR(64) COMMENT '子订单号',
                SKU VARCHAR(64) COMMENT '商品SKC',
                违规发起时间 DATETIME COMMENT '违规开始时间',
                违规金额 DECIMAL(10,2) DEFAULT 0 COMMENT '处罚金额',
                进度 VARCHAR(50) COMMENT '处罚状态',
                数据抓取时间 DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '抓取时间',
                UNIQUE KEY uk_shop_punish (店铺, 违规编号)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        try:

            self.cursor.execute(sql)
            self.db.commit()
            self.logger.info("违规记录表检查 / 创建完成")
        except Exception as e:
            self.logger.error(f"创建表失败: {e}")
            self.db.rollback()

    """插入数据"""
    def save_record(self, item: dict):
        sql = """
            INSERT INTO temu_violation_record (
                店铺,
                违规编号,
                违规类型,
                违规颗粒,
                备货单号,
                SKU,
                违规发起时间,
                违规金额,
                进度,
                数据抓取时间
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                进度 = VALUES(进度),
                违规金额 = VALUES(违规金额),
                数据抓取时间 = VALUES(数据抓取时间);
            """

        try:
            self.cursor.execute(sql, (
                self.shop_name,
                item['违规编号'],
                item['违规类型'],
                item['违规颗粒'],
                item['备货单号'],
                item['SKU'],
                item['违规发起时间'],
                item['违规金额'],
                item['进度'],
                item['数据抓取时间']
            ))
            self.db.commit()
            self.logger.info(f'{item["违规编号"]}---保存成功')
        except Exception as e:
            self.logger.error(f"入库失败 punish_sn={item.get('违规编号')}: {e}")
            self.db.rollback()

    async def run(self):
        self.logger.info(f'正在爬取店铺---------------{self.shop_name}-------------的数据')

        # 创建数据表
        self.create_table()

        for attempt in range(5):  # 最多尝试 5次
            cookies, shop_id = await self.cookie_manager.get_auth()

            json_data = self.get_info(cookies, shop_id)

            if not json_data:
                self.logger.error("接口返回为空")
                return

            # ✅ 登录态失效
            if json_data.get("error_msg") == "Invalid Login State":
                if attempt == 0:
                    self.logger.warning(
                        f"[{self.shop_name}] cookie 失效，开始自动刷新登录态"
                    )
                    await self.cookie_manager.refresh()
                    await asyncio.sleep(2)
                    continue
                else:
                    self.logger.error(
                        f"[{self.shop_name}] 刷新 cookie 后仍然失效，终止任务"
                    )
                    # 将刷新时候cookie还是失败的发送给自己
                    ding_bot_send('me',f"[{self.shop_name}] 刷新 cookie 后仍然失效，终止任务")
                    return

            # ✅ 正常数据
            self.parse_data(json_data)
            return


# if __name__ == '__main__':
#     t = Temu_ViolationRecored("102-Temu全托管")
#     asyncio.run(t.run())
