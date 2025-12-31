import requests
from utils.cookie_manager import CookieManager
from utils.logger import get_logger
import asyncio
import hashlib
import redis
import pymysql
from datetime import datetime
from utils.dingtalk_bot import ding_bot_send

"""金额限制--数据获取"""

class Temu_Funds_Restriction:
    def __init__(self,shop_name):
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.logger = get_logger(f"funds_restriction")

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
        self.cursor = self.db.cursor()
        self.cookies=None
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        self.url='https://agentseller.temu.com/api/merchant/fund-frozen/rules'

    def get_info(self,cookies,shop_id):
        self.headers['mallid'] = str(shop_id)
        json_data = {}
        self.cookies = cookies

        try:
            response = requests.post(
                url=self.url,
                cookies=self.cookies,
                headers=self.headers,
                json=json_data
            )
            print(response.text[:200])
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
            ding_bot_send('me', f"{self.shop_name}❌ 请求超时")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 请求异常: {e}")
            ding_bot_send('me', f"{self.shop_name}❌ 请求异常: {e}")
        return None

    def get_detail_info(self,frozen_type):
        json_data = {
            'pageNum': 1,
            'pageSize': 10,
            'frozenType': f'{frozen_type}',
        }
        try:

            response = requests.post(
                'https://agentseller.temu.com/api/merchant/fund-frozen/details',
                cookies=self.cookies,
                headers=self.headers,
                json=json_data,
            )
            print(response.text[:200])
            freeze_ts=response.json()['result']['detailsRows'][0]['freezeStartTime']
            return datetime.fromtimestamp(freeze_ts / 1000)
        except Exception as e:
            self.logger.error(f'获取限制时间失败')

    def parse_data(self, json_data):
        rules = json_data.get("result", {}).get('rules', [])

        if not rules:
            self.logger.info("暂无资金限制数据")
            return

        for i in rules:
            try:
                item = {
                    '店铺': self.shop_name,
                    '冻结类型': i['frozenType'],  # ✅ 必须字段
                    '限制原因': i.get('reason'),
                    '限制总金额': str(i.get('amount', 0)).replace("￥", "").replace(',','').strip(),
                    '限制时间': self.get_detail_info(i['frozenType']),
                    '数据抓取时间': datetime.now(),
                }
                # print(item)

                self.save_record(item)

            except Exception as e:
                self.logger.error(f"解析处理失败: {e}")

    # ================= 去重 =================
    def need_update(self, item: dict) -> bool:
        """
        判断资金冻结记录是否需要更新
        """
        key = f"temu:funds:{item['店铺']}:{item['冻结类型']}{item['限制时间']}:"

        hash_source = f"{item['冻结类型']}|{item['限制总金额']}|{item['限制时间']}"
        new_hash = hashlib.md5(hash_source.encode("utf-8")).hexdigest()

        old_hash = self.redis_client.get(key)

        # 第一次出现
        if not old_hash:
            self.redis_client.set(key, new_hash)
            return True

        # 数据有变化
        if old_hash != new_hash:
            self.redis_client.set(key, new_hash)
            return True

        # 完全没变化
        return False

    """创建数据表"""
    def create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS temu_funds_restriction (
            id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键',
            `店铺` VARCHAR(100) NOT NULL COMMENT '店铺名称',
            `冻结类型` VARCHAR(50) NOT NULL COMMENT '冻结类型',
            `限制原因` VARCHAR(255) COMMENT '限制原因',
            `限制总金额` DECIMAL(12,2) DEFAULT 0 COMMENT '限制总金额',
            `限制时间` DATETIME COMMENT '冻结开始时间',
            `数据抓取时间` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '抓取时间'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        
        """
        try:

            self.cursor.execute(sql)
            self.db.commit()
            self.logger.info("资金限制表（中文字段）检查 / 创建完成")
        except Exception as e:
            self.logger.error(f"创建表失败: {e}")
            self.db.rollback()

    """保存数据"""
    def save_record(self, item: dict):
        # ✅ 先判断是否需要更新
        if not self.need_update(item):
            self.logger.info(
                f"[跳过] {item['店铺']} | {item['冻结类型']} 无变化"
            )
            return

        sql = """
            INSERT INTO temu_funds_restriction (
                `店铺`,
                `冻结类型`,
                `限制原因`,
                `限制总金额`,
                `限制时间`,
                `数据抓取时间`
            ) VALUES (%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                `限制原因` = VALUES(`限制原因`),
                `限制总金额` = VALUES(`限制总金额`),
                `限制时间` = VALUES(`限制时间`),
                `数据抓取时间` = VALUES(`数据抓取时间`);
            """

        try:
            self.cursor.execute(sql, (
                item['店铺'],
                item['冻结类型'],
                item['限制原因'],
                item['限制总金额'],
                item['限制时间'],
                item['数据抓取时间']
            ))
            self.db.commit()
            self.logger.info(
                f"[保存] {item['店铺']} | {item['冻结类型']} 金额={item['限制总金额']}-----保存成功"
            )
        except Exception as e:
            self.logger.error(
                f"入库失败 frozen_type={item['冻结类型']}: {e}"
            )

    async def run(self):
        self.logger.info(f'正在爬取店铺---------------{self.shop_name}-------------的数据')

        self.create_table()   # ✅【必须调用】

        for attempt in range(3):
            cookies, shop_id = await self.cookie_manager.get_auth()
            json_data = self.get_info(cookies, shop_id)

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
                    ding_bot_send('me', f"[{self.shop_name}] 刷新 cookie 后仍然失效，终止任务")
                    return

            self.parse_data(json_data)
            return

# if __name__ == '__main__':
#     t = Temu_Funds_Restriction("102-Temu全托管")
#     asyncio.run(t.run())



