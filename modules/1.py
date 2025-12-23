import asyncio
import aiohttp
import aiomysql
import redis
import hashlib
from datetime import datetime

from utils.cookie_manager import CookieManager
from utils.logger import get_logger


class Temu_Funds_Restriction:
    def __init__(self, shop_name):
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.logger = get_logger(f"Temu-{shop_name}")

        # Redis（同步，做状态去重，影响极小）
        self.redis_client = redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True
        )

        # MySQL 连接池
        self.pool = None

        self.cookies = None
        self.headers = {
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        }

        self.rule_url = "https://agentseller.temu.com/api/merchant/fund-frozen/rules"
        self.detail_url = "https://agentseller.temu.com/api/merchant/fund-frozen/details"

    # =========================
    # MySQL 初始化
    # =========================
    async def init_mysql(self):
        if self.pool:
            return

        self.pool = await aiomysql.create_pool(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="1234",
            db="py_spider",
            minsize=1,
            maxsize=10,
            autocommit=True,
            charset="utf8mb4"
        )

        self.logger.info("✅ MySQL 连接池初始化完成")

    # =========================
    # 创建表
    # =========================
    async def create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS temu_funds_restriction (
            id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键',
            `店铺` VARCHAR(100) NOT NULL COMMENT '店铺名称',
            `冻结类型` VARCHAR(50) NOT NULL COMMENT '冻结类型',
            `限制原因` VARCHAR(255) COMMENT '限制原因',
            `限制总金额` DECIMAL(12,2) DEFAULT 0 COMMENT '限制总金额',
            `限制时间` DATETIME COMMENT '冻结开始时间',
            `数据抓取时间` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '抓取时间',
            UNIQUE KEY uk_shop_frozen (`店铺`, `冻结类型`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

        self.logger.info("资金限制表检查 / 创建完成")

    # =========================
    # aiohttp POST
    # =========================
    async def async_post(self, url, cookies=None, headers=None, json_data=None):
        timeout = aiohttp.ClientTimeout(total=15)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                url,
                cookies=cookies,
                headers=headers,
                json=json_data
            ) as resp:
                text = await resp.text()
                try:
                    return await resp.json()
                except Exception:
                    self.logger.error(f"❌ 非 JSON 响应: {text[:200]}")
                    return None

    # =========================
    # 获取冻结规则
    # =========================
    async def get_info(self, cookies, shop_id):
        self.cookies = cookies
        self.headers["mallid"] = str(shop_id)

        return await self.async_post(
            url=self.rule_url,
            cookies=self.cookies,
            headers=self.headers,
            json_data={}
        )

    # =========================
    # 获取冻结详情时间
    # =========================
    async def get_detail_info(self, frozen_type):
        json_data = {
            "pageNum": 1,
            "pageSize": 10,
            "frozenType": frozen_type
        }

        try:
            data = await self.async_post(
                url=self.detail_url,
                cookies=self.cookies,
                headers=self.headers,
                json_data=json_data
            )

            freeze_ts = data["result"]["detailsRows"][0]["freezeStartTime"]
            return datetime.fromtimestamp(freeze_ts / 1000)

        except Exception:
            self.logger.error("获取限制时间失败")
            return None

    # =========================
    # Redis 去重
    # =========================
    def need_update(self, item: dict) -> bool:
        key = f"temu:funds:{item['店铺']}:{item['冻结类型']}"

        hash_source = (
            f"{item['冻结类型']}|"
            f"{item['限制总金额']}|"
            f"{item['限制时间']}"
        )

        new_hash = hashlib.md5(hash_source.encode("utf-8")).hexdigest()
        old_hash = self.redis_client.get(key)

        if not old_hash or old_hash != new_hash:
            self.redis_client.set(key, new_hash)
            return True

        return False

    # =========================
    # 保存数据（异步）
    # =========================
    async def save_record(self, item: dict):
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

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (
                    item["店铺"],
                    item["冻结类型"],
                    item["限制原因"],
                    item["限制总金额"],
                    item["限制时间"],
                    item["数据抓取时间"]
                ))

        self.logger.info(
            f"[保存] {item['店铺']} | {item['冻结类型']} 金额={item['限制总金额']}"
        )

    # =========================
    # 单条规则处理
    # =========================
    async def handle_rule(self, rule):
        item = {
            "店铺": self.shop_name,
            "冻结类型": rule["frozenType"],
            "限制原因": rule.get("reason"),
            "限制总金额": str(rule.get("amount", 0)).replace("￥", "").replace(",", ""),
            "限制时间": await self.get_detail_info(rule["frozenType"]),
            "数据抓取时间": datetime.now()
        }

        await self.save_record(item)

    # =========================
    # 解析规则
    # =========================
    async def parse_data(self, json_data):
        rules = json_data.get("result", {}).get("rules", [])

        if not rules:
            self.logger.info("暂无资金限制数据")
            return

        tasks = [self.handle_rule(rule) for rule in rules]
        await asyncio.gather(*tasks)

    # =========================
    # 主入口
    # =========================
    async def run(self):
        self.logger.info(f"开始抓取店铺 {self.shop_name}")

        await self.init_mysql()
        await self.create_table()

        cookies, shop_id = await self.cookie_manager.get_auth()
        json_data = await self.get_info(cookies, shop_id)

        if not json_data:
            return

        if json_data.get("error_msg") == "Invalid Login State":
            await self.cookie_manager.refresh()
            return

        await self.parse_data(json_data)


# =========================
# 并发多店铺
# =========================
async def main():
    shops = [
        "103-Temu全托管",
        "1105-Temu全托管",
        "2106-Temu全托管",
    ]

    tasks = [Temu_Funds_Restriction(shop).run() for shop in shops]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
