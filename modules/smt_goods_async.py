import json
import time
import csv
import hashlib
import asyncio
import aiohttp
from datetime import datetime
from pathlib import Path

from utils.cookie_manager import CookieManager
from utils.logger import get_logger


class SMTGoodsSpiderAsync:
    def __init__(self, shop_name: str):
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.logger = get_logger(f"SMTGoods-{shop_name}")

        self.url = (
            "https://seller-acs.aliexpress.com/"
            "h5/mtop.ae.scitem.read.pagequery/1.0/"
        )

        self.headers = {
            "origin": "https://csp.aliexpress.com",
            "referer": "https://csp.aliexpress.com/",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/127.0.0.0 Safari/537.36"
            ),
        }

        self.total_pages = 1

    # ---------- 签名 ----------
    def make_sign(self, token, ts, app_key, data):
        raw = f"{token}&{ts}&{app_key}&{data}"
        return hashlib.md5(raw.encode()).hexdigest()

    # ---------- 请求 ----------
    async def fetch_page(self, session, token, page):
        self.logger.info(f"开始爬取第 {page} 页数据")

        ts = int(time.time() * 1000)
        app_key = "30267743"
        channel_id = self.cookie_manager.channel_id

        data_dict = {
            "pageIndex": page,
            "pageSize": 20,
            "channelId": str(channel_id),
        }
        data_str = json.dumps(data_dict, separators=(",", ":"))

        sign = self.make_sign(token, ts, app_key, data_str)

        params = {
            "jsv": "2.7.2",
            "appKey": app_key,
            "t": str(ts),
            "sign": sign,
            "v": "1.0",
            "api": "mtop.ae.scitem.read.pagequery",
            "type": "originaljson",
            "data": data_str,
        }

        try:
            async with session.post(
                self.url,
                headers=self.headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:

                if resp.status != 200:
                    self.logger.error(f"HTTP 状态异常: {resp.status}")
                    return None

                result = await resp.json()

        except Exception as e:
            self.logger.error(f"请求异常: {e}")
            return None

        # 👇 token / session 失效判定
        ret = result.get("ret", [])
        if ret and isinstance(ret, list):
            code = ret[0]
            if "FAIL_SYS_TOKEN" in code or "SESSION_EXPIRED" in code:
                return "COOKIE_EXPIRED"

        return result

    # ---------- 解析 ----------
    def parse_page(self, data, page):
        if not data or "data" not in data:
            return []

        if "totalPages" in data["data"]:
            self.total_pages = data["data"]["totalPages"]

        items = []
        for row in data["data"].get("data", []):
            sku = ""
            if row.get("items"):
                sku = row["items"][0].get("skuOuterId", "")

            items.append({
                "货号ID": row.get("scitemId"),
                "sku": sku,
            })

        return items

    # ---------- 保存 ----------
    def save_items(self, items):
        out_dir = Path(__file__).resolve().parent.parent / "data" / "result"
        out_dir.mkdir(parents=True, exist_ok=True)

        fname = out_dir / f"{self.shop_name}_goods_{datetime.now():%Y%m%d}.csv"
        exists = fname.exists()

        with open(fname, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=["货号ID", "sku"])
            if not exists:
                writer.writeheader()
            writer.writerows(items)

    # ---------- 主流程 ----------
    async def run(self):
        self.logger.info(f"正在爬取店铺 ------ {self.shop_name} ------ 的数据")

        cookies, token = await self.cookie_manager.get_auth()

        page = 1
        retry = False

        async with aiohttp.ClientSession(cookies=cookies) as session:

            while True:
                data = await self.fetch_page(session, token, page)

                # ---------- cookie 失效 ----------
                if data == "COOKIE_EXPIRED":
                    if retry:
                        raise RuntimeError("cookie 刷新后仍然失效")

                    self.logger.warning(
                        f"[{self.shop_name}] cookie 失效，重新登录中..."
                    )
                    await self.cookie_manager.refresh()
                    cookies, token = await self.cookie_manager.get_auth()

                    # 更新 session cookie
                    session.cookie_jar.clear()
                    session.cookie_jar.update_cookies(cookies)

                    retry = True
                    continue  # 👈 用新 cookie 重试当前页

                if not data:
                    self.logger.error("请求失败，终止任务")
                    return

                items = self.parse_page(data, page)
                self.save_items(items)

                self.logger.info(
                    f"[{self.shop_name}] 第 {page} 页 {len(items)} 条 —— 保存成功"
                )

                if page >= self.total_pages:
                    self.logger.info("🎉 数据爬取完毕")
                    break

                page += 1
                retry = False
                await asyncio.sleep(1)
