import json
import time
import csv
import hashlib
from datetime import datetime
from pathlib import Path
import requests
import asyncio

from utils.cookie_manager import CookieManager
from utils.logger import get_logger


class SMTGoodsSpider:
    def __init__(self, shop_name: str):
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.logger = get_logger("smt_goods")

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

    def is_cookie_invalid(self, json_data):
        """
        统一判断 cookie 是否失效
        """

        # 请求异常
        if not json_data:
            return True

        # get_info 主动标记
        if 'FAIL_SYS_TOKEN_EXOIRED' in json_data['ret'][0]:
            return True

        if not isinstance(json_data, dict):
            return True

        return False

    # ---------- 签名 ----------
    def make_sign(self, token, ts, app_key, data):
        text = f"{token}&{ts}&{app_key}&{data}"
        return hashlib.md5(text.encode()).hexdigest()

    # ---------- 请求 ----------
    def fetch_page(self, cookies, token, page):
        self.logger.info(f'开始爬取第{page}/{self.total_pages}页数据')

        ts = int(time.time() * 1000)
        app_key = "30267743"
        channelId = self.cookie_manager.channel_id

        data_dict = {
            "pageIndex": page,
            "pageSize": 20,
            "channelId": f"{channelId}"
        }
        data_str = json.dumps(data_dict, separators=(",", ":"))

        sign = self.make_sign(token, ts, app_key, data_str)
        params = {
            'jsv': '2.7.2',
            'appKey': app_key,
            't': str(ts),
            'sign': sign,
            'v': '1.0',
            'timeout': '30000',
            'H5Request': 'true',
            'url': 'mtop.ae.scitem.read.pagequery',
            'params': '[object Object]',
            '__channel-id__': f'{channelId}',
            'api': 'mtop.ae.scitem.read.pagequery',
            'type': 'originaljson',
            'dataType': 'json',
            'valueType': 'original',
            'x-i18n-regionID': 'AE',
            'data': data_str,
        }

        try:

            resp = requests.post(
                self.url,
                cookies=cookies,
                headers=self.headers,
                params=params,
                timeout=15,
            )
            print(resp.text[:200])

            if resp.status_code != 200:
                return None

            result = resp.json()
        except Exception as e:
            self.logger.error(f'请求响应失败:{e}')

        # 👇 关键：token / session 失效判定
        ret = result.get("ret", [])
        if ret and isinstance(ret, list):
            code = ret[0]
            if "FAIL_SYS_TOKEN" in code or "SESSION_EXPIRED" in code:
                return "COOKIE_EXPIRED"

        return result

    # ---------- 解析 ----------
    def parse_page(self, data):
        if not data or "data" not in data:
            return []

        if "totalPages" in data["data"]:
            self.total_pages = data["data"]["totalPages"]

        items = []
        for i in data["data"].get("data", []):
            try:
                sku = ""
                if i.get("items"):
                    sku = i["items"][0].get("skuOuterId", "")

                items.append({
                    "货号ID": i.get("scitemId"),
                    "sku": sku,
                })
            except Exception as e:
                self.logger.error(f'解析数据失败:{e}')
        return items

    # ---------- 保存 ----------
    def save_items(self, items):
        out_dir = Path(__file__).resolve().parent.parent / "data" / "sale"
        out_dir.mkdir(parents=True, exist_ok=True)

        fname = out_dir / f"{self.shop_name}_goods_{datetime.now():%Y%m%d}.csv"
        exists = fname.exists()

        with open(fname, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=items[0].keys())
            if not exists:
                writer.writeheader()
            writer.writerows(items)

    # ---------- 主流程 ----------
    async def run(self):
        self.logger.info(f'正在爬取店铺------{self.shop_name}------的数据')
        page = 1
        max_retry = 3

        while True:
            data = None
            for attempt in range(1,max_retry+1):
                try:
                    cookies,token = await self.cookie_manager.get_auth()
                    # 请求响应数据
                    data = self.fetch_page(cookies, token, page)

                    # ⭐ 核心：统一失效判断
                    if self.is_cookie_invalid(data):
                        raise PermissionError("cookie 已失效或接口异常")
                    # 成功直接跳出 retry
                    break
                except PermissionError as e:
                    self.logger.warning(
                        f"[{self.shop_name}] 第 {page} 页 cookie 失效，刷新中（{attempt}/{max_retry}）"
                    )
                    await self.cookie_manager.refresh()
                    await asyncio.sleep(2)

                except Exception as e:
                    self.logger.error(
                        f"[{self.shop_name}] 第 {page} 页请求异常（{attempt}/{max_retry}）：{e}"
                    )
                    await self.cookie_manager.refresh()
                    await asyncio.sleep(2)

            # ---------- retry 全失败 ----------
            if not data:
                self.logger.error(
                    f"[{self.shop_name}] 第 {page} 页多次失败，终止任务"
                )
                break

            # 解析数据
            items = self.parse_page(data)

            if items:
                self.save_items(items)

            self.logger.info(f"[{self.shop_name}] 第 {page} 页 {len(items)} 条---保存成功")

            if page >= self.total_pages:
                print('数据爬取完毕')
                break

            page += 1

            time.sleep(1)


# async def main():
#     # shop_name_list = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']
#     shop_name_list=['SMT208']
#     for shop_name in shop_name_list:
#         spider_socket = SMTGoodsSpider(shop_name)
#         await spider_socket.run()
#
#
# if __name__ == '__main__':
#     asyncio.run(main())
