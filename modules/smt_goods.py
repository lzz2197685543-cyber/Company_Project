import json
import time
import csv
import hashlib
from datetime import datetime
from pathlib import Path
import requests

from utils.cookie_manager import CookieManager
from utils.logger import get_logger


class SMTGoodsSpider:
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
        text = f"{token}&{ts}&{app_key}&{data}"
        return hashlib.md5(text.encode()).hexdigest()

    # ---------- 请求 ----------
    def fetch_page(self, cookies, token, page):
        self.logger.info(f'开始爬取第{page}页数据')

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
            # print(resp.text)

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
    def parse_page(self, data,page):
        if not data or "data" not in data:
            return []

        if "totalPages" in data["data"]:
            self.total_pages = data["data"]["totalPages"]
            print(f'总共{self.total_pages}')

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
        out_dir =Path(__file__).resolve().parent.parent / "data" / "result"
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
        self.logger.info(f'正在爬取店铺------{self.shop_name}------的数据')
        cookies, token = await self.cookie_manager.get_auth()

        page = 1
        retry = False

        while True:
            # 请求响应数据
            data = self.fetch_page(cookies, token, page)
            print(data)

            # ---------- cookie 失效 ----------
            if data == "COOKIE_EXPIRED":
                if retry:
                    raise RuntimeError("cookie 刷新后仍然失效")

                self.logger.info(f"[{self.shop_name}] cookie 失效，自动重新登录中...")
                await self.cookie_manager.refresh()
                cookies, token = await self.cookie_manager.get_auth()

                retry = True  # retry为了防止cookie一直失效进入死循环
                continue  # 👈 用新 cookie 重试当前页，continue 会让程序回到 while True 的开头

            # 解析数据
            items = self.parse_page(data, page)


            self.save_items(items)

            self.logger.info(f"[{self.shop_name}] 第 {page} 页 {len(items)} 条---保存成功")

            if page >= self.total_pages:
                print('数据爬取完毕')
                break

            page += 1
            retry = False
            time.sleep(1)

