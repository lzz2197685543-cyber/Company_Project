import json
import asyncio
import requests
from utils.cookie_manager import CookieManager
import time
from pathlib import Path
from datetime import datetime
import csv
from utils.logger import get_logger

class SMTStockSpider:
    def __init__(self, shop_name: str,):
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.logger = get_logger(f"SMTGoods-{shop_name}")
        self.url = (
            "https://scm-supplier.aliexpress.com/"
            "aidc-aic-console/aic-inventory-manage/getRealTimeInvWithClearanceInfo"
        )
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }

    def is_cookie_invalid(self, json_data):
        """
        统一判断 cookie 是否失效
        """

        # 请求异常
        if not json_data:
            return True


        # get_info 主动标记
        if json_data == "COOKIE_EXPIRED":
            return True

        if 401 in json_data:
            self.logger.info('cookie无效')
            return True

        if not isinstance(json_data, dict):
            return True

        return False

    # ---------- 请求 ----------
    def fetch_page(self, cookies,page_index: int):
        self.logger.info(f'正在爬取第{page_index}页')
        """发起HTTP请求获取数据"""
        payload = {
            'groupDimension': 0,
            'stockingMode': 'WAREHOUSE',
            'pageIndex': page_index,
            'pageSize': 50,
            '_scm_token_': 'lz4vmSbNuZUqpDDIF-wUzjicndw',
        }

        try:
            response = requests.post(
                url=self.url,
                headers=self.headers,
                cookies=cookies,
                json=payload,
                timeout=30
            )
            print(response.text[:200])

            data=response.json()

            return data

        except Exception as e:
            self.logger.error(f'请求响应数据失败:{e}')

    # ---------- 解析 ----------
    def parse_page(self,json_data):
        items = []

        if not json_data or 'data' not in json_data:
            self.logger.info('返回的数据格式不正确')
            return items

        for i in json_data.get('data', []):
            try:
                item={
                    '平台': '速卖通',
                    '店铺': self.shop_name,
                    '货号ID': i['scItemInfo']['scItemId'],
                    '商品名称': i['scItemInfo']['scItemName'],
                    '抓取数据日期': int(time.time() * 1000),
                    '今日销量': i['saleInfo'][0]['value'],
                    '近7天销量': i['saleInfo'][1]['value'],
                    '近30天销量': i['saleInfo'][3]['value'],
                    '平台库存': i['warehouseQuantityLabelInfo'][0]['value'],
                    '在途库存': int(i['onWayQuantityLabelInfo'][0]['value'])
                }
                items.append(item)
            except Exception as e:
                self.logger.error('解析数据有问题:{e}')

        return items

    def save_items(self, items):
        out_dir = Path(__file__).resolve().parent.parent / "data" / "sale"
        self.logger.info(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        fname = out_dir / f"{self.shop_name}_stock_{datetime.now():%Y%m%d}.csv"
        exists = fname.exists()

        with open(fname, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames= items[0].keys())
            if not exists:
                writer.writeheader()
            writer.writerows(items)

    async def run(self):
        self.logger.info(f'正在爬取店铺-------{self.shop_name}------的数据')

        page_index = 1

        max_retry=3

        while True:
            json_data=None
            for attempt in range(1, max_retry + 1):
                try:
                    cookies, token = await self.cookie_manager.get_auth()
                    # 请求响应数据
                    json_data = self.fetch_page(cookies,page_index)

                    # ⭐ 核心：统一失效判断
                    if self.is_cookie_invalid(json_data):
                        raise PermissionError("cookie 已失效或接口异常")
                    # 成功直接跳出 retry
                    break
                except PermissionError as e:
                    self.logger.warning(
                        f"[{self.shop_name}] 第 {page_index} 页 cookie 失效，刷新中（{attempt}/{max_retry}）"
                    )
                    await self.cookie_manager.refresh()
                    await asyncio.sleep(2)

                except Exception as e:
                    self.logger.error(
                        f"[{self.shop_name}] 第 {page_index} 页请求异常（{attempt}/{max_retry}）：{e}"
                    )
                    await asyncio.sleep(2)


            # 解析数据
            items=self.parse_page(json_data)
            self.logger.info(f'解析得到{len(items)}条数据')

            # 保存数据
            if items:
                self.save_items(items)
                self.logger.info(f'第{page_index}页，数据保存成功')

            if len(items)<50:
                self.logger.info('已经达到最后一页了')
                break

            page_index += 1

            await asyncio.sleep(0.8)

async def main():
    shop_name_list = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']
    for shop_name in shop_name_list:
        spider_socket = SMTStockSpider(shop_name)
        await spider_socket.run()


# if __name__ == '__main__':
#     asyncio.run(main())
