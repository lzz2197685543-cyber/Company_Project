import requests
from utils.cookie_manager import CookieManager
from utils.logger import get_logger
from pathlib import Path
from datetime import datetime, timezone, timedelta
import asyncio
import csv
import time

"""shopee对账单"""

class Shopee_Financial_Data_Statement:
    def __init__(self, shop_name, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.cookies = None

        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-region': 'CN',
        }
        self.url ='https://seller.scs.shopee.cn/api/v4/finance/srm/payout_report/list'

        self.logger = get_logger("financial_data_statement")

    def is_cookie_invalid(self, json_data):
        """
        统一判断 cookie 是否失效
        """
        # 请求异常
        if not json_data:
            return True

        # get_info 主动标记
        if json_data.get("msg")=="20302":
            return True

        if not isinstance(json_data, dict):
            return True

        return False

    # ======================
    # 时间转换
    # ======================
    def date_range_to_timestamp_ms(self, tz_offset=8):
        tz = timezone(timedelta(hours=tz_offset))

        start_dt = datetime.strptime(self.start_time, "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, tzinfo=tz
        )
        end_dt = datetime.strptime(self.end_time, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=tz
        )

        return int(start_dt.timestamp()), int(end_dt.timestamp())

    def get_info(self,page,cookies):
        self.cookies = cookies
        begin_ts, end_ts = self.date_range_to_timestamp_ms()
        json_data = {
            'sort_fields': [],
            'page_no': page,
            'page_size': 100,
            'actual_paid_start_time': begin_ts,
            'actual_paid_end_time': end_ts,
        }
        try:
            response = requests.post(
                url=self.url,
                cookies=self.cookies,
                headers=self.headers,
                json=json_data,
            )
            print(response.text)
            return response.json()
        except Exception as e:
            self.logger.error(f'请求响应失败:{e}')

    def parse_data(self,json_data):
        try:
            result_list = json_data['data']['list']
            if not result_list:
                return []
        except (KeyError, TypeError):
            self.logger.error("接口数据结构异常")
            return []

        items = []

        for i in result_list:
            items.append( {
                "Creation Date": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(i.get('creation_date'))) if i.get(
                    'creation_date') else '',
                "Seller Name": i.get('seller_name', ''),
                "Seller ID": i.get('seller_id', ''),
                "Payout ID": i.get('payout_id', ''),
                "Payout Cycle": f"{time.strftime('%Y-%m-%d', time.localtime(i.get('payment_cycle', {}).get('cycle_start_date')))} to {time.strftime('%Y-%m-%d', time.localtime(i.get('payment_cycle', {}).get('cycle_end_date')))}" if i.get(
                    'payment_cycle') else '',
                "Total Settlement Quantity": i.get('total_settlement_qty', 0),
                "Total Payout Amount": i.get('total_payment_amount', '0.00'),
                "Linked Payout Request ID": i.get('payment_request_id_list', [''])[0] if i.get(
                    'payment_request_id_list') else '',
                "Actual Paid Date": time.strftime('%Y-%m-%d %H:%M:%S',
                                                  time.localtime(i.get('actual_paid_timestamp'))) if i.get(
                    'actual_paid_timestamp') else ''
            })
        return items

    def save_batch(self, items):
        if not items:
            return

        out_dir = Path(__file__).resolve().parent.parent / "data" / "financial" / "shopee"
        out_dir.mkdir(parents=True, exist_ok=True)

        fname = out_dir / f"{self.shop_name}_{datetime.now().strftime('%m')}_对账单.csv"
        exists = fname.exists()

        with open(fname, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=items[0].keys())
            if not exists:
                writer.writeheader()
            writer.writerows(items)

    async def get_all_page(self):
        self.logger.info(f"开始爬取店铺-------------{self.shop_name}------------")
        page=1
        while True:
            json_data=None
            for attempt in range(3):
                try:
                    cookies=await self.cookie_manager.get_auth()
                    json_data=self.get_info(page,cookies)

                    # ⭐ 核心：统一失效判断
                    if self.is_cookie_invalid(json_data):
                        raise PermissionError("cookie 已失效或接口异常")
                    # 成功直接跳出 retry
                    break

                except PermissionError:
                    self.logger.warning(f"[{self.shop_name}] 登录失效，刷新 cookie（第 {attempt+1} 次）")
                    await self.cookie_manager.refresh()
                    await asyncio.sleep(2)

                except Exception as e:
                    self.logger.error(f"[{self.shop_name}] 第 {attempt+1} 次请求失败: {e}")
                    await asyncio.sleep(2)

            if not json_data:
                self.logger.error(f"[{self.shop_name}] 重试失败，终止任务")
                return

            items = self.parse_data(json_data)

            # 数据不为空才进行保存
            if items:
                # 保存数据
                self.save_batch(items)

            self.logger.info(f"店铺-{self.shop_name}-第 {page} 页保存成功")

            # ---------- 没数据，结束 ----------
            if not json_data['data']['list']:
                self.logger.info(f'第{page}页已经没有数据了,程序结束')
                break

            if len(items) < 100:
                self.logger.info("已到最后一页")
                break

            page += 1

async def run_shopee_financial_statement():
    start_time = '2025-12-01'
    end_time = '2025-12-28'
    name_list = ["虾皮全托1501店", "虾皮全托507-lxz", "虾皮全托506-kedi", "虾皮全托505-qipei", "虾皮全托504-huanchuang", "虾皮全托503-juyule",
                 "虾皮全托502-xiyue", "虾皮全托501-quzhi"]
    for shop_name in name_list:
        shein = Shopee_Financial_Data_Statement(shop_name,start_time,end_time)
        await shein.get_all_page()


if __name__ == '__main__':
    asyncio.run(run_shopee_financial_statement())