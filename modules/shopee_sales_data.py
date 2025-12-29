import requests
import time
import os
import csv
from datetime import datetime
from utils.logger import get_logger
from pathlib import Path
from utils.cookie_manager import CookieManager
import asyncio

class Shopee:
    def __init__(self,shop_name):
        self.shop_name = shop_name
        self.headers = {
            'accept-language': 'zh-CN,zh;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://seller.scs.shopee.cn',
            'priority': 'u=1, i',
            'referer': 'https://seller.scs.shopee.cn/inventory/current-inventory-list',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-business-type': 'SCS',
            'x-request-id': 'edd98f73-3306-45fa-8b09-58438e5e9f5a',
        }
        self.cookie_manager=CookieManager(shop_name)
        self.cookie=None
        self.url = 'https://seller.scs.shopee.cn/api/v4/srm/sales_inventory/list'
        self.logger= get_logger('shopee_sale_data')

    def is_cookie_invalid(self, json_data):
        """
        统一判断 cookie 是否失效
        """

        # 请求异常
        if not json_data:
            return True

        # get_info 主动标记
        if json_data.get("__cookie_invalid__") is True:
            return True

        if not isinstance(json_data, dict):
            return True

        return False

    def get_info(self, page, cookies):
        payload = {
            'page_no': page,
            'count': 100,
            'fields_filter': {},
            'whs_region': 'CN',
            'order_by': 3,
            'is_asc': 0,
        }

        try:
            resp = requests.post(
                url=self.url,
                headers=self.headers,
                cookies=cookies,
                json=payload,
                timeout=10
            )

            text = resp.text
            print(text[:200])

            # ⭐ 关键：直接识别 user not found
            if "user not found" in text.lower():
                self.logger.warning(
                    f"[{self.shop_name}] 第 {page} 页返回 user not found"
                )
                return {
                    "__cookie_invalid__": True,
                    "__raw_text__": text
                }

            return resp.json()

        except Exception as e:
            self.logger.error(
                f"[{self.shop_name}] 第 {page} 页请求异常: {e}"
            )
            return None

    """解析一页的数据"""
    def parse_data(self,json_data):
        items = []
        try:
            if not json_data or 'data' not in json_data:
                self.logger.info('返回的数据格式不正确')
                return items

            for i in json_data['data']['sales_inventory_info']:
                try:
                    item = {}
                    item['平台'] = '虾皮'
                    item['店铺'] = self.shop_name
                    item['商品名称'] = i['product_name']

                    item['抓取数据日期'] = int(time.time()*1000)

                    if i.get('model_info_list'):
                        for i_k in i['model_info_list'][1:]:
                            item['sku'] = i_k.get('seller_sku_id', '')
                            item['今日销量'] = i_k.get('today_sales', 0)
                            item['近7天销量'] = i_k.get('L7D_sales', 0)
                            item['近30天销量'] = i_k.get('L30D_sales', 0)
                            item['平台库存'] = i_k.get('total_on_hand', 0) + i_k.get('mt_in_transit', 0)
                            item['在途库存'] = i_k.get('pending_putaway', 0) + i_k.get('asn_in_transit', 0)

                            if (
                                    item['今日销量']
                                    + item['近7天销量']
                                    + item['近30天销量']
                                    + item['平台库存']
                                    + item['在途库存']
                            ) != 0:
                                items.append(item.copy())  # ⭐ 必须 copy

                            # print(f"⏭️  跳过零数据: {item['商品名称']} - {item['sku']}")
                except Exception as e:
                    self.logger.error(f"解析单个商品数据时出错: {e}")
                    continue  # 继续处理下一个商品
        except Exception as e:
            self.logger.error(f'解析数据时发生错误: {e}')

        self.logger.info(f'解析完成，共找到 {len(items)} 条有效数据')
        return items

    """批量保存数据到CSV文件"""
    def save_batch(self, items):
        """批量保存数据到CSV文件"""
        out_dir = Path(__file__).resolve().parent.parent / "data" / "sale"
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir)
                self.logger.info(f"创建目录: {out_dir}")
            except Exception as e:
                self.logger.error(f"创建目录失败: {e}")
                return

        # 获取当前年月日，格式为 YYYYMMDD
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"{out_dir}\\shopee_sale_{current_date}.csv"

        # 检查文件是否存在
        file_exists = os.path.exists(filename)

        # 使用追加模式写入
        with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
            f_csv = csv.DictWriter(f, fieldnames=items[0].keys())
            if not file_exists:
                f_csv.writeheader()
            f_csv.writerows(items)

        self.logger.info(f"已保存到文件: {filename}")

    """获取所有页面的数据"""

    async def get_all_page(self):
        self.logger.info(
            f"开始爬取店铺------------------{self.shop_name}------------------"
        )

        page = 1
        max_page = 100
        max_retry = 3

        while page < max_page:
            json_data = None

            for attempt in range(1, max_retry + 1):
                try:
                    cookies = await self.cookie_manager.get_auth()
                    json_data = self.get_info(page, cookies)

                    # ⭐ 核心：统一失效判断
                    if self.is_cookie_invalid(json_data):
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
            if not json_data:
                self.logger.error(
                    f"[{self.shop_name}] 第 {page} 页多次失败，终止任务"
                )
                break

            # ---------- 没数据，结束 ----------
            data_list = json_data.get("data", {}).get("sales_inventory_info", [])
            if not data_list:
                self.logger.info(
                    f"[{self.shop_name}] 第 {page} 页无数据，结束爬取"
                )
                break

            # ---------- 解析 & 保存 ----------
            items = self.parse_data(json_data)
            if items:
                self.save_batch(items)

            page += 1
            time.sleep(1)


# async def run_shopee_sale():
#     name_list = ["虾皮全托1501店", "虾皮全托507-lxz","虾皮全托506-kedi", "虾皮全托505-qipei","虾皮全托504-huanchuang","虾皮全托503-juyule","虾皮全托502-xiyue","虾皮全托501-quzhi"]
#     for shop_name in name_list:
#         shein = Shopee(shop_name)
#         await shein.get_all_page()
#
#
# if __name__ == '__main__':
#     asyncio.run(run_shopee_sale())