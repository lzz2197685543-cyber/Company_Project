import requests
import time
from utils.logger import get_logger
from datetime import datetime
import os
import csv
import json
from pathlib import Path
from utils.cookie_manager import CookieManager
import asyncio

class TK_Sale:
    def __init__(self,shop_name):
        self.shop_name=shop_name
        self.logger= get_logger('tk_sale_data')
        self.cookie_manager = CookieManager(shop_name)
        self.cookies = None

        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'agw-js-conv': 'str',
            'content-type': 'application/json',
            'origin': 'https://seller.tiktokshopglobalselling.com',
            'priority': 'u=1, i',
            'referer': 'https://seller.tiktokshopglobalselling.com/',
            'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        }
        self.url='https://api16-normal-sg.tiktokshopglobalselling.com/api/plan/supplier/SupplierQueryPlanningV2'

    def is_cookie_invalid(self, json_data):
        """
        统一判断 cookie 是否失效
        """
        # 请求异常
        if not json_data:
            return True

        # get_info 主动标记
        if json_data.get("base_resp").get("message")=="未登录":
            return True

        if not isinstance(json_data, dict):
            return True

        if json_data.get("error_msg") == "登录过期，请重新登录":
            return True
        if json_data.get("error_code") in (40001, 401):
            return True


        return False
    """获取指定页面的数据"""
    def get_info(self,page,cookies):
        self.cookies = cookies
        json_data = {
            'query_param': {
                'sku_filter_on_sale_status_list': [
                    '2',
                ],
            },
            'sort_info': {
                'sort_fields': [
                    {
                        'field': 'pay_sub_ord_cnt_7d',
                        'asc': False,
                    },
                ],
            },
            'page_info': {
                'page_no': page,
                'page_size': 50,
            },
            'sort_sku_in_spu_flag': False,
            'view_mode': 3,
        }

        try:
            response = requests.post(
                url=self.url,
                cookies=self.cookies,
                headers=self.headers,
                json=json_data,
                timeout=10
            )
            response.raise_for_status()
            print(response.text[:200])
            return response.json()

        except requests.exceptions.RequestException as e:
            self.logger.error(f"请求失败: {e}")
            return {}

        except Exception as e:
            self.logger.error(f"发生错误: {e}")
            return {}

    """解析返回的数据"""
    def parse_data(self,json_data):
        items=[]
        try:
            if not json_data or 'data' not in json_data:
                self.logger.info(f'返回的数据格式不正确')
                return items

            for i in json_data['data']['data']:
                try:
                    item = {}
                    item['平台'] = 'tk全托管'
                    item['店铺'] = self.shop_name
                    item['商品名称'] = i['sku_code']

                    item['抓取数据日期'] = int(time.time()*1000)

                    item['sku'] = i.get('supply_code', '')
                    item['今日销量'] = i.get('pay_sub_ord_cnt_today', 0)
                    item['近7天销量'] = i.get('pay_sub_ord_cnt_7d', 0)
                    item['近30天销量'] = i.get('pay_sub_ord_cnt_30d', 0)
                    item['平台库存'] = i.get('sale_stock_td', 0)
                    item['在途库存'] = i.get('confirmed_onway_stock_td', 0)
                    # 判断这些字段的总和是否为0，当这几个都是0的话就不保存
                    if (item['今日销量'] + item['近7天销量'] + item['近30天销量'] +
                        item['平台库存'] + item['在途库存']) != 0:
                        # print(f"✅ 有效数据: {item}")
                        items.append(item.copy())  # ⭐ 必须 copy

                except Exception as e:
                    self.logger.error(f"解析单个商品数据时出错: {e}")
                    continue  # 继续处理下一个商品
        except Exception as e:
            self.logger.error(f'解析数据时发生错误: {e}')

        self.logger.info(f"📊 解析完成，共找到 {len(items)} 条有效数据")
        return items

    """批量保存数据到CSV文件"""
    def save_batch(self, items):
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
        filename = f"{out_dir}\\tk_sale_{current_date}.csv"

        # 检查文件是否存在
        file_exists = os.path.exists(filename)

        # 使用追加模式写入
        with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
            f_csv = csv.DictWriter(f, fieldnames=items[0].keys())
            if not file_exists:
                f_csv.writeheader()
            f_csv.writerows(items)

        self.logger.info(f"已保存到文件: {filename}")

    """实现翻页，获取所有页面的数据"""
    async def get_all_page(self):
        self.logger.info(f"开始爬取店铺------------------{self.shop_name}------------------")

        page = 1
        max_page = 100

        while page <= max_page:
            self.logger.info(f"正在爬取---{self.shop_name}---第 {page} 页的数据")

            json_data = None

            for attempt in range(3):
                try:
                    cookies = await self.cookie_manager.get_auth()

                    json_data = self.get_info(page, cookies)

                    # ---------- cookie 失效判断 ----------
                    # ⭐ 核心：统一失效判断
                    if self.is_cookie_invalid(json_data):
                        raise PermissionError("cookie 已失效或接口异常")
                    # 成功直接跳出 retry
                    break

                except PermissionError:
                    self.logger.warning(
                        f"[{self.shop_name}] 第 {page} 页 cookie 失效，刷新登录态（第 {attempt + 1} 次）"
                    )
                    await self.cookie_manager.refresh()
                    await asyncio.sleep(2)

                except Exception as e:
                    self.logger.error(
                        f"[{self.shop_name}] 第 {page} 页第 {attempt + 1} 次请求失败: {e}"
                    )
                    await self.cookie_manager.refresh()
                    await asyncio.sleep(2)

            # ---------- retry 全失败 ----------
            if not json_data:
                self.logger.error(
                    f"[{self.shop_name}] 第 {page} 页重试失败，终止任务"
                )
                break

            # ---------- 没数据，结束 ----------
            if not json_data.get("data", {}).get("data"):
                self.logger.info(f"第 {page} 页已经没有数据了，程序结束")
                break

            # ---------- 解析 + 保存 ----------
            items = self.parse_data(json_data)

            if items:
                self.save_batch(items)

            time.sleep(1)
            page += 1


#
# if __name__ == '__main__':
#     shop_name_list = ["TK全托1401店", "TK全托408-LXZ", "TK全托407-huidan", "TK全托406-yuedongwan",
#                       "TK全托405-huanchuang", "TK全托404-kedi", "TK全托403-juyule", "TK全托401-xiyue",
#                       "TK全托402-quzhi", "TK全托1402店"]
#     for shop_name in shop_name_list:
#         tk = TK_Sale(shop_name)
#
#         asyncio.run(tk.get_all_page())
