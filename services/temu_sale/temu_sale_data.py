import requests
import json
import time
import os
import csv
import asyncio
from datetime import datetime
from utils.logger import get_logger
from utils.cookie_manager import CookieManager
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "data"


class Temu:
    def __init__(self, job):
        self.headers = {
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://ww.erp321.com',
            'priority': 'u=1, i',
            'referer': 'https://ww.erp321.com/app/wms/crossborder/deliveryware/Temu/SalesStockManager.aspx',
            'sec-ch-ua': '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0',
            'x-requested-with': 'XMLHttpRequest',
        }
        self.cookies = None
        self.params = {
            "ts___": "1775804189442",
            "am___": "LoadDataToJSON"
        }
        self.logger = get_logger(job)
        self.cookie_manager = CookieManager(job)

    """获取指定页面的数据"""
    def get_info(self, page):
        try:

            data = {
                "__VIEWSTATE": "/wEPDwUKLTc0NTY3NDc4MGRkRbe3LGXIPYkmIo52p0FzUYtuKlg=",
                '__VIEWSTATEGENERATOR': '96FE7ACB',
                'pagetype': 'Temu',
                'sales_qty': 'today',
                '_jt_page_count_enabled': 'true',
                '_jt_page_size': '500',
                '__CALLBACKID': 'JTable1',
                '__CALLBACKPARAM': f'{{"Method":"LoadDataToJSON","Args":["{str(page)}","[]","{{}}"]}}',

            }

            url = 'https://ww.erp321.com/app/wms/crossborder/deliveryware/Temu/SalesStockManager.aspx'
            response = requests.post(url=url, params=self.params, headers=self.headers, cookies=self.cookies, data=data)
            print("响应数据：", response.text[:200])
            return response.text
        except Exception as e:
            self.logger.error('请求解析错误:', e)

    def parse_data(self, res_text):
        # 解析数据
        try:
            # 首先去除开头的 "0|"
            if res_text.startswith('0|'):
                res_text = res_text[2:]

            # 尝试直接解析整个JSON
            try:
                response_data = json.loads(res_text)
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON解析失败: {e}")
                print(f'JSON解析失败: {e}')

                # 尝试修复常见的JSON格式问题
                # 1. 处理可能的转义字符问题
                res_text = res_text.replace('\\', '\\\\')
                # 2. 尝试再次解析
                try:
                    response_data = json.loads(res_text)
                except:
                    self.logger.error("修复后仍然解析失败")
                    return []

            # 获取ReturnValue
            return_value_str = response_data.get('ReturnValue', '{}')

            # 尝试解析ReturnValue
            try:
                return_data = json.loads(return_value_str)
            except json.JSONDecodeError as e:
                self.logger.error(f"ReturnValue JSON解析失败: {e}")

                # 尝试不同的修复策略
                # 策略1: 使用 ast.literal_eval 作为备选方案
                import ast
                try:
                    return_data = ast.literal_eval(return_value_str)
                except:
                    # 策略2: 使用简单的字符串替换
                    # 修复未转义的双引号
                    return_value_str = return_value_str.replace('"', '\"')
                    # 处理Unicode转义
                    return_value_str = return_value_str.encode('unicode_escape').decode('utf-8')

                    try:
                        return_data = json.loads(return_value_str)
                    except:
                        self.logger.error("所有修复尝试都失败")
                        return []

            data_list = return_data.get('datas', [])

        except Exception as e:
            self.logger.error(f"解析数据时发生错误: {e}")
            return []

        # 定义需要排除的店铺列表
        excluded_shops = [
            "107-Temu全托管",
            "112-Temu全托管",
            "1106-Temu全托管",
            "2102-Temu全托管",
            "2104-Temu全托管",
            "2105-Temu全托管",
            "2107-Temu全托管",
            "2108-Temu全托管"
        ]

        items = []
        last_product_name = ""  # 初始化上一个商品名称

        for i in data_list:
            shop_name = i.get('shop_name', '')

            # 如果店铺在排除列表中，跳过本次循环
            if shop_name in excluded_shops:
                continue

            item = {}
            item['平台'] = 'TEMU'
            item['店铺'] = shop_name
            item['sku'] = i.get('sku_ext_code', '')

            # 处理商品名称：如果当前商品名称为空，使用上一个非空名称
            current_name = i.get('product_name', '')
            if current_name and current_name.strip():  # 如果当前名称不为空
                item['商品名称'] = current_name
                last_product_name = current_name  # 更新上一个商品名称
            elif last_product_name:  # 如果当前名称为空，但之前有保存过名称
                item['商品名称'] = last_product_name
            else:  # 如果这是第一个且名称为空
                item['商品名称'] = ''

            item['抓取数据日期'] = int(time.time() * 1000)
            item['今日销量'] = i.get('today_sale_volume', 0)
            item['近7天销量'] = i.get('last_seven_days_sale_volume', 0.0)
            item['近30天销量'] = i.get('last_thirty_days_sale_volume', 0.0)
            item['平台库存'] = i.get('warehouse_inventory_num', 0)
            item['在途库存'] = i.get('wait_receive_num', 0)

            items.append(item)
        return items

    """批量保存数据到CSV文件"""
    def save_batch(self, items, header):
        """批量保存数据到CSV文件"""
        data_dir = f"{CONFIG_PATH}/sale"
        if not os.path.exists(data_dir):
            try:
                os.makedirs(data_dir)
                self.logger.info(f"创建目录: {data_dir}")
            except Exception as e:
                self.logger.error(f"创建目录失败: {e}")
                return

        # 获取当前年月日，格式为 YYYYMMDD
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"{data_dir}/temu_sale_{current_date}.csv"

        # 检查文件是否存在
        file_exists = os.path.exists(filename)

        # 使用追加模式写入
        with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
            f_csv = csv.DictWriter(f, fieldnames=header)
            if not file_exists:
                f_csv.writeheader()
            f_csv.writerows(items)

        print(f"💾 已保存到文件: {filename}")
        self.logger.info(f"已保存到文件: {filename}")

    """实现翻页，获取所有页面的数据"""
    async def get_all_page(self):
        page = 1
        max_page = 100
        total_items = 0

        while page <= max_page:
            self.logger.info(f'正在爬取第{page}页的数据')

            retry = 0
            while retry < 3:  # 最多尝试 2 次（第一次失败后 refresh）
                try:
                    self.cookies = self.cookie_manager.load_cookies()

                    res_text = self.get_info(page)

                    # 1️⃣ res_text 为空，直接触发 refresh
                    if not res_text or not res_text.strip():
                        raise ValueError('res_text 为空')

                    items = self.parse_data(res_text)

                    # 2️⃣ 解析后无数据，也视为异常
                    if not items:
                        raise ValueError('解析后数据为空，可能登录失效')

                    # ===== 正常流程 =====
                    self.logger.info(f"📊 第{page}页获取到{len(items)}条数据")
                    total_items += len(items)

                    header = [
                        '平台', '店铺', '商品名称', 'sku', '抓取数据日期',
                        '今日销量', '近7天销量', '近30天销量', '平台库存', '在途库存'
                    ]
                    self.save_batch(items, header)

                    # 最后一页判断
                    if len(items) < 500:
                        self.logger.info(f"✅ 第{page}页为最后一页，共获取{total_items}条")
                        return

                    break  # 成功，跳出 retry 循环

                except Exception as e:
                    retry += 1
                    self.logger.warning(f"第{page}页第{retry}次失败：{e}")

                    if retry == 1:
                        self.logger.info("🔄 尝试 refresh 登录态...")
                        await self.cookie_manager.refresh()  # 👈 关键
                        time.sleep(5)
                    else:
                        self.logger.info(f"❌ 第{page}页重试失败，终止程序")
                        return

            page += 1
            time.sleep(2)


async def temu_run():
    temu = Temu('temu_sale_data')
    await temu.get_all_page()


if __name__ == '__main__':
    asyncio.run(temu_run())
