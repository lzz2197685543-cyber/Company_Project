from utils.logger import get_logger
import requests
import time
import os
import csv
from datetime import datetime
import json
from pathlib import Path
from utils.cookie_manager import CookieManager
from utils.dingtalk_bot import ding_bot_send
import asyncio

class Shein_Sale:
    def __init__(self,shop_name):
        self.shop_name=shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.cookies=None
        self.headers = {
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://sso.geiwohuo.com',
            'priority': 'u=1, i',
            'referer': 'https://sso.geiwohuo.com/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        self.url = 'https://sso.geiwohuo.com/idms/goods-skc/list'

        self.logger= get_logger('shein_sale_data')

    def is_cookie_invalid(self, json_data):
        """
        统一判断 cookie 是否失效
        """
        # 请求异常
        if not json_data:
            return True

        # get_info 主动标记
        if json_data.get("msg")=="子系统登录重定向":
            return True

        if not isinstance(json_data, dict):
            return True

        return False

    """获取指定页面的数据"""
    def get_info(self, page,cookies):

        json_data = {
            'pageNumber': page,
            'pageSize': 100,
            'sortBy7dSaleCnt': 2,
        }
        try:
            # 发送请求，设置超时防止卡死
            response = requests.post(
                url=self.url,
                cookies=cookies,
                headers=self.headers,
                json=json_data,
                timeout=15
            )
            print(response.text[:200])

            # 检查响应状态
            response.raise_for_status()

            # 尝试解析JSON
            data = response.json()
            return data
        except Exception as e:
            self.logger.error(
                f"[{self.shop_name}] 第 {page} 页请求异常: {e}"
            )
            return None

    """解析返回的数据"""
    def parse_data(self, json_data):
        items = []
        try:
            if not json_data or 'info' not in json_data:
                self.logger.info(f'返回的数据格式不正确')
                return items

            for i in json_data['info']['list']:
                try:
                    # 每次循环创建一个新的字典
                    item = {}
                    item['平台'] = 'SHEIN'
                    item['店铺'] = self.shop_name
                    item['商品名称']=i['categoryName']

                    item['抓取数据日期'] = int(time.time())

                    # 不要合计的
                    if i.get('skuList'):
                        for i_k in i['skuList'][:-1]:
                            item['sku'] = i_k.get('supplierSku', '')
                            item['今日销量'] = i_k.get('totalSaleVolume', 0)
                            item['近7天销量'] = i_k.get('c7dSaleCnt', 0)
                            item['近30天销量'] = i_k.get('c30dSaleCnt', 0)
                            item['平台库存'] = i_k.get('stock', 0)
                            item['在途库存'] = i_k.get('transit', 0)

                            # 判断这些字段的总和是否为0，当这几个都是0的话就不保存
                            if (item['今日销量'] + item['近7天销量'] + item['近30天销量'] +
                                item['平台库存'] + item['在途库存']) != 0:
                                items.append(item.copy())
                            else:
                                pass
                                # print(f"⏭️  跳过零数据: {item['商品名称']} - {item['sku']}")

                except Exception as e:
                    self.logger.error(f"解析单个商品数据时出错: {e}")
                    continue  # 继续处理下一个商品

        except Exception as e:
            self.logger.error(f'解析数据时发生错误: {e}')
        self.logger.info(f"📊 解析完成，共找到 {len(items)} 条有效数据")
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
        filename = f"{out_dir}/shein_sale_{current_date}.csv"

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
        page = 1
        max_page = 100
        max_retry=3
        while page < max_page:
            json_data=None
            try:
                self.logger.info(f'正在爬取---{self.shop_name}---第{page}页的数据')

                for attempt in range(3):
                    try:
                        # 获取当前页的数据
                        cookies=await self.cookie_manager.get_auth()
                        json_data = self.get_info(page,cookies)

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
                if not json_data['info']['list']:
                    self.logger.info(f'第{page}页已经没有数据了,程序结束')
                    break

                # 解析数据
                items = self.parse_data(json_data)

                # 数据不为空才进行保存
                if items:
                    # 保存数据
                    self.save_batch(items)
                # 等待1秒继续下一页
                time.sleep(1)

                page += 1
            except KeyboardInterrupt:
                self.logger.info("用户中断爬取")
                break


# async def run_shein_sale():
#     name_list = ["希音全托301-yijia", "希音全托302-juyule", "希音全托303-kedi", "希音全托304-xiyue"]
#     for shop_name in name_list:
#         shein = Shein_Sale(shop_name)
#         await shein.get_all_page()
#
#
# if __name__ == '__main__':
#     asyncio.run(run_shein_sale())
