import pprint

import requests
import time
import os
import csv
import json
from datetime import datetime
from logger_config import SimpleLogger


class SMT_Stock:
    def __init__(self, shop_name, cookie):
        self.shop_name = shop_name
        self.cookies = cookie
        self.logger = SimpleLogger(name='SMT_SOCKET')

        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }
        self.url = 'https://scm-supplier.aliexpress.com/aidc-aic-console/aic-inventory-manage/getRealTimeInvWithClearanceInfo'

        self.csv_headers = [
            '平台', '店铺', '货号ID', '商品名称', '抓取数据日期',
            '今日销量', '近7天销量', '近30天销量', '平台库存', '在途库存'
        ]

        self.data_dir = "./data/data"
        self._make_data_dir()

    def _make_data_dir(self):
        """创建数据目录"""
        if not os.path.exists(self.data_dir):
            try:
                os.makedirs(self.data_dir)
                self.logger.info(f"创建目录: {self.data_dir}")
            except Exception as e:
                self.logger.error(f"创建目录失败: {e}")
                raise

    def _get_filename(self):
        """生成CSV文件名"""
        current_date = datetime.now().strftime("%Y%m%d")
        return f"./data/data/{self.shop_name}_stock_{current_date}.csv"

    def _make_request(self, page, test_mode=False):
        """发起HTTP请求获取数据"""
        payload = {
            'groupDimension': 0,
            'stockingMode': 'WAREHOUSE',
            'pageIndex': page,
            'pageSize': 50,
            '_scm_token_': 'lz4vmSbNuZUqpDDIF-wUzjicndw',
        }

        try:
            self.headers.update({'Cookie': self.cookies})
            response = requests.post(
                url=self.url,
                headers=self.headers,
                json=payload,
                timeout=30
            )

            self.logger.info(f"第{page}页响应状态码: {response.status_code}")

            if test_mode:
                result = {
                    'status_code': response.status_code,
                    'success': response.status_code == 200
                }
                if response.status_code == 200:
                    try:
                        result['json_data'] = response.json()
                    except:
                        result['json_data'] = None
                return result

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 403:
                self.logger.error("访问被拒绝 (403)，可能是cookie失效")
                return None
            else:
                self.logger.error(f"请求失败，状态码: {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"第{page}页请求失败: {e}")
            return None
        except Exception as e:
            self.logger.error(f"第{page}页发生错误: {e}")
            return None

    def _extract_item(self, item_data):
        """提取单个商品数据"""
        try:
            return {
                '平台': '速卖通',
                '店铺': self.shop_name,
                '货号ID': item_data['scItemInfo']['scItemId'],
                '商品名称': item_data['scItemInfo']['scItemName'],
                '抓取数据日期': int(time.time()*1000),
                '今日销量': item_data['saleInfo'][0]['value'],
                '近7天销量': item_data['saleInfo'][1]['value'],
                '近30天销量': item_data['saleInfo'][3]['value'],
                '平台库存': item_data['warehouseQuantityLabelInfo'][0]['value'],
                '在途库存': int(item_data['onWayQuantityLabelInfo'][0]['value'])
            }
        except (KeyError, IndexError) as e:
            self.logger.error(f"数据字段错误: {e}")
            return None

    def _parse_data(self, json_data):
        """解析API响应数据"""
        if not json_data or 'data' not in json_data:
            self.logger.warning("无有效数据")
            return []

        items = json_data.get('data', [])
        if not items:
            return []

        parsed_items = []
        for item in items:
            parsed_item = self._extract_item(item)
            if parsed_item:
                parsed_items.append(parsed_item)

        return parsed_items

    def _save_data(self, items):
        """保存数据到CSV"""
        if not items:
            return

        filename = self._get_filename()
        file_exists = os.path.exists(filename)

        try:
            with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_headers)

                if not file_exists:
                    writer.writeheader()

                writer.writerows(items)

            self.logger.info(f"已保存{len(items)}条数据到文件: {filename}")
        except Exception as e:
            self.logger.error(f"保存文件失败: {e}")

    def _get_page_data(self, page, max_retries=3):
        """获取单个页面数据，包含重试"""
        self.logger.info(f'正在爬取第{page}页的数据')

        for retry in range(max_retries):
            data = self._make_request(page)

            if data is not None:
                return data

            if retry < max_retries - 1:
                self.logger.info(f"第{retry + 1}次重试...")
                time.sleep(2)

        self.logger.error(f"第{page}页重试{max_retries}次后仍然失败")
        return None

    def run(self):
        """主运行函数"""
        self.logger.info(f"🚀 开始爬取 {self.shop_name} 的库存数据")

        page = 1
        total_items = 0
        page_size = 50

        while True:
            # 获取页面数据
            json_data = self._get_page_data(page)
            if json_data is None:
                break

            # 解析数据
            items = self._parse_data(json_data)
            if not items:
                self.logger.info('没有更多数据了')
                break

            # 保存数据
            self._save_data(items)

            # 更新统计
            current_count = len(items)
            total_items += current_count
            self.logger.info(f"第{page}页处理完成，累计处理: {total_items}条")

            # 检查是否还有下一页
            if current_count < page_size:
                self.logger.info('已到达最后一页')
                break

            page += 1
            time.sleep(1)

        self.logger.info(f"✅ {self.shop_name} 库存数据爬取完成！共获取{total_items}条数据")
        return total_items




if __name__ == '__main__':
    # 店铺列表
    shop_names = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']
    # shop_names = ['SMT002']
    # 加载cookies
    cookies_file = './data/socket_cookies.json'
    """从文件加载cookies"""
    try:
        with open(cookies_file, 'r', encoding='utf-8') as f:
             all_cookies=json.load(f)
    except Exception as e:
        print(f'加载cookies失败---应该没有cookie文件')

    if not all_cookies:
        print("❌ 无法加载cookies，程序退出")
        exit(1)

    # 遍历所有店铺
    for shop_name in shop_names:
        print(f"\n{'=' * 50}")
        print(f"处理店铺: {shop_name}")
        print(f"{'=' * 50}")

        if shop_name not in all_cookies:
            print(f"❌ 店铺 {shop_name} 的cookie不存在，跳过")
            continue

        try:
            crawler = SMT_Stock(shop_name, all_cookies[shop_name])
            count = crawler.run()
            print(f"✅ {shop_name}: 成功爬取 {count} 条数据")
        except Exception as e:
            print(f"❌ {shop_name}: 爬取失败 - {e}")
            continue