import requests
import time
import os
from datetime import datetime
import csv
from utils.logger import get_logger
import json

from pathlib import Path
CONFIG_DIR = Path(__file__).resolve().parent.parent / "data"
# print(CONFIG_DIR)

class NewYmxNewData:
    def __init__(self, country_config=None):
        self.logger = get_logger('YmxNews')

        # 国家配置字典
        self.country_configs = {
            "美国": {
                "market": "US",
                "minAmzUnit": "300",
                "nodeIdPaths": ["165793011"],
                "site_name": "美国"
            },
            "英国": {
                "market": "UK",
                "minAmzUnit": "100",
                "nodeIdPaths": ["468292"],
                "site_name": "英国"
            },
            "德国": {
                "market": "DE",
                "minAmzUnit": "100",
                "nodeIdPaths": ["12950651"],
                "site_name": "德国"
            },
            "法国": {
                "market": "FR",
                "minAmzUnit": "100",
                "nodeIdPaths": ["322086011"],
                "site_name": "法国"
            },
            "西班牙": {
                "market": "ES",
                "minAmzUnit": "100",
                "nodeIdPaths": ["599385031"],
                "site_name": "西班牙"
            }
        }

        # 如果传入特定国家配置，则使用该配置
        if country_config:
            self.country_config = country_config
        else:
            # 默认使用美国配置
            self.country_config = self.country_configs["美国"]

        self.current_country = None

        self.cookies = None

        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'content-type': 'application/json;charset=UTF-8',
            'origin': 'https://www.sellersprite.com',
            'priority': 'u=1, i',
            'referer': 'https://www.sellersprite.com/v3/product-research?market=US&page=4&size=100&symbolFlag=true&monthName=bsr_sales_nearly&selectType=2&filterSub=false&weightUnit=g&order%5Bfield%5D=amz_unit&order%5Bdesc%5D=true&productTags=%5B%5D&nodeIdPaths=%5B%22165793011%22%5D&sellerTypes=%5B%5D&eligibility=%5B%5D&pkgDimensionTypeList=%5B%5D&sellerNationList=%5B%5D&minAmzUnit=300&putawayMonth=3&lowPrice=N&video=',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        }
        self.url = 'https://www.sellersprite.com/v3/api/product-research'

        # 创建数据目录
        self.data_dir = f"{CONFIG_DIR}"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            self.logger.info(f"创建目录: {self.data_dir}")

    def get_cookies(self):
        try:
            with open(f'{CONFIG_DIR}/ymx_cookies_dict.json', 'r', encoding='utf-8') as f:
                return json.loads(f.read())
        except Exception as e:
            print(f'获取cookie出错:{e}')

    def set_country(self, country_name):
        """设置要爬取的国家"""
        if country_name in self.country_configs:
            self.country_config = self.country_configs[country_name]
            self.current_country = country_name
            self.logger.info(f"已设置为爬取{country_name}站点")
        else:
            raise ValueError(f"不支持的国家: {country_name}。支持的国家有: {list(self.country_configs.keys())}")

    def get_info(self, page):
        self.cookies = self.get_cookies()

        json_data = {
            'market': self.country_config['market'],
            'page': page,
            'size': 100,
            'symbolFlag': False,
            'monthName': 'bsr_sales_nearly',
            'selectType': '2',
            'filterSub': False,
            'weightUnit': 'g',
            'order': {
                'field': 'amz_unit',
                'desc': True,
            },
            'productTags': [],
            'nodeIdPaths': self.country_config['nodeIdPaths'],
            'sellerTypes': [],
            'eligibility': [],
            'pkgDimensionTypeList': [],
            'sellerNationList': [],
            'minAmzUnit': str(self.country_config['minAmzUnit']),
            'putawayMonth': '3',
            'lowPrice': 'N',
        }
        try:
            response = requests.post(url=self.url, cookies=self.cookies, headers=self.headers, json=json_data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f'请求出错: {e}')
            return None
        except Exception as e:
            self.logger.error(f'处理响应出错: {e}')
            return None

    def parse_data(self, json_data):
        items = []
        if not json_data or 'data' not in json_data or 'items' not in json_data['data']:
            return items

        for i in json_data['data']['items']:
            item = {}
            try:
                item['发现日期'] = int(time.time() * 1000)
                item['来源平台'] = '亚马逊'
                item['商品ID'] = i.get('asin', '')
                item['图片'] = i.get('imageUrl', '')
                item['产品名称'] = i.get('title', '')

                # 根据国家生成不同的产品链接
                country_code = self.country_config['market'].lower()
                if country_code == 'uk':
                    domain = 'co.uk'
                elif country_code == 'de':
                    domain = 'de'
                elif country_code == 'fr':
                    domain = 'fr'
                elif country_code == 'es':
                    domain = 'es'
                else:
                    domain = 'com'  # 美国和其他国家默认使用.com

                item['产品链接'] = f'https://www.amazon.{domain}/dp/{i.get("asin", "")}?th=1'
                item['上架日期'] = i.get('availableDate', '')
                item['月销量'] = i.get('amzUnit', 0)
                item['在售站点'] = self.country_config['site_name']
                item['类目'] = i.get('nodeLabelPathLocale', '').split(':')[1]
                items.append(item)
            except Exception as e:
                self.logger.error(f'解析数据出错:{e}')
        return items

    """批量保存数据到CSV文件"""
    def save_batch(self, items, header, country_name):

        # 获取当前年月日，格式为 YYYYMMDD
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"{CONFIG_DIR}/ymx_get_new_{current_date}.csv"

        # 检查文件是否存在
        file_exists = os.path.exists(filename)

        # 使用追加模式写入
        with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
            f_csv = csv.DictWriter(f, fieldnames=header)
            if not file_exists:
                f_csv.writeheader()
            f_csv.writerows(items)

        self.logger.info(f"{country_name}数据已保存到文件: {filename}")

    """实现翻页，获取所有页面的数据"""
    def get_all_page(self, start_page=1, max_page=1000):
        page = start_page
        total_items = 0  # 记录总数据条数
        self.logger.info(f"开始爬取{self.current_country}站点，最小销量: {self.country_config['minAmzUnit']}")

        while page <= max_page:
            self.logger.info(f'正在爬取{self.current_country}第{page}页的数据')

            try:
                # 获取当前页的数据
                res_text = self.get_info(page)

                # 添加数据有效性检查
                if not res_text:
                    self.logger.warning(f'{self.current_country}第{page}页请求失败')
                    time.sleep(2)
                    page += 1
                    continue

                if 'data' not in res_text or 'items' not in res_text['data']:
                    self.logger.warning(f'{self.current_country}第{page}页无数据或数据结构异常')
                    break

                # 解析数据
                items = self.parse_data(res_text)
                if not items:  # 如果当前页没有数据，停止爬取
                    self.logger.info(f'{self.current_country}第{page}页无数据，停止爬取')
                    break

                # 保存数据
                if items:
                    header = items[0].keys() if items else []
                    self.save_batch(items, header,self.current_country)
                    total_items += len(items)
                    self.logger.info(f'{self.current_country}第{page}页获取到{len(items)}条数据，累计{total_items}条')

                # 添加延迟，避免请求过快
                time.sleep(1)
                page += 1

            except KeyboardInterrupt:
                self.logger.info(f"用户中断{self.current_country}爬取")
                break
            except Exception as e:
                self.logger.error(f'处理{self.current_country}第{page}页数据时出错:{e}')
                time.sleep(5)
                page += 1

        self.logger.info(f'{self.current_country}爬取完成，共获取{total_items}条数据')


def ymx_main():
    """主程序入口"""
    """爬取所有国家"""
    countries = ["美国", "英国", "德国", "法国", "西班牙"]
    for country in countries:
        try:
            print("=" * 50)
            print("亚马逊商品爬虫启动")
            print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 50)

            ymx = NewYmxNewData()
            ymx.set_country(country)
            ymx.get_all_page(start_page=1, max_page=1000)

            print(f"亚马逊{country}站点爬取完成")
            time.sleep(3)  # 国家之间间隔3秒


        except Exception as e:
            print(f"爬取{country}时出错: {e}")

            continue


if __name__ == '__main__':
    ymx_main()

    # 可自定义配置
    # custom_config = {
    #     "market": "US",
    #     "minAmzUnit": "500",  # 自定义最小销量
    #     "nodeIdPaths": ["165793011", "12345678"],  # 可以添加多个类目
    #     "site_name": "美国"
    # }
    # ymx = NewYmxNewData(custom_config)
    # ymx.get_all_page(start_page=1)
