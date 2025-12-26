import requests
import time
from logger_config import SimpleLogger
import os
import csv
from datetime import datetime
import json

class Shopee:
    def __init__(self,shop_name):
        self.shop_name = shop_name
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'content-type': 'application/json',
            # 'cookie': 'SPC_CDS=32bde343-e625-425e-ac09-187a09cdda08; _gid=GA1.2.792727857.1745293080; _gat=1; _ga=GA1.1.2112181742.1745293080; _ga_N181PS3K9C=GS2.1.s1758791314$o6$g1$t1758791449$j22$l0$h852510014; language=en; x_region=CN; biz_type=SCS; lang_id=zhCN; userEmail=work2207303@163.com; srmid=a60ccd0bcdd6d24b118e46f8af413ce8; csrf_token=93443001c499a8232650aace06cf1ad1657f5c5e7e8502a0c2e6f9c331cb06b5',
            'origin': 'https://seller.scs.shopee.cn',
            'priority': 'u=1, i',
            'referer': 'https://seller.scs.shopee.cn/inventory/current-inventory-list',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-business-type': 'SCS',
            'x-lang-id': 'zhCN',
            'x-portal-type': 'SRM',
            'x-region': 'CN',
            'x-request-id': 'edd98f73-3306-45fa-8b09-58438e5e9f5a',
        }
        self.cookie=None
        self.url = 'https://seller.scs.shopee.cn/api/v4/srm/sales_inventory/list'
        self.logger=SimpleLogger('shopee_sales_data')

    def get_cookies(self):
        try:
            with open(f'./data/{self.shop_name}_cookies.json', 'r',encoding='utf-8') as f:
                self.cookies = json.loads(f.read())
                return self.cookies
        except Exception as e:
            print('获取cookie失败')
            self.logger.info('获取cookie失败')

    def get_info(self, page):
        self.cookies=self.get_cookies()['cookies']
        json_data = {
            'page_no': page,
            'count': 100,
            'fields_filter': {},
            'whs_region': 'CN',
            'order_by': 3,
            'is_asc': 0,
        }

        try:
            response = requests.post(url=self.url,headers=self.headers,cookies=self.cookies,json=json_data,timeout=10)
            response.raise_for_status()
            return response.json()

        except (requests.RequestException, ValueError) as e:
            print(f"获取页面 {page} 数据失败: {e}")
            self.logger.error(f"获取页面 {page} 数据失败: {e}")

            return None  # 或返回空字典：return {}

    """解析一页的数据"""
    def parse_data(self,json_data):
        items = []
        try:
            if not json_data or 'data' not in json_data:
                self.logger.info('返回的数据格式不正确')
                print('返回的数据格式不正确')
                self.logger.error('返回的数据格式不正确')
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
                    print(f"❌ 解析单个商品数据时出错: {e}")
                    self.logger.error(f"解析单个商品数据时出错: {e}")
                    continue  # 继续处理下一个商品
        except Exception as e:
            print(f'❌ 解析数据时发生错误: {e}')
            self.logger.error(f'解析数据时发生错误: {e}')

        print(f"📊 解析完成，共找到 {len(items)} 条有效数据")
        self.logger.info(f'解析完成，共找到 {len(items)} 条有效数据')
        return items

    """批量保存数据到CSV文件"""
    def save_batch(self, items, header):
        """批量保存数据到CSV文件"""
        data_dir = "./data/data"
        if not os.path.exists(data_dir):
            try:
                os.makedirs(data_dir)
                print(f"📁 创建目录: {data_dir}")
                self.logger.info(f"创建目录: {data_dir}")
            except Exception as e:
                print(f"❌ 创建目录失败: {e}")
                self.logger.error(f"创建目录失败: {e}")
                return

        # 获取当前年月日，格式为 YYYYMMDD
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"./data/data/shopee_sale_{current_date}.csv"

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

    """获取所有页面的数据"""
    def get_all_page(self):
        page = 1
        max_page = 100
        while page < max_page:
            try:
                print(f'🔍 正在爬取---{self.shop_name}---第{page}页的数据')
                self.logger.info(f'正在爬取---{self.shop_name}---第{page}页的数据')

                # 获取当前页的数据
                json_data = self.get_info(page)

                if not json_data['data']['sales_inventory_info']:
                    print(f'❌ 第{page}页已经没有数据了，程序结束')
                    self.logger.info(f'第{page}页已经没有数据了,程序结束')
                    break

                # 解析数据
                items = self.parse_data(json_data)


                # 数据不为空才进行保存
                if items:
                    # 保存数据
                    header = ['平台', '店铺', '商品名称', 'sku', '抓取数据日期', '今日销量', '近7天销量',
                              '近30天销量',
                              '平台库存', '在途库存']
                    self.save_batch(items, header)

                # 等待1秒继续下一页
                time.sleep(1)

                page += 1
            except KeyboardInterrupt:
                self.logger.info("用户中断爬取")
                print("用户中断爬取")
                break

def run_shopee_sale():
    name_list = ["虾皮全托1501店", "虾皮全托507-lxz","虾皮全托506-kedi", "虾皮全托505-qipei","虾皮全托504-huanchuang","虾皮全托503-juyule","虾皮全托502-xiyue","虾皮全托501-quzhi"]
    for shop_name in name_list:
        print(f'开始爬取店铺---{shop_name}---的数据')
        shein = Shopee(shop_name)
        shein.get_all_page()


if __name__ == '__main__':
    run_shopee_sale()