from logger_config import SimpleLogger
import requests
import time
import os
import csv
from datetime import datetime
import json

class Shein:
    def __init__(self,shop_name):
        self.shop_name=shop_name

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

        self.logger = SimpleLogger(name='Shein_sale')

    def get_cookies(self):
        try:
            with open(f'./data/{self.shop_name}_cookies.json', 'r',encoding='utf-8') as f:
                self.cookies = json.loads(f.read())
                return self.cookies['cookies']
        except Exception as e:
            print('获取cookie失败')
            self.logger.info(f'获取cookie失败:{e}')

    """获取指定页面的数据"""
    def get_info(self, page, max_retries=3):
        self.cookies = self.get_cookies()

        json_data = {
            'pageNumber': page,
            'pageSize': 100,
            'sortBy7dSaleCnt': 2,
        }

        for attempt in range(max_retries):
            try:
                # 发送请求，设置超时防止卡死
                response = requests.post(
                    url=self.url,
                    cookies=self.cookies,
                    headers=self.headers,
                    json=json_data,
                    timeout=15
                )

                # 检查响应状态
                response.raise_for_status()

                # 尝试解析JSON
                data = response.json()

                # 简单检查数据有效性
                if data and 'info' in data:
                    return data
                else:
                    self.logger.warning(f"第{page}页返回数据格式不完整")
                    print(f"第{page}页返回数据格式不完整")
                    return None

            except requests.exceptions.Timeout:
                self.logger.warning(f"第{page}页请求超时 (第{attempt + 1}次重试)")
                print(f"第{page}页请求超时 (第{attempt + 1}次重试)")
                if attempt < max_retries - 1:
                    time.sleep(2)  # 等待2秒后重试
                    continue

            except requests.exceptions.ConnectionError:
                self.logger.warning(f"第{page}页连接错误 (第{attempt + 1}次重试)")
                print(f"第{page}页连接错误 (第{attempt + 1}次重试)")
                if attempt < max_retries - 1:
                    time.sleep(3)  # 等待3秒后重试
                    continue

            except requests.exceptions.HTTPError as e:
                self.logger.error(f"第{page}页HTTP错误: {e}")
                print(f"第{page}页HTTP错误: {e}")
                return None  # HTTP错误通常不需要重试

            except requests.exceptions.RequestException as e:
                self.logger.error(f"第{page}页请求异常: {e}")
                print(f"第{page}页请求异常: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue

            except ValueError:
                self.logger.error(f"第{page}页返回的不是有效的JSON")
                print(f"第{page}页返回的不是有效的JSON")
                return None  # JSON解析失败不需要重试

            except Exception as e:
                self.logger.error(f"第{page}页获取数据时发生未知错误: {e}")
                print(f"第{page}页获取数据时发生未知错误: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue

        # 所有重试都失败
        self.logger.error(f"第{page}页获取失败，已重试{max_retries}次")
        return None

    """解析返回的数据"""
    def parse_data(self, json_data):
        items = []
        try:
            if not json_data or 'info' not in json_data:
                self.logger.info(f'返回的数据格式不正确')
                print(f'返回的数据格式不正确')
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
                                print(f"✅ 有效数据: {item}")
                                items.append(item)
                            else:
                                pass
                                # print(f"⏭️  跳过零数据: {item['商品名称']} - {item['sku']}")

                except Exception as e:
                    print(f"❌ 解析单个商品数据时出错: {e}")
                    self.logger.error(f"解析单个商品数据时出错: {e}")
                    continue  # 继续处理下一个商品

        except Exception as e:
            print(f'❌ 解析数据时发生错误: {e}')
            self.logger.error(f'解析数据时发生错误: {e}')

        print(f"📊 解析完成，共找到 {len(items)} 条有效数据")
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
        filename = f"./data/data/shein_sale_{current_date}.csv"

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
    def get_all_page(self):
        page = 1
        max_page = 100
        while page < max_page:
            try:
                print(f'🔍 正在爬取---{self.shop_name}---第{page}页的数据')
                self.logger.info(f'正在爬取---{self.shop_name}---第{page}页的数据')

                # 获取当前页的数据
                json_data = self.get_info(page)

                if not json_data['info']['list']:
                    print(f'❌ 第{page}页已经没有数据了，程序结束')
                    self.logger.info(f'第{page}页已经没有数据了,程序结束')
                    break

                # 解析数据
                items = self.parse_data(json_data)

                # 数据不为空才进行保存
                if items:
                    # 保存数据
                    header = ['平台', '店铺', '商品名称', 'sku', '抓取数据日期', '今日销量', '近7天销量', '近30天销量',
                              '平台库存', '在途库存']
                    self.save_batch(items, header)

                # 等待1秒继续下一页
                time.sleep(1)

                page += 1
            except KeyboardInterrupt:
                self.logger.info("用户中断爬取")
                print("用户中断爬取")
                break


def run_shein_sale():
    name_list = ["希音全托301-yijia", "希音全托302-juyule", "希音全托303-kedi", "希音全托304-xiyue"]
    for shop_name in name_list:
        print(f'开始爬取店铺---{shop_name}---的数据')
        shein = Shein(shop_name)
        shein.get_all_page()


if __name__ == '__main__':
    run_shein_sale()
