import requests
import time
from datetime import datetime,timedelta
import os
import csv
from logger_config import SimpleLogger
import json
from urllib.parse import quote


class TemuNews:
    def __init__(self):
        self.logger = SimpleLogger('TemuNews')
        self.mode_dict = {
            1: "全托",
            2: "半托"
        }
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnZWVrYmkiLCJpZCI6IjM0Nzg5IiwiaWF0IjoxNzY1ODc3MTc2LCJleHAiOjE3NjcxNzMxNzZ9.V2wvPx4A5jLOOfEiBagnHXKi6B_vMNEMr4svTVRXCcI',
            'origin': 'https://www.geekbi.com',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        }
        self.url = 'https://api.geekbi.com/api/v1/temu/goods/search'

    def get_authorization(self):
        try:
            with open('./data/authorization.json', 'r', encoding='utf-8') as f:
                return json.loads(f.read())['authorization']
        except Exception as e:
            self.logger.error(f'获取authorization出错:{e}')

    def get_last_three_months(self):
        # 获取当前时间（UTC）
        now = datetime.utcnow()

        # 计算三个月前的时间
        three_months_ago = now - timedelta(days=90)

        # 格式化为ISO字符串（带Z表示UTC）
        on_sale_time_min = three_months_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        on_sale_time_max = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        return {
            'onSaleTimeMin': on_sale_time_min,
            'onSaleTimeMax': on_sale_time_max
        }

    def get_info(self,page):
        params = {
            'matchMode': '2',
            'catIds': '25439,26246',
            'siteId': '1000',
            'monthSalesMin': '300',
            'status': '1',
            'sort': 'monthSold',
            'order': 'descend',
            'showPreference': '2',
            'onSaleTimeMin': self.get_last_three_months()['onSaleTimeMin'],
            'onSaleTimeMax': self.get_last_three_months()['onSaleTimeMax'],
            'page': f'{page}',
            'size': '100',
        }
        self.headers['authorization']=self.get_authorization()
        try:
            response = requests.get(url=self.url, params=params, headers=self.headers)
        except Exception as e:
            print(f'请求响应出错:{e}')
        return response.json()

    def parse_data(self, json_data):
        items = []
        for i in json_data['data']['list']:
            item = {}
            try:
                item['发现日期'] = int(time.time() * 1000)
                item['来源平台'] = 'temu'
                item['商品ID'] = i['goodsId']
                item['图片'] = i['thumbnail']
                item['产品名称'] = i['goodsName']
                item['上架日期'] = i['createTime']
                item['总销量'] = i['sold']
                item['月销量'] = i['monthSold']
                item['托管模式'] = self.mode_dict[i['hostingMode']]
                item['在售站点'] = i['site']['cnName']
                item[
                    '产品链接'] = f'https://www.temu.com/search_result.html?search_key={i["goodsId"]}&search_method=user&region={i["regionId"]}&regionCnName={quote(i["site"]["cnName"])}'
                item['类目'] = i['catItems'][2]['catName']
                if i['monthSold']>300:
                    items.append(item)
                else:
                    self.logger.info('月销量小于300了程序结束')
                    break
            except Exception as e:
                self.logger.error(f'解析数据出错:{e}')
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
        filename = f"./data/data/temu_get_new_{current_date}.csv"

        # 检查文件是否存在
        file_exists = os.path.exists(filename)

        # 使用追加模式写入
        with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
            f_csv = csv.DictWriter(f, fieldnames=header)
            if not file_exists:
                f_csv.writeheader()
            f_csv.writerows(items)

        self.logger.info(f"已保存到文件: {filename}")

    """实现翻页，获取所有页面的数据"""
    def get_all_page(self):
        page = 1
        max_page = 1000
        total_items = 0  # 记录总数据条数
        while page <= max_page:
            self.logger.info(f' 正在爬取第{page}页的数据')

            try:
                # 获取当前页的数据
                res_text = self.get_info(page)

                # 添加数据有效性检查
                if not res_text or 'data' not in res_text or 'list' not in res_text['data']:
                    self.logger.warning(f'第{page}页无数据或数据结构异常')
                    break

                # 解析数据
                items = self.parse_data(res_text)
                if not items:  # 如果当前页没有数据，停止爬取
                    self.logger.info(f'第{page}页无数据，停止爬取')
                    break

                # 保存数据
                if items:
                    header = items[0].keys() if items else []
                    self.save_batch(items, header)
                    total_items += len(items)
                    self.logger.info(f'第{page}页获取到{len(items)}条数据，累计{total_items}条')

                # 检查是否还有更多页
                if page >= res_text.get('data', {}).get('pages', page + 1):
                    break

                page+=1

            except Exception as e:
                self.logger.error(f'处理第{page}页数据时出错: {e}')
                break
        self.logger.info(f'爬取完成，共获取{total_items}条数据')



def temu_new_run():
    """主程序入口"""
    try:
        print("=" * 50)
        print("Temu商品爬虫启动")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)

        temu_crawler = TemuNews()
        temu_crawler.get_all_page()

        print("=" * 50)
        print("爬虫执行完成")
        print("=" * 50)

    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        print(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    temu_new_run()


