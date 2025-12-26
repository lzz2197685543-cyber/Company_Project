import requests
import time
import os
from datetime import datetime
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, current_thread
from logger_config import SimpleLogger
import json


class NewYmxNewData:
    def __init__(self, country_config=None, file_lock=None):
        # 使用线程名作为日志名，方便区分不同线程
        thread_name = current_thread().name
        self.logger = SimpleLogger(f'YmxNews_{thread_name}')

        # 文件写入锁
        self.file_lock = file_lock or Lock()

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
        self.thread_id = thread_name

        self.cookies = {
            'ecookie': 'TKleRCxIhGfBrt8g_CN',
            '_ga': 'GA1.1.985206539.1763968727',
            '_gcl_au': '1.1.1494557471.1763968727',
            'MEIQIA_TRACK_ID': '35unGZF1woG8USPBEaCTP0BTIvN',
            'MEIQIA_VISIT_ID': '35unGTF2YOUiYrPo67haJWGQaKk',
            'd68f353782d8991859d6': '58010fe27586378c3aa758221cd0bcd7',
            'cde95a00a693e80deb81': 'ba41853d69a2dae4731b9486b2d7060b',
            '_fp': '7669a35f27c744c45575905baf08a872',
            'JSESSIONID': '59B6DD990FA5A8CC10876D01C372F4CF',
            '_ga_CN0F80S6GL': 'GS2.1.s1765854541$o2$g1$t1765855007$j60$l0$h0',
            '_gaf_fp': '20dc24d0d760e7f170a230cf5b0f5629',
            'rank-login-user': '2162195671YA5wQXw7csYJASJgTzlRog/uymH6n7gzSz+tDINdn0wnp/KbOMjE1DW/UFYuftc9',
            'rank-login-user-info': '"eyJuaWNrbmFtZSI6IkJBSzIwMjMiLCJpc0FkbWluIjpmYWxzZSwiYWNjb3VudCI6IkJBSzIwMjMiLCJ0b2tlbiI6IjIxNjIxOTU2NzFZQTV3UVh3N2NzWUpBU0pnVHpsUm9nL3V5bUg2bjdnelN6K3RESU5kbjB3bnAvS2JPTWpFMURXL1VGWXVmdGM5In0="',
            'Sprite-X-Token': 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjE2Nzk5NjI2YmZlMDQzZTBiYzI5NTEwMTE4ODA3YWExIn0.eyJqdGkiOiJEaGpPNEdiVDlhRUdzQmY1OTJpUm9nIiwiaWF0IjoxNzY1ODU1MDEyLCJleHAiOjE3NjU5NDE0MTIsIm5iZiI6MTc2NTg1NDk1Miwic3ViIjoieXVueWEiLCJpc3MiOiJyYW5rIiwiYXVkIjoic2VsbGVyU3BhY2UiLCJpZCI6NzcxNzk0LCJwaSI6NzAzOTksIm5uIjoi5omY566hIiwic3lzIjoiU1NfQ04iLCJlZCI6Ik4iLCJlbSI6IkJBSzIwMjNAc2VsbGVyc3ByaXRlLmNvbSIsIm1sIjoiUyIsImVuZCI6MTc5NDE5NDIxMjQyNH0.PPR0RrQBds_M7DhEJE0btKZl31Olpe2obI5uPKCTlNYwusxegxtN2S6-cSDBkTq8w6F0-PvNQIcjqarjtnL_xq-XxBL477nQ9LuwT4RbWj_0aCfgeGpWtpPQZBEvserCOCcRIvWTx9hDD2a-noD6wRFcj5WcF5T_nd7qtahZwlvkSjp0Wa4-8f1iMQSl4XRmQHHROAx7gg5XQJm7nrIFWNbC5KnTO0BuQIMSDSLvt9Lf6UHcJ-Qkq4RqYj6BD6QsDiiOiU4IEcLSf4JK1JJ6rRWH2woBsOqMr7j2vQs3mxQHzNtBtTRp-lgKr_laR4tNFbTyg0c1k2y8CwGmvrvNLg',
            'ao_lo_to_n': '"2162195671YA5wQXw7csYJASJgTzlRog/uymH6n7gzSz+tDINdn0yOTcaIX3bUxu8bFSfySXiusEzH6tR98oG4QH/0a8uNthvcmp6Kd4gpGMCxYS8WwWQ="',
            '_ga_38NCVF2XST': 'GS2.1.s1765854537$o6$g1$t1765855007$j40$l0$h1670407018',
        }

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
        self.data_dir = "./data/data"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            self.logger.info(f"创建目录: {self.data_dir}")

    def get_cookies(self):
        try:
            with open('./data/sellersprite_cookie_dict.json', 'r', encoding='utf-8') as f:
                return json.loads(f.read())
        except Exception as e:
            print(f'获取cookie出错:{e}')

    def set_country(self, country_name):
        """设置要爬取的国家"""
        if country_name in self.country_configs:
            self.country_config = self.country_configs[country_name]
            self.current_country = country_name
            self.logger.info(f"线程 {self.thread_id}: 已设置为爬取{country_name}站点")
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
            self.logger.error(f'线程 {self.thread_id} 请求出错: {e}')
            return None
        except Exception as e:
            self.logger.error(f'线程 {self.thread_id} 处理响应出错: {e}')
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
                item['类目'] = i.get('nodeLabelPathLocale', '').split(':')[1] if i.get('nodeLabelPathLocale') else ''
                items.append(item)
            except Exception as e:
                self.logger.error(f'线程 {self.thread_id} 解析数据出错:{e}')
        return items

    def save_batch(self, items, header, country_name):
        # 获取当前年月日，格式为 YYYYMMDD
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"./data/data/ymx_get_new_{current_date}.csv"

        # 使用锁保护文件写入
        with self.file_lock:
            # 检查文件是否存在
            file_exists = os.path.exists(filename)

            # 使用追加模式写入
            with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
                f_csv = csv.DictWriter(f, fieldnames=header)
                if not file_exists:
                    f_csv.writeheader()
                f_csv.writerows(items)

        self.logger.info(f"线程 {self.thread_id} ({country_name}): 已保存{len(items)}条数据到文件")

    def get_all_page(self, start_page=1, max_page=1000):
        """实现翻页，获取所有页面的数据"""
        page = start_page
        total_items = 0  # 记录总数据条数

        # 在日志中显示线程信息和国家信息
        thread_info = f"线程 {self.thread_id}"
        country_info = f"{self.current_country}"
        self.logger.info(f"{thread_info} 开始爬取{country_info}站点，最小销量: {self.country_config['minAmzUnit']}")

        while page <= max_page:
            self.logger.info(f'{thread_info} 正在爬取{country_info}第{page}页的数据')

            try:
                # 获取当前页的数据
                res_text = self.get_info(page)

                # 添加数据有效性检查
                if not res_text:
                    self.logger.warning(f'{thread_info} {country_info}第{page}页请求失败')
                    time.sleep(2)
                    page += 1
                    continue

                if 'data' not in res_text or 'items' not in res_text['data']:
                    self.logger.warning(f'{thread_info} {country_info}第{page}页无数据或数据结构异常')
                    break

                # 解析数据
                items = self.parse_data(res_text)
                if not items:  # 如果当前页没有数据，停止爬取
                    self.logger.info(f'{thread_info} {country_info}第{page}页无数据，停止爬取')
                    break

                # 保存数据
                if items:
                    header = items[0].keys() if items else []
                    self.save_batch(items, header, self.current_country)
                    total_items += len(items)
                    self.logger.info(
                        f'{thread_info} {country_info}第{page}页获取到{len(items)}条数据，累计{total_items}条')

                # 添加延迟，避免请求过快
                time.sleep(1)
                page += 1

            except KeyboardInterrupt:
                self.logger.info(f"{thread_info} 用户中断{country_info}爬取")
                break
            except Exception as e:
                self.logger.error(f'{thread_info} 处理{country_info}第{page}页数据时出错:{e}')
                time.sleep(5)
                page += 1

        self.logger.info(f'{thread_info} {country_info}爬取完成，共获取{total_items}条数据')
        return total_items


def crawl_country(country_name, file_lock):
    """线程任务函数：爬取单个国家"""
    thread_name = current_thread().name
    print(f"[{thread_name}] 开始爬取{country_name}站点")

    try:
        ymx = NewYmxNewData(file_lock=file_lock)
        ymx.set_country(country_name)
        total_items = ymx.get_all_page(start_page=1, max_page=1000)

        print(f"[{thread_name}] √ {country_name}: 爬取成功，获取{total_items}条数据")
        return {
            "thread": thread_name,
            "country": country_name,
            "status": "success",
            "total_items": total_items
        }
    except Exception as e:
        print(f"[{thread_name}] × {country_name}: 爬取失败，错误: {e}")
        return {
            "thread": thread_name,
            "country": country_name,
            "status": "error",
            "error": str(e)
        }

def ymx_main_thread_pool(max_workers=3):
    """使用线程池的主程序入口"""
    print("=" * 60)
    print("亚马逊商品爬虫启动（线程池版）")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"线程池大小: {max_workers}")
    print(f"目标国家: 美国、英国、德国、法国、西班牙")
    print("=" * 60)
    print("分配任务中...")

    countries = ["美国", "英国", "德国", "法国", "西班牙"]

    # 创建文件锁，确保线程安全地写入文件
    file_lock = Lock()

    # 统计信息
    success_count = 0
    error_count = 0
    total_items_all = 0
    thread_results = {}

    print("\n启动线程池，开始并发爬取...")
    print("-" * 60)

    # 创建线程池
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="YmxThread") as executor:
        # 提交所有任务
        future_to_country = {
            executor.submit(crawl_country, country, file_lock): country
            for country in countries
        }

        # 显示线程分配信息
        print(f"任务分配完成:")
        for future, country in future_to_country.items():
            print(f"  - {country} -> 已提交到线程池")

        print("\n等待任务执行...")
        print("-" * 60)

        # 等待任务完成并处理结果
        completed_count = 0
        for future in as_completed(future_to_country):
            completed_count += 1
            country = future_to_country[future]

            try:
                result = future.result(timeout=300)  # 5分钟超时
                thread_name = result.get("thread", "未知线程")

                if result["status"] == "success":
                    success_count += 1
                    total_items = result.get("total_items", 0)
                    total_items_all += total_items
                    thread_results[thread_name] = {
                        "country": country,
                        "status": "成功",
                        "items": total_items
                    }
                    print(
                        f"[进度 {completed_count}/{len(countries)}] {thread_name}: √ {country} 完成，获取{total_items}条数据")
                else:
                    error_count += 1
                    thread_results[thread_name] = {
                        "country": country,
                        "status": "失败",
                        "error": result.get("error", "未知错误")
                    }
                    print(f"[进度 {completed_count}/{len(countries)}] {thread_name}: × {country} 失败")

            except Exception as e:
                error_count += 1
                print(f"[进度 {completed_count}/{len(countries)}] 处理{country}结果时出错: {e}")

    print("\n" + "=" * 60)
    print("所有国家爬取完成！")
    print("=" * 60)

    # 详细统计信息
    print("\n详细统计:")
    print("-" * 40)
    for thread_name, result in thread_results.items():
        if result["status"] == "成功":
            print(f"{thread_name}: {result['country']} - {result['status']} ({result['items']}条数据)")
        else:
            print(f"{thread_name}: {result['country']} - {result['status']} ({result.get('error', '未知错误')})")

    print("-" * 40)
    print(f"总结:")
    print(f"  成功: {success_count}个国家")
    print(f"  失败: {error_count}个国家")
    print(f"  总数据量: {total_items_all}条")
    print(f"  结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 保存统计结果到文件
    save_statistics(thread_results, total_items_all, success_count, error_count)

def save_statistics(thread_results, total_items, success_count, error_count):
    """保存爬取统计信息到文件"""
    current_date = datetime.now().strftime("%Y%m%d")
    stats_file = f"./logs/ymx_stats_{current_date}.txt"

    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("亚马逊商品爬虫统计报告\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")

        f.write("线程执行详情:\n")
        f.write("-" * 40 + "\n")
        for thread_name, result in thread_results.items():
            if result["status"] == "成功":
                f.write(f"{thread_name}: {result['country']} - {result['status']} ({result['items']}条数据)\n")
            else:
                f.write(
                    f"{thread_name}: {result['country']} - {result['status']} ({result.get('error', '未知错误')})\n")

        f.write("\n" + "-" * 40 + "\n")
        f.write(f"总结统计:\n")
        f.write(f"  成功国家数: {success_count}\n")
        f.write(f"  失败国家数: {error_count}\n")
        f.write(f"  总数据量: {total_items}条\n")
        f.write("=" * 60 + "\n")

    print(f"统计信息已保存到: {stats_file}")


def ymx_main_sequential():
    """原来的顺序执行版本（保留作为备选）"""
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
    # 使用线程池版本（推荐）
    ymx_main_thread_pool(max_workers=5)  # 可以调整线程数

    # 或者使用原来的顺序版本
    # ymx_main_sequential()