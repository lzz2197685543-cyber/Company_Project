import requests
import time
from datetime import datetime,timedelta
from urllib.parse import quote

import os
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from queue import Queue
import traceback
from logger_config import SimpleLogger
import json



class TemuNews:
    def __init__(self, max_workers=5):
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

        # 多线程相关
        self.max_workers = max_workers
        self.data_queue = Queue()
        self.total_items = 0
        self.lock = threading.Lock()
        self.stats = {
            'start_time': datetime.now(),
            'end_time': None,
            'total_pages': 0,
            'success_pages': 0,
            'failed_pages': 0,
            'total_items': 0
        }

        # 创建数据目录
        self.data_dir = "./data/data"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            self.logger.info(f"创建目录: {self.data_dir}")

        # 控制爬取的变量
        self.should_stop = False
        self.max_page_to_crawl = 1000

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

    def get_info(self, page):
        self.headers['authorization'] = self.get_authorization()

        """获取单页数据"""
        if self.should_stop:
            return None

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

        try:
            response = requests.get(url=self.url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            json_data = response.json()

            # 检查是否有数据
            if not json_data or 'data' not in json_data:
                self.logger.warning(f'第{page}页返回数据格式异常')
                return None

            return json_data
        except requests.exceptions.RequestException as e:
            self.logger.error(f'第{page}页请求失败: {e}')
            return None
        except Exception as e:
            self.logger.error(f'第{page}页处理出错: {e}')
            return None

    def parse_data(self, json_data):
        """解析数据"""
        items = []
        if not json_data or 'data' not in json_data or 'list' not in json_data['data']:
            return items

        # 检查是否没有数据了
        if not json_data['data']['list']:
            self.logger.info(f'没有更多数据了')
            self.should_stop = True
            return items

        for i in json_data['data']['list']:
            try:
                item = {
                    '发现日期': int(time.time() * 1000),
                    '来源平台': 'temu',
                    '商品ID': i.get('goodsId', ''),
                    '图片': i.get('thumbnail', ''),
                    '产品名称': i.get('goodsName', ''),
                    '上架日期': i.get('createTime', ''),
                    '总销量': i.get('sold', 0),
                    '月销量': i.get('monthSold', 0),
                    '托管模式': self.mode_dict.get(i.get('hostingMode', 1), '未知'),
                    '在售站点': i.get('site', {}).get('cnName', ''),
                    '产品链接' : f'https://www.temu.com/search_result.html?search_key={i["goodsId"]}&search_method=user&region={i["regionId"]}&regionCnName={quote(i["site"]["cnName"])}',
                    '类目': i.get('catItems', [{}])[1].get('catName', '') if i.get('catItems') else ''
                }

                items.append(item)
            except Exception as e:
                self.logger.error(f'解析单个商品数据出错:{e}')

        return items

    def save_batch(self, items, header):
        """批量保存数据到CSV文件"""
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

        return filename

    def process_page(self, page):
        """处理单页数据的线程函数"""
        try:
            self.logger.info(f'线程 {threading.current_thread().name} 正在处理第{page}页')

            # 获取数据
            json_data = self.get_info(page)
            if not json_data:
                with self.lock:
                    self.stats['failed_pages'] += 1
                return {'page': page, 'items': [], 'success': False}

            # 解析数据
            items = self.parse_data(json_data)

            if items:
                # 获取总页数
                total_pages = json_data.get('data', {}).get('pages', 0)

                # 将数据放入队列
                self.data_queue.put({
                    'page': page,
                    'items': items,
                    'total_pages': total_pages
                })

                with self.lock:
                    self.stats['success_pages'] += 1

                return {'page': page, 'items': items, 'success': True, 'count': len(items)}
            else:
                return {'page': page, 'items': [], 'success': False}

        except Exception as e:
            self.logger.error(f'处理第{page}页数据时出错: {e}')
            with self.lock:
                self.stats['failed_pages'] += 1
            return {'page': page, 'items': [], 'success': False}

    def save_worker(self):
        """保存数据的独立线程"""
        header_written = False
        header = []
        items_buffer = []
        buffer_size = 50  # 每50条数据保存一次
        save_count = 0

        while not self.should_stop or not self.data_queue.empty():
            try:
                # 从队列获取数据，设置超时时间
                data = self.data_queue.get(timeout=3)

                # 更新总页数（取第一次获取的值）
                if data['total_pages'] > 0 and self.stats['total_pages'] == 0:
                    with self.lock:
                        self.stats['total_pages'] = data['total_pages']

                # 获取表头
                if data['items'] and not header_written:
                    header = list(data['items'][0].keys())
                    header_written = True

                # 添加到缓冲区
                if data['items']:
                    items_buffer.extend(data['items'])

                # 缓冲区达到阈值时保存
                if len(items_buffer) >= buffer_size and header:
                    filename = self.save_batch(items_buffer, header)
                    with self.lock:
                        self.stats['total_items'] += len(items_buffer)
                    save_count += 1
                    self.logger.info(f'第{save_count}次保存: {len(items_buffer)}条数据到 {filename}')
                    items_buffer = []

                self.data_queue.task_done()

            except Exception as e:
                if "empty" in str(e).lower():  # 队列空异常
                    continue
                else:
                    self.logger.warning(f'保存线程异常: {e}')

        # 保存缓冲区剩余数据
        if items_buffer and header:
            filename = self.save_batch(items_buffer, header)
            with self.lock:
                self.stats['total_items'] += len(items_buffer)
            self.logger.info(f'最终保存: {len(items_buffer)}条数据到 {filename}')

    def get_total_pages(self):
        """获取总页数"""
        try:
            json_data = self.get_info(1)
            if json_data and 'data' in json_data:
                total_pages = json_data['data'].get('pages', 0)
                self.logger.info(f'获取到总页数: {total_pages}')
                return total_pages
        except Exception as e:
            self.logger.error(f'获取总页数失败: {e}')
        return 0

    def get_all_page_multithread(self):
        """多线程获取所有页面数据"""
        # 先获取总页数
        total_pages = self.get_total_pages()
        if total_pages > 0:
            self.max_page_to_crawl = min(total_pages, self.max_page_to_crawl)
            self.logger.info(f'需要爬取 {self.max_page_to_crawl} 页数据')

        # 启动保存线程
        save_thread = threading.Thread(target=self.save_worker, name='SaveThread')
        save_thread.daemon = True
        save_thread.start()

        # 使用线程池处理页面
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix='TemuWorker') as executor:
            # 所有待处理页面的列表
            pages_to_process = list(range(1, self.max_page_to_crawl + 1))
            futures = {}

            # 首先提交第一批任务
            initial_batch = pages_to_process[:self.max_workers * 2]
            for page in initial_batch:
                future = executor.submit(self.process_page, page)
                futures[future] = page

            # 记录已处理的页面
            processed_pages = set(initial_batch)

            # 处理任务直到所有页面都处理完或应该停止
            while futures and not self.should_stop:
                try:
                    # 等待任意一个任务完成
                    done_futures, _ = wait(futures.keys(), return_when=FIRST_COMPLETED, timeout=10)

                    if not done_futures:
                        self.logger.warning("等待任务完成超时")
                        continue

                    # 处理已完成的任务
                    for future in done_futures:
                        page_num = futures.pop(future)

                        try:
                            result = future.result()
                            if result and result.get('success'):
                                self.logger.info(f'第{page_num}页处理完成，获取{result.get("count", 0)}条数据')
                            elif result and not result.get('success'):
                                self.logger.warning(f'第{page_num}页处理失败')
                        except Exception as e:
                            self.logger.error(f'第{page_num}页任务执行出错: {e}')

                        # 提交新任务（如果有更多页面需要处理）
                        next_page = None
                        for page in pages_to_process:
                            if page not in processed_pages and page <= self.max_page_to_crawl:
                                next_page = page
                                processed_pages.add(page)
                                break

                        if next_page and not self.should_stop:
                            new_future = executor.submit(self.process_page, next_page)
                            futures[new_future] = next_page
                            self.logger.debug(f'提交新任务: 第{next_page}页')

                except Exception as e:
                    self.logger.error(f'任务调度出错: {e}')
                    break

            # 等待所有剩余任务完成
            if futures:
                self.logger.info(f'等待剩余 {len(futures)} 个任务完成...')
                for future in futures:
                    try:
                        result = future.result(timeout=30)
                        page_num = futures[future]
                        if result and result.get('success'):
                            self.logger.info(f'第{page_num}页处理完成，获取{result.get("count", 0)}条数据')
                    except Exception as e:
                        self.logger.error(f'等待第{page_num}页任务完成时出错: {e}')

        # 设置停止标志并等待保存线程完成
        self.should_stop = True
        save_thread.join(timeout=10)

        # 更新统计信息
        self.stats['end_time'] = datetime.now()
        self.print_stats()

    def print_stats(self):
        """打印统计信息"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        print("\n" + "=" * 60)
        print("爬虫执行统计")
        print("=" * 60)
        print(f"开始时间: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"结束时间: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总耗时: {duration:.2f} 秒")
        print(f"总页数: {self.stats['total_pages']}")
        print(f"成功页数: {self.stats['success_pages']}")
        print(f"失败页数: {self.stats['failed_pages']}")
        print(f"获取数据总数: {self.stats['total_items']} 条")
        if duration > 0:
            print(f"平均速度: {self.stats['total_items'] / duration:.2f} 条/秒")
        print("=" * 60)

def temu_new_run():
    """主程序入口"""
    try:
        print("=" * 60)
        print("Temu商品多线程爬虫启动")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"线程数: 5")
        print("=" * 60)

        # 创建爬虫实例，设置线程数
        temu_crawler = TemuNews(max_workers=5)

        # 执行多线程爬取
        temu_crawler.get_all_page_multithread()

        print("=" * 60)
        print("爬虫执行完成")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        print(f"程序执行出错: {e}")
        traceback.print_exc()


if __name__ == '__main__':
    temu_new_run()