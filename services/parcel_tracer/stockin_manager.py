import asyncio
from core.base_client import TemuBaseClient
import random
import time
from services.parcel_tracer.sql_save import StockInStorage
from utils.webchat_send import webchat_send

"""备货单--入库"""

class Stockin_Manager(TemuBaseClient):
    URL = 'https://agentseller.temu.com/mms/venom/api/supplier/purchase/manager/querySubOrderList'

    def __init__(self, shop_name, logger_name):
        super().__init__(shop_name, logger_name)
        self.storage = StockInStorage(
            mysql_conf={
                "host": "localhost",
                "user": "root",
                "password": "1234",
                "database": "py_spider"
            },
            redis_conf={
                "host": "localhost",
                "port": 6379,
                "db": 0
            },
            redis_prefix="temu:purchase"
        )

        self.storage.create_table()

    async def fetch_page(self,page):
        payload = {
            'pageNo': page,
            'pageSize': 100,
            'urgencyType': 0,
            'isCustomGoods': False,
            'statusList': [
                7,
            ],
            'oneDimensionSort': {
                'firstOrderByParam': 'statusFinishTime',
                'firstOrderByDesc': 1,
            },
        }
        data = await self.post(self.URL, payload)
        return data

    async def fetch_all_pages(self):
        """获取所有页面的数据"""
        all_items=[]
        page=1
        total_items = 0

        while True:
            try:
                self.logger.info(f"正在获取第 {page} 页数据...")
                data = await self.fetch_page(page)

                # 检查响应是否成功
                if not data or 'success' not in data or not data['success']:
                    self.logger.error(f"第 {page} 页请求失败: {data.get('errorMsg', '未知错误')}")
                    break

                # 检查是否有结果数据
                if 'result' not in data:
                    self.logger.error(f"第 {page} 页没有result字段")
                    break

                result = data['result']

                # 检查是否有列表数据
                if 'subOrderForSupplierList' not in result:
                    self.logger.error(f"第 {page} 页没有subOrderForSupplierList字段")
                    break

                current_items = result['subOrderForSupplierList']


                # 如果当前页没有数据，则结束循环
                if not current_items:
                    self.logger.info(f"第 {page} 页没有数据，停止获取")
                    break

                # 解析当前页数据
                parsed_items,list_item = await self._parse(data, page)

                total_items += len(list_item)

                # ==============保存异常数据，并且发送数据=============
                # Redis 去重
                new_items = self.storage.filter_new_items(parsed_items)

                # 将去重
                all_items.extend(new_items)

                # 批量入库
                self.storage.batch_insert(new_items)

                # 异常报警
                abnormal = self.storage.detect_abnormal(new_items)
                self.storage.alarm_abnormal(abnormal, self.logger)

                # 打印当前页信息
                self.logger.info(f"✓ 第 {page} 页获取到异常数据 {len(parsed_items)} 条数据")

                # 检查是否有更多页
                # 如果当前页获取的数据量小于pageSize，可能是最后一页
                if len(list_item) < 100:
                    self.logger.info(f"当前页数据并且数据为当月产生的数据不足100条，可能是最后一页，停止获取")
                    break

                page += 1

                # 添加短暂延迟，避免请求过快
                delay_time = random.uniform(1, 3)  # 1-3秒随机延迟
                await asyncio.sleep(delay_time)

            except KeyError as e:
                self.logger.info(f"第 {page} 页响应缺少必要字段: {e}")
                break
            except Exception as e:
                self.logger.info(f"获取第 {page} 页数据时发生异常: {e}")
                break

        return all_items

    async def _parse(self, json_data, page):
        """解析数据"""
        items = [] # 放符合异常的数据
        list_item = [] # 放判断时间是否在当前时间的一个月内的数据，要是这个数据小于100那么我们就不继续往下爬了
        try:
            order_list = json_data['result']['subOrderForSupplierList']
            current_time_ms = int(time.time() * 1000)  # 当前时间戳（毫秒）
            one_month_ms = 30 * 24 * 60 * 60 * 1000  # 30天的毫秒数（一个月）

            for i in order_list:
                purchase_time = i['purchaseTime']

                # 计算备货单创建时间与当前时间的差值
                time_diff_ms = current_time_ms - purchase_time

                # 判断是否在一个月内
                if time_diff_ms <= one_month_ms:
                    item = {
                        "数据抓取时间": current_time_ms,
                        "店铺": self.shop_name,
                        "备货单号": i['originalPurchaseOrderSn'],
                        "备货单创建时间": purchase_time,
                        "送货数": i['skuQuantityTotalInfo']['deliverQuantity'],
                        "入库数": i['skuQuantityTotalInfo']['realReceiveAuthenticQuantity'],
                        "交接时间": i['deliverInfo']['deliverTime'],
                        "收货时间": i['deliverInfo']['receiveTime']
                    }

                    list_item.append(item)

                    # 检查送货数和入库数是否相等
                    if int(item['送货数']) != int(item['入库数']):
                        items.append(item)

            return items,list_item
        except Exception as e:
            self.logger.error(f"解析第 {page} 页数据时发生异常: {e}")

async def main():
    s = Stockin_Manager("103-Temu全托管",'temu_parcel_tracer')
    skcs = await s.fetch_all_pages()

# if __name__ == '__main__':
#     asyncio.run(main())