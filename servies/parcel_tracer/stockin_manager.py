import time

from core.base_client import SheinBaseClient
import asyncio
from datetime import datetime
from servies.parcel_tracer.sql_save import StockInStorage
from utils.dingding_table import DingTalkDocClient,DingTalkTokenManager

class StockinManager(SheinBaseClient):
    URL = 'https://sso.geiwohuo.com/pfmp/order/list'

    def __init__(self, shop_name, logger_name):
        super().__init__(shop_name, logger_name)
        self.storage = StockInStorage(
            mysql_conf={
                "host": 'rm-bp186omby3lautfn0no.mysql.rds.aliyuncs.com',
                "port": 3306,
                "user": 'root_lxz',
                "password": 'Lxz123456',
                "database": 'py_spider'
            },
            redis_conf={
                "host": "r-bp1ogeji1wtu8f6ed7pd.redis.rds.aliyuncs.com",
                "password": 'Lxz123456',
                "port": 6379,
                "db": 0,
            },
            redis_prefix="shein:stockin",
            job='shein_parcel_tracer',
        )

        self.storage.create_tables()
        # 添加入库大差异表格配置
        # self.STOCKIN_BIG_DIFF_SHEET_CONFIG = {
        #     "workbook_id": "kDnRL6jAJMO3D450HBM0ogPDWyMoPYe1",  # 你的表格ID
        #     "sheet_id": "st-352d4170-19503",  # 工作表ID
        #     "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
        # }
        #
        # self.token_manager = DingTalkTokenManager()
        #
        # # 创建客户端实例
        # self.client = DingTalkDocClient(self.token_manager)

    async def fetch(self,page):
        json_data = {
            'orderType': 2,
            'status': [
                7,
            ],
            'page': page,
            # 'allocateTimeEnd': '2026-01-29 23:59:59',
            # 'allocateTimeStart': '2025-10-29 00:00:00',
            'perPage': 200,
        }
        data=await self.post(self.URL,json_data)

        return data

    def parse_time_string(self, time_str):
        """将时间字符串转换为毫秒时间戳"""
        if not time_str:
            return None

        try:
            # 清理字符串
            time_str = str(time_str).strip()

            # 尝试常见的时间格式
            time_formats = [
                "%Y-%m-%d %H:%M:%S",  # 完整格式
                "%Y-%m-%d %H:%M",  # 缺少秒数
                "%Y-%m-%d",  # 只有日期
                "%Y/%m/%d %H:%M:%S",  # 斜杠分隔
                "%Y/%m/%d %H:%M",  # 斜杠分隔缺少秒数
            ]

            for fmt in time_formats:
                try:
                    dt = datetime.strptime(time_str, fmt)
                    # 转换为毫秒时间戳
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    continue

            # 如果都不匹配，记录警告
            self.logger.warning(f"无法解析的时间格式: {time_str}")
            return None

        except Exception as e:
            self.logger.error(f"时间转换错误: {e}, time_str: {time_str}")
            return None

    def parse(self, json_data):
        items = []
        try:
            for i in json_data['info']['data']:
                # 转换时间
                deliver_time_str = i.get('deliveryTime')
                finish_time_str = i.get('finishTime')

                deliver_time_ms = self.parse_time_string(deliver_time_str)
                receive_time_ms = self.parse_time_string(finish_time_str)

                item = {
                    "数据爬取日期": int(time.time() * 1000),
                    "店铺": self.shop_name,
                    "订单号": i['sellerOrderNo'],
                    "送货数量": int(i['detail'][0]['deliveryQuantity']),
                    "上架数量": int(i['detail'][0]['groundingQuantity']),
                    "发货时间": deliver_time_ms,
                    "上架时间": receive_time_ms,
                }
                # print(item)
                if int(i['detail'][0]['deliveryQuantity'])!=int(i['detail'][0]['groundingQuantity']):
                    print('送货数量与上架数量不相等:',item)
                    items.append(item)

                # if int(item['送货数量']) - int(item['上架数量']) >= 10:
                #     single_row_data = [datetime.now().strftime('%Y-%m-%d'), self.shop_name,
                #                        str(int(item['送货数量']) - int(item['上架数量'])), item["订单号"]]
                #     self.client.insert_data_at_empty_row(
                #         self.STOCKIN_BIG_DIFF_SHEET_CONFIG["workbook_id"],
                #         self.STOCKIN_BIG_DIFF_SHEET_CONFIG["sheet_id"],
                #         self.STOCKIN_BIG_DIFF_SHEET_CONFIG["operator_id"],
                #         single_row_data
                #     )
            return items
        except Exception as e:
            self.logger.error(f'数据解析报错: {e}')
            return items

    async def fetch_all_pages(self):
        """获取所有页面的数据"""
        all_items=[]
        page=1
        while True:
            try:
                data = await self.fetch(page)

                # 检查是否有结果数据
                if 'info' not in data:
                    self.logger.error(f"第 {page} 页没有info字段")
                    break

                result = data['info']

                # 检查是否有列表数据
                if 'data' not in result:
                    self.logger.error(f"第 {page} 页没有subOrderForSupplierList字段")
                    break

                current_items = result['data']

                # 如果当前页没有数据，则结束循环
                if not current_items:
                    self.logger.info(f"第 {page} 页没有数据，停止获取")
                    break

                items=self.parse(data)

                # ==============保存异常数据=============
                # Redis 去重
                new_items = self.storage.filter_new_items(items)

                # 将去重
                all_items.extend(new_items)

                # 批量入库
                self.storage.batch_insert(new_items)

                # 异常报警
                abnormal = self.storage.detect_abnormal(new_items)
                self.storage.alarm_abnormal(abnormal, self.logger)

                if len(items) < 200:
                    self.logger.info(f"当前页数据并且数据为当月产生的数据不足200条，可能是最后一页，停止获取")
                    break

                page += 1

            except Exception as e:
                print(e)

        return all_items


async def main():
    s = StockinManager("希音全托301-yijia", 'shein_parcel_tracer')
    await s.fetch_all_pages()


# if __name__ == "__main__":
#     asyncio.run(main())