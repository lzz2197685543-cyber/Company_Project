import requests
import asyncio
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from services.parcel_tracer.sql_save import StockInStorage
from utils.dingding_table import DingTalkDocClient, DingTalkTokenManager
from core.base_client import SMTBaseClient
import time


class StockinManager(SMTBaseClient):
    def __init__(self, shop_name, job):
        super().__init__(shop_name, job)
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
            redis_prefix="smt:stockin",
            job=job,
        )
        self.storage.create_tables()

        # # 添加入库大差异表格配置
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

    def get_today_date_range(self):
        """
        获取当前日期当天的日期范围
        返回格式：('YYYY-MM-DD', 'YYYY-MM-DD') 左闭右闭区间
        """
        today = datetime.now().strftime('%Y-%m-%d')
        return today, today

    async def fetch(self, page):
        left_date, right_date = self.get_today_date_range()

        params = {
            'query.fromSource': 'repCoListPage',
            'query.gmtCreateLeft': left_date,
            'query.gmtCreateRight': right_date,
            'query.hasReceiveDiff': 'true',
            'query.pageIndex': f'{page}',
            'query.pageSize': '100',
            'query.poTypeList': [
                '10',
                '60',
                '70',
            ],
            'query.sortByScItemId': 'true',
        }

        res_json_data = await self.post(
            'https://scm-supplier.aliexpress.com/aidc-procurement/webapi/inbound/queryInboundOrders',
            params=params
        )
        return res_json_data

    def parse(self, res_json_data):
        """解析单页数据"""
        items = []
        for i in res_json_data['data']:
            # print(i)
            item = {
                "数据爬取日期": int(time.time() * 1000),
                "店铺": self.shop_name,
                "备货单号": i['purchaseOrderNo'],
                "发货数量": i['totalQuantity'],
                '总收货数量': i['receivedQuantityDec'],
                "总上架数量": i['totalUpShelfQtyDec']
            }
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

    async def fetch_all_pages(self):
        """获取所有页面的数据"""
        all_items = []
        page = 1

        while True:
            try:
                self.logger.info(f'正在获取第{page}页数据...')
                data = await self.fetch(page)

                if 'data' not in data:
                    self.logger.error(f"第 {page} 页没有data字段")
                    break

                items = self.parse(data)

                # ==============保存异常数据=============
                # Redis 去重
                new_items = self.storage.filter_new_items(items)

                # 将去重
                all_items.extend(new_items)

                # 批量入库
                self.storage.batch_insert(new_items)

                if len(items) < 100:
                    self.logger.info(f"当前页数据并且数据为当月产生的数据不足100条，可能是最后一页，停止获取")
                    break

                page += 1

            except Exception as e:
                self.logger.error(f'获取所有数据出错:', e)

        return all_items


async def main():
    shop_name_list = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']
    for shop_name in shop_name_list:
        print(f'正在爬取店铺----{shop_name}----的数据')
        s = StockinManager(shop_name, 'smt_parcel_tracer')
        data = await s.fetch_all_pages()

# if __name__ == '__main__':
#     asyncio.run(main())
