import time

from core.base_client import ShopeeBaseClient
import asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta
from servies.parcel_tracer.sql_save import StockInStorage
from utils.dingding_table import DingTalkDocClient,DingTalkTokenManager

class StockinManager(ShopeeBaseClient):
    URL = 'https://seller.scs.shopee.cn/api/v4/srm/asn/list/'

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
            redis_prefix="shopee:stockin",
            job='shopee_parcel_tracer',
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

    def get_last_month_date_range(self):
        """
        获取当前时间往前一个月的时间范围

        Returns:
            tuple: (start_date_str, end_date_str)
                   格式分别为 "YYYY-MM-DD 00:00:00" 和 "YYYY-MM-DD 23:59:59"
        """
        # 获取当前时间
        now = datetime.now()

        # 计算一个月前的日期
        one_month_ago = now - relativedelta(months=1)

        # 格式化开始日期（一个月前的当天，从00:00:00开始）
        start_date_str = one_month_ago.strftime("%Y-%m-%d 00:00:00")

        # 格式化结束日期（当前日期，到23:59:59结束）
        end_date_str = now.strftime("%Y-%m-%d 23:59:59")

        return start_date_str, end_date_str

    async def fetch(self, page):
        start_date_str, end_date_str=self.get_last_month_date_range()
        json_data = {
            'page_no': page,
            'count': 100,
            'sku_name': '',
            "actual_delivery_date_str_left":start_date_str,
            "actual_delivery_date_str_right":end_date_str,
            'purchase_reason_list': [],
            'park_name': '',
            'whs_id_list': '',
            'asn_create_date': [],
            'expected_delivery_time': [],
            'actual_delivery_time': [],
            'logistics_state_list': [],
            'logistics_anomalies': [],
            'asn_anomalies': [],
            'ordering_list_str': '',
            'pageSize': 100,
            'currentPage': page,
            'asn_tab_type': 0,
            'status_list': [
                4,
            ],
        }

        data=await self.post(self.URL, json_data)

        return data
    def parse(self,json_data):
        items=[]
        try:
            for i in json_data['data']['asn_list']:
                item={
                    "数据爬取日期": int(time.time() * 1000),
                    "店铺": self.shop_name,
                    "入库ID":i['inbound_id'],
                    "送货数":i['shipping_qty'],
                    "入库数":i['inbound_qty'],
                    "实际入库时间":i['actual_inbound_date']
                }
                # print(item)
                items.append(item)

                # if int(item['送货数'])-int(item['入库数'])>=10:
                #     single_row_data = [datetime.now().strftime('%Y-%m-%d'), self.shop_name, str(int(item['送货数'])-int(item['入库数'])), item["入库ID"]]
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
                if 'data' not in data:
                    self.logger.error(f"第 {page} 页没有data字段")
                    break

                result = data['data']

                # 检查是否有列表数据
                if 'asn_list' not in result:
                    self.logger.error(f"第 {page} 页没有asn_list字段")
                    break

                current_items = result['asn_list']

                # 如果当前页没有数据，则结束循环
                if not current_items:
                    self.logger.info(f"第 {page} 页没有数据，停止获取")
                    break

                items=self.parse(data)

                # ==============保存异常数据=============
                # Redis 去重
                new_items = self.storage.filter_new_items(items)
                # 判断是否异常
                abnormal = self.storage.detect_abnormal(new_items)

                # 将去重
                all_items.extend(abnormal)

                # 批量入库
                self.storage.batch_insert(abnormal)

                # 异常报警
                self.storage.alarm_abnormal(abnormal, self.logger)

                if len(items) < 100:
                    self.logger.info(f"当前页数据并且数据为当月产生的数据不足200条，可能是最后一页，停止获取")
                    break

                page += 1

            except Exception as e:
                print(e)

        self.logger.info(f'总共有{len(all_items)}条异常数据')

        return all_items

async def main():
    shop_name_list = ["虾皮全托1501店", "虾皮全托507-lxz","虾皮全托506-kedi", "虾皮全托505-qipei","虾皮全托504-huanchuang","虾皮全托503-juyule","虾皮全托502-xiyue","虾皮全托501-quzhi"]
    # shop_name_list=["虾皮全托501-quzhi"]
    for shop_name in shop_name_list:
        s = StockinManager(shop_name, 'shopee_parcel_tracer')
        s.logger.info(f'开始爬取店铺{shop_name}的数据')
        await s.fetch_all_pages()


# if __name__ == "__main__":
#     asyncio.run(main())