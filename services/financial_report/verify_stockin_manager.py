import time
import asyncio
from datetime import datetime
from utils.dingding_collector import DingTalkDataQuery
from utils.load_sku_mapping import load_cost_mapping, get_cost_price
from core.base_client import TemuBaseClient
from utils.dingding_doc import DingTalkTokenManager, upload_multiple_records, test_delete_records, DingTalkSheetQuery


class VerifyStockinManager(TemuBaseClient):
    URL = 'https://agentseller.temu.com/mms/venom/api/supplier/purchase/manager/querySubOrderList'

    def __init__(self, shop_name, logger,month):
        super().__init__(shop_name, logger)
        self.verify_config = {
            "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
            "sheet_id": "NpGmkWQ",
            "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
        }
        self.shop_name = shop_name
        self.month = month

    def prepare_stockin_table_data(self, stockin_items):
        """构造钉钉表数据（仅保留有差异的记录）"""
        records = []
        for item in stockin_items:
            deliver_qty = int(item.get("送货数", 0))
            receive_qty = int(item.get("入库数", 0))

            if deliver_qty == receive_qty:
                continue

            records.append({
                "数据爬取日期": item['数据抓取时间'],
                "平台": "temu",
                "店铺": item.get("店铺", ""),
                "货号": item.get('货号', ''),
                "备货单号": item.get("备货单号", ""),
                "送货数量": deliver_qty,
                "入库数量": receive_qty,
                "交接时间": item.get("交接时间", ""),
                "收货时间": item.get("收货时间", ""),
                "单价": item.get("单价", ""),
            })
        return records

    async def up_stockin_data(self, table_data):
        """上传入库情况数据"""
        if not table_data:
            self.logger.info(f'店铺 {self.shop_name} 无差异数据，跳过上传')
            return

        self.logger.info(f'开始上传 {len(table_data)} 条差异数据到钉钉')
        upload_multiple_records(self.verify_config, table_data, self.logger)

    async def fetch_info(self, order_id):
        """获取备货单详情"""
        json_data = {
            'pageNo': 1,
            'pageSize': 20,
            'urgencyType': 0,
            'isCustomGoods': False,
            'statusList': [7],
            'oneDimensionSort': {
                'firstOrderByParam': 'statusFinishTime',
                'firstOrderByDesc': 1,
            },
            'subPurchaseOrderSnList': [order_id],
        }

        try:
            data = await self.post(self.URL, payload=json_data)
            return data
        except Exception as e:
            self.logger.error(f'获取备货单 {order_id} 信息失败: {e}')
            return None

    async def parse(self, data, order_id):
        """解析订单数据"""
        items = []
        try:
            order_list = data.get('result', {}).get('subOrderForSupplierList', [])
            if not order_list:
                self.logger.warning(f'备货单 {order_id} 无数据')
                return items

            current_time_ms = int(time.time() * 1000)
            single_sku_cost, combined_sku_cost = load_cost_mapping(use_cache=True)

            for order in order_list:
                sku_list=order.get('skuQuantityDetailList', [])
                for j in sku_list:


                    if int(j.get('deliverQuantity',0))!=int(j.get('realReceiveAuthenticQuantity',0)):
                        sku = j.get("extCode", "")
                        cost = get_cost_price(sku, single_sku_cost, combined_sku_cost)
                        items.append({
                            "数据抓取时间": current_time_ms,
                            "店铺": self.shop_name,
                            "备货单号": order.get('subPurchaseOrderSn', ''),
                            "货号": sku,
                            "备货单创建时间": order.get('purchaseTime', ''),
                            "送货数": j.get('deliverQuantity',0),
                            "入库数": j.get('realReceiveAuthenticQuantity',0),
                            "交接时间": order.get('deliverInfo', {}).get('deliverTime', ''),
                            "收货时间": order.get('deliverInfo', {}).get('receiveTime', ''),
                            "单价": cost
                        })
        except Exception as e:
            self.logger.error(f'解析备货单 {order_id} 数据失败: {e}')

        return items

    def read_info(self):
        """读取钉钉中待处理的备货单号"""
        try:
            query = DingTalkDataQuery(self.month)
            shop_data = query.filter_by_shop(self.shop_name)
            return set(list(shop_data['备货单号']))
        except Exception as e:
            self.logger.error(f'读取钉钉备货单号失败: {e}')
            return []

    async def run(self):
        """主执行流程"""
        self.logger.info(f'========== 开始处理店铺 {self.shop_name} 的丢件成本 ==========')

        # 获取待处理备货单号
        id_list = self.read_info()
        if not id_list:
            self.logger.info(f'店铺 {self.shop_name} 无待处理备货单')
            return 0

        self.logger.info(f'共获取 {len(id_list)} 个备货单号')

        # 获取所有订单详情
        all_items = []
        for order_id in id_list:
            data = await self.fetch_info(order_id)
            if data:
                items = await self.parse(data, order_id)
                all_items.extend(items)

        if not all_items:
            self.logger.info(f'店铺 {self.shop_name} 无有效入库数据')
            return 0

        # 计算总差异金额
        total_diff_value = 0
        for item in all_items:
            deliver_qty = int(item.get("送货数", 0))
            receive_qty = int(item.get("入库数", 0))
            price = float(item.get("单价", 0) or 0)
            total_diff_value += (deliver_qty - receive_qty) * price

        self.logger.info(f'店铺 {self.shop_name} 丢件总价：{total_diff_value:.2f}')

        # 上传差异数据
        records = self.prepare_stockin_table_data(all_items)
        await self.up_stockin_data(records)

        return total_diff_value


if __name__ == '__main__':
    # 验证4月份数据
    manager = VerifyStockinManager("103-Temu全托管", 'temu_parcel_tracer',"04")
    asyncio.run(manager.run())