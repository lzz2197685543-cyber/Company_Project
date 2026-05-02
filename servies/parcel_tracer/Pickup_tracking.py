import time
from core.base_client import ShopeeBaseClient
from servies.parcel_tracer.sql_save import DeliveryNoteStorage
import asyncio
from datetime import datetime, timedelta
import re
from typing import List, Dict, Any
from utils.dingding_table import DingTalkDocClient,DingTalkTokenManager


class PickupTrace(ShopeeBaseClient):

    def __init__(self, shop_name, job):
        super().__init__(shop_name, job)
        self.url = 'https://seller.scs.shopee.cn/api/v4/srm/asn/list/'

        self.storage = DeliveryNoteStorage(
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
            redis_prefix="shopee:delivery",
            job=job,
        )

        self.storage.create_table()

        # 添加揽收空包丢件表配置
        # self.PICKUP_BIG_DIFF_SHEET_CONFIG = {
        #     "workbook_id": "kDnRL6jAJMO3D450HBM0ogPDWyMoPYe1",  # 你的表格ID
        #     "sheet_id": "st-514b97fa-74330",  # 工作表ID
        #     "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
        # }
        #
        # self.token_manager = DingTalkTokenManager()

        # 创建客户端实例
        # self.client = DingTalkDocClient(self.token_manager)

    async def fetch(self, page):
        items = []
        json_data = {
            'page_no': page,
            'count': 100,
            'status_list': [],
            'sku_name': '',
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
            'pageSize': 20,
            'currentPage': 1,
            'asn_tab_type': 1,
        }

        res_json_data = await self.post(self.url, json_data)

        for i in res_json_data['data']['asn_list']:
            shipping_batch_id = i['shipping_batch_id']
            request_id = await self.get_shipping_order_id(shipping_batch_id)
            traces = await self.get_traces(request_id)

            item = {
                "数据抓取日期": int(time.time() * 1000),
                "店铺": self.shop_name,
                "入库ID": i['inbound_id'],
                "到货数量": i['inbound_qty'],
                "物流轨迹": traces,
                "标记状态": '正常',
                "标记原因": ''
            }
            # print(item)

            # 对每个item进行空包/丢件判断
            item = self.check_single_item(item)
            items.append(item)

            if '空包' in item['标记状态']:
                single_row_data = [datetime.now().strftime('%Y-%m-%d'), self.shop_name,
                                   "1", item["入库ID"]]
                self.client.insert_data_at_empty_row(
                    self.PICKUP_BIG_DIFF_SHEET_CONFIG["workbook_id"],
                    self.PICKUP_BIG_DIFF_SHEET_CONFIG["sheet_id"],
                    self.PICKUP_BIG_DIFF_SHEET_CONFIG["operator_id"],
                    single_row_data
                )

        return items

    def check_single_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        检查单个订单是否需要标记为空包/丢件

        判断逻辑：
        1. 物流轨迹包含"已签收"或"已代收"
        2. 到货数量为0
        3. 当前日期超过签收时间3天
        """
        try:
            # 获取物流轨迹
            traces = item.get("物流轨迹", [])

            # 检查物流轨迹是否包含签收
            has_sign = False
            sign_time = None
            sign_trace = None

            if traces and isinstance(traces, list):
                # 遍历轨迹列表查找签收记录
                for trace in traces:
                    if "已签收" in trace or "已代收" in trace:
                        has_sign = True
                        sign_trace = trace
                        # 提取签收时间
                        sign_time = self.extract_sign_time(trace)
                        break

            # 判断条件
            to_qty = item.get("到货数量", 0)

            if has_sign and to_qty == 0:
                # 如果有签收时间，检查是否超过3天
                if sign_time:
                    current_time = datetime.now()
                    days_diff = (current_time - sign_time).days
                    if days_diff >= 3:
                        item["标记状态"] = "空包/丢件"
                        item[
                            "标记原因"] = f"物流已签收但到货数量为0，签收时间：{sign_time.strftime('%Y-%m-%d %H:%M:%S')}，已超过{days_diff}天"
                        self.logger.info(
                            f"标记空包/丢件：入库ID {item['入库ID']}，签收时间 {sign_time}，已过{days_diff}天")
                    else:
                        self.logger.debug(
                            f"未标记：入库ID {item['入库ID']}，签收时间 {sign_time}，仅过去{days_diff}天，未满3天")
                else:
                    # 如果无法获取签收时间，但满足其他条件，也标记为可疑
                    item["标记状态"] = "可疑空包"
                    item["标记原因"] = "物流已签收但到货数量为0，无法确定签收时间"
                    self.logger.info(f"标记可疑空包：入库ID {item['入库ID']}，无法确定签收时间")
            elif has_sign and to_qty > 0:
                self.logger.debug(f"正常订单：入库ID {item['入库ID']}，已签收且到货数量为{to_qty}")

            return item

        except Exception as e:
            self.logger.error(f"检查订单标记时发生错误，入库ID: {item.get('入库ID')}, 错误: {e}")
            return item

    def extract_sign_time(self, trace_str: str) -> datetime | None:
        """
        从物流轨迹字符串中提取签收时间
        适配格式："2026-02-27 08:30:48 您的快件已签收..."
        """
        try:
            # 匹配开头的日期时间格式
            pattern = r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})'
            match = re.search(pattern, trace_str)

            if match:
                time_str = match.group(1)
                sign_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                return sign_time

            return None

        except Exception as e:
            self.logger.error(f"提取签收时间失败: {e}, 轨迹: {trace_str[:100]}")
            return None

    async def get_shipping_order_id(self, shipping_batch_id):
        params = {
            'shipping_batch_id': shipping_batch_id,
        }
        try:
            resp_data = await self.get(
                'https://seller.scs.shopee.cn/api/v4/srm/asn/shipping_batch/detail',
                payload=params
            )
            shipping_order_id = resp_data['data']["shipping_order_list"][0]["shipping_order_id"]

            return shipping_order_id
        except Exception as e:
            self.logger.info(f'获取shipping_order_id失败, shipping_batch_id: {shipping_batch_id}, 错误: {e}')
            return None

    async def get_traces(self, request_id):
        """
        获取指定订单的物流轨迹
        Args:
            request_id: 订单ID
        Returns:
            list: 格式化后的物流轨迹列表，按时间排序
        """
        if not request_id:
            return []

        params = {
            'request_id': request_id,
        }

        try:
            # 发送请求获取物流轨迹数据
            res_data = await self.get(
                'https://seller.scs.shopee.cn/api/v4/srm/basis/logistics_tracking/detail/',
                payload=params,
            )

            # 检查返回数据是否为空
            if not res_data:
                self.logger.warning(f"获取物流轨迹失败: 返回数据为空, request_id: {request_id}")
                return []

            # 检查是否包含data字段
            if 'data' not in res_data:
                self.logger.warning(f"获取物流轨迹失败: 返回数据中没有data字段, request_id: {request_id}")
                return []

            # 获取detail_list并检查是否为None
            detail_list = res_data['data'].get('detail_list')

            # 如果detail_list为None，说明没有轨迹数据
            if detail_list is None:
                self.logger.info(f"订单 {request_id} 暂无物流轨迹数据")
                return []

            # 确保detail_list是列表类型
            if not isinstance(detail_list, list):
                self.logger.warning(f"订单 {request_id} 的detail_list不是列表类型: {type(detail_list)}")
                return []

            # 处理每条轨迹数据
            traces = []
            for trace in detail_list:
                # 确保每条轨迹数据是字典类型
                if not isinstance(trace, dict):
                    continue

                # 获取物流时间和内容
                logistics_time = trace.get('logistics_time', 0)
                logistics_context = trace.get('logistics_context', '')

                # 格式化时间戳
                try:
                    if logistics_time and int(logistics_time) > 0:
                        time_str = datetime.fromtimestamp(int(logistics_time)).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        time_str = '未知时间'
                except (ValueError, TypeError, OverflowError):
                    self.logger.warning(f"时间戳格式错误: {logistics_time}, request_id: {request_id}")
                    time_str = '时间格式错误'

                # 构建轨迹信息
                trace_info = f"{time_str} {logistics_context}".strip()

                # 只添加非空轨迹
                if trace_info and trace_info != '未知时间 ' and trace_info != '时间格式错误 ':
                    traces.append(trace_info)

            # 按时间正序排序
            traces.sort()

            self.logger.info(f"订单 {request_id} 获取到 {len(traces)} 条物流轨迹")
            return traces

        except Exception as e:
            self.logger.error(
                f"获取物流轨迹时发生异常, request_id: {request_id}, 错误类型: {type(e).__name__}, 错误信息: {e}")
            return []

    async def fetch_all_pages(self):
        """
        处理店铺的所有数据，包括分页获取
        """
        all_items=[]
        page=1

        while True:
            self.logger.info(f'正在爬取店铺 {self.shop_name} 第 {page} 页数据')

            try:
                items = await self.fetch(page)

                if not items:
                    self.logger.info(f'店铺 {self.shop_name} 第 {page} 页没有数据')
                    break

                # ==============保存异常数据=============
                # Redis 去重
                new_items = self.storage.filter_new_items(items)
                all_items.extend(new_items)

                # 批量入库
                self.storage.batch_insert(new_items)

                # 异常报警
                abnormal = self.storage.detect_abnormal(new_items)

                self.logger.info(abnormal)


                if len(items)<100:
                    self.logger.info('没有下一页了')
                    break

                page += 1

                # 添加短暂延迟，避免请求过快
                await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f'处理店铺 {self.shop_name} 第 {page} 页数据时出错: {e}')
                break
        self.logger.info(f"总共获取到 {len(all_items)} 条数据")

        return all_items


async def main():
    shop_name_list = [
        "虾皮全托1501店",
        "虾皮全托507-lxz",
        "虾皮全托506-kedi",
        "虾皮全托505-qipei",
        "虾皮全托504-huanchuang",
        "虾皮全托503-juyule",
        "虾皮全托502-xiyue",
        "虾皮全托501-quzhi"
    ]
    # shop_name_list=["虾皮全托501-quzhi"]

    for shop_name in shop_name_list:
        try:
            s = PickupTrace(shop_name, 'shopee_parcel_tracer')
            s.logger.info(f'开始爬取店铺{shop_name}的数据')
            await s.fetch_all_pages()
            s.logger.info(f'店铺{shop_name}数据爬取完成')
        except Exception as e:
            print(f"处理店铺 {shop_name} 时出错: {e}")
            continue

#
# if __name__ == "__main__":
#     asyncio.run(main())