import time

from core.base_client import ShopeeBaseClient
from servies.parcel_tracer.sql_save import DeliveryNoteStoragel
import asyncio
from datetime import datetime, timedelta
import re
from typing import List, Dict, Any
from utils.dingding_table import DingTalkTokenManager,DingTalkDocClient
from utils.dingtalk_bot import ding_bot_send


class PickupTracel(ShopeeBaseClient):
    def __init__(self, shop_name, job):
        super().__init__(shop_name, job)

        self.storage = DeliveryNoteStoragel(
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
            redis_prefix="shopee:delivery12",
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
        #
        # # 创建客户端实例
        # self.client = DingTalkDocClient(self.token_manager)

    async def fetch(self, page):
        items = []
        for transit_type in [1, 2]:
            json_data = {
                'page_no': page,
                'count': 100,
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
                # 'is_asc': 0,
                'in_transit_tab_type': transit_type,  # 1是“待揽收”  2是“已揽收"
                'asn_tab_type': 0,  # 0是运输中，1是收货中
                # 'status_list': [
                #     1,
                # ],
            }

            res_json_data = await self.post('https://seller.scs.shopee.cn/api/v4/srm/asn/shipping_batch/list/',
                                            json_data)

            # 添加空值检查
            shipping_batch_list = res_json_data.get('data', {}).get('shipping_batch_list')

            if not shipping_batch_list:
                if transit_type == 1:
                    self.logger.info(f'店铺{self.shop_name}没有待揽收的数据')
                if transit_type == 2:
                    self.logger.info(f'店铺{self.shop_name}没有已揽收的数据')
                continue

            for i in shipping_batch_list:
                shipping_order_id = i['shipping_order_info']['shipping_order_id']
                traces = await self.get_traces(shipping_order_id)

                # 订单状态转换
                asn_status = i['asn_list'][0]['asn_status']
                asn_status_text = self.convert_asn_status(asn_status)

                # 物流状态转换
                logistics_state = i['shipping_order_info']['logistics_state']
                logistics_state_text = self.convert_logistics_state(logistics_state)

                item = {
                    "数据抓取日期": int(time.time() * 1000),
                    "店铺": self.shop_name,
                    "入库ID": i['asn_list'][0]['inbound_id'],
                    "创建时间": datetime.fromtimestamp(i['shipping_batch_info']['creation_time']).strftime(
                        '%Y-%m-%d %H:%M:%S'),
                    "订单状态": asn_status_text,  # 转换为文字
                    "物流状态": logistics_state_text,  # 转换为文字
                    "物流轨迹": traces,
                    "标记状态": '正常',
                    '标记原因': '',
                }

                item=self.check_single_item(item)

                # if '丢件' in item['标记状态']:
                #     single_row_data = [datetime.now().strftime('%Y-%m-%d'), self.shop_name,
                #                        "1", item["入库ID"]]
                #     self.client.insert_data_at_empty_row(
                #         self.PICKUP_BIG_DIFF_SHEET_CONFIG["workbook_id"],
                #         self.PICKUP_BIG_DIFF_SHEET_CONFIG["sheet_id"],
                #         self.PICKUP_BIG_DIFF_SHEET_CONFIG["operator_id"],
                #         single_row_data
                #     )

                if '未知' in item['标记状态']:
                    ding_bot_send('me',f'shopee:{item}')

                items.append(item)

    # 添加状态转换方法
    def convert_asn_status(self, status):
        """转换订单状态"""
        status_map = {
            1: '运送中',  # 从注释中已知
        }
        return status_map.get(status, f'未知({status})')

    def convert_logistics_state(self, state):
        """转换物流状态"""
        state_map = {
            3: '已签收',  # 从注释中已知
            5: '派送中',  # 从注释中已知
        }
        return state_map.get(state, f'未知({state})')

    async def get_traces(self, request_id):
        """
        获取指定订单的物流轨迹
        Args:
            request_id: 订单ID
        Returns:
            list: 格式化后的物流轨迹列表，按时间排序
        """
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
                else:
                    self.logger.debug(f"订单 {request_id} 包含空轨迹数据: {trace}")

            # 按时间正序排序
            traces.sort()

            self.logger.info(f"订单 {request_id} 获取到 {len(traces)} 条物流轨迹")
            return traces

        except Exception as e:
            self.logger.error(
                f"获取物流轨迹时发生异常, request_id: {request_id}, 错误类型: {type(e).__name__}, 错误信息: {e}")
            return []

    def check_single_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        检查单个订单是否需要标记

        判断逻辑：
        1. 订单状态为"运送中"且当前日期超过创建时间3天后，则标记为"异常"
        2. 物流状态为"已揽收"，且当前日期距离物流轨迹中提取的最新日期超过3天或空值，则标记为"丢件"
        """
        try:
            # 获取当前时间
            current_time = datetime.now()

            # 获取订单基本信息
            inbound_id = item.get('入库ID', '未知')
            order_status = item.get('订单状态', '')
            logistics_status = item.get('物流状态', '')
            traces = item.get("物流轨迹", [])
            create_time_str = item.get('创建时间', '')

            # 逻辑1：订单状态为"运送中"且当前日期超过创建时间3天后，标记为"异常"
            if order_status == "运送中" and create_time_str:
                try:
                    create_time = datetime.strptime(create_time_str, '%Y-%m-%d %H:%M:%S')
                    days_diff = (current_time - create_time).days
                    if days_diff >= 3:
                        item["标记状态"] = "异常"
                        item["标记原因"] = f"订单状态为运送中，创建时间：{create_time_str}，已超过{days_diff}天"
                        self.logger.info(f"标记异常：入库ID {inbound_id}，创建时间 {create_time_str}，已过{days_diff}天")
                        return item
                    else:
                        self.logger.debug(f"未标记：入库ID {inbound_id}，运送中仅过去{days_diff}天，未满3天")
                except Exception as e:
                    self.logger.error(f"解析创建时间失败: {e}, 入库ID: {inbound_id}")

            # 逻辑2：物流状态为"已揽收"，且当前日期距离物流轨迹中提取的最新日期超过3天或空值，标记为"丢件"
            if logistics_status == "已揽收":
                latest_time = None

                # 从物流轨迹中提取最新日期
                if traces and isinstance(traces, list):
                    for trace in traces:
                        trace_time = self.extract_sign_time(trace)
                        if trace_time:
                            if latest_time is None or trace_time > latest_time:
                                latest_time = trace_time

                if latest_time:
                    days_diff = (current_time - latest_time).days
                    if days_diff >= 3:
                        item["标记状态"] = "丢件"
                        item[
                            "标记原因"] = f"物流状态为已揽收，最新轨迹时间：{latest_time.strftime('%Y-%m-%d %H:%M:%S')}，已超过{days_diff}天无更新"
                        self.logger.info(f"标记丢件：入库ID {inbound_id}，最新轨迹时间 {latest_time}，已过{days_diff}天")
                        return item
                    else:
                        self.logger.debug(f"未标记：入库ID {inbound_id}，已揽收最新轨迹仅过去{days_diff}天，未满3天")
                else:
                    # 无轨迹记录
                    item["标记状态"] = "丢件"
                    item["标记原因"] = "物流状态为已揽收，但无有效物流轨迹记录"
                    self.logger.info(f"标记丢件：入库ID {inbound_id}，无有效物流轨迹")
                    return item

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
                trace_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                return trace_time

            return None

        except Exception as e:
            self.logger.error(f"提取时间失败: {e}, 轨迹: {trace_str[:100]}")
            return None

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
                #
                # # 批量入库
                self.storage.batch_insert(new_items)
                #
                # # 异常报警
                abnormal = self.storage.detect_abnormal(new_items)
                #
                self.logger.info(abnormal)

                if len(items) < 100:
                    self.logger.info('没有下一页了')
                    break

                page+=1

                await asyncio.sleep(1)
            except Exception as e :
                self.logger.error(f'处理店铺 {self.shop_name} 第 {page} 页数据时出错: {e}')
                break
        self.logger.info(f"总共获取到 {len(all_items)} 条数据")
        return all_items

async def main():
    shop_name_list = ["虾皮全托1501店", "虾皮全托507-lxz", "虾皮全托506-kedi", "虾皮全托505-qipei",
                      "虾皮全托504-huanchuang", "虾皮全托503-juyule", "虾皮全托502-xiyue", "虾皮全托501-quzhi"]
    # shop_name_list=["虾皮全托502-xiyue"]
    for shop_name in shop_name_list:
        s = PickupTracel(shop_name, 'shoee_parcel_tracer')
        s.logger.info(f'开始爬取店铺{shop_name}的数据')
        await s.fetch_all_pages()

#
# if __name__ == "__main__":
#     asyncio.run(main())