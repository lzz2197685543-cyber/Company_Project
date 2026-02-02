import time

from core.base_client import SheinBaseClient
from servies.parcel_tracer.sql_save import DeliveryNoteStorage
import asyncio
from datetime import datetime, timedelta
import re

class PickupTrace(SheinBaseClient):

    def __init__(self, shop_name, job):
        super().__init__(shop_name, job)
        self._trace_cache = {}  # 实例级别的缓存
        self.storage = DeliveryNoteStorage(
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
            redis_prefix="shein:delivery",
            job='shein_parcel_tracer',
        )

        self.storage.create_table()

    async def fetch(self,page):
        """主流程"""
        items=[]
        for status in range(3, 5):
            orders_resp = await self.post('https://sso.geiwohuo.com/pfmp/order/list', {
                'orderType': 2,
                'status': [status],
                'page': page,
                # 'allocateTimeEnd': '2026-01-29 23:59:59',
                # 'allocateTimeStart': '2025-10-29 00:00:00',
                'perPage': 200,
            })

            for order in orders_resp.get('info', {}).get('data', []):
                traces,logisticsStatusName = await self.get_logistics_info(order['deliveryNo'])

                # 判断物流状态
                mark_status, mark_reason = self._check_logistics_status(
                    order_status=order['stateName'] + " " + logisticsStatusName,
                    delivery_time=order.get('deliveryTime'),
                    receipt_time=order.get('receiptTime'),
                    traces=traces,
                    shop_name=self.shop_name,
                    order_no=order['sellerOrderNo']
                )

                item = {
                    "数据抓取日期":int(time.time()*1000),
                    "店铺": self.shop_name,
                    "订单号": order['sellerOrderNo'],
                    "订单状态": order['stateName']+" "+logisticsStatusName,
                    "发货时间": order['deliveryTime'],
                    "收货时间": order['receiptTime'],
                    "物流轨迹": traces,
                    "标记状态": mark_status,
                    "标记原因": mark_reason if mark_reason else "无",
                }

                items.append(item)
        return items

    async def fetch_all_pages(self):
        """获取所有页面的数据"""
        all_items=[]
        page=1
        while True:
            try:
                self.logger.info(f'正在获取第{page}页数据...')
                items=await self.fetch(page)


                # 如果当前页没有数据，则结束循环
                if not items:
                    self.logger.info(f"第 {page} 页没有数据，停止获取")
                    break


                # ==============保存异常数据=============
                # Redis 去重
                new_items=self.storage.filter_new_items(items)
                all_items.extend(new_items)

                # 批量入库
                self.storage.batch_insert(new_items)

                # 异常报警
                abnormal = self.storage.detect_abnormal(new_items)

                self.logger.info(abnormal)

                if len(items)<200:
                    self.logger.info('没有下一页了')
                    break

                page += 1

                # 添加短暂延迟，避免请求过快
                await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"获取第 {page} 页数据时发生异常: {e}")
                break

        self.logger.info(f"总共获取到 {len(all_items)} 条数据")
        return all_items

    def _parse_datetime_from_trace(self, trace_str):
        """从物流轨迹字符串中提取日期时间"""
        if not isinstance(trace_str, str) or not trace_str:
            return None

        # 尝试提取日期时间，支持多种格式
        patterns = [
            # 格式: 2026-01-28 09:06
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2})',
            # 格式: 2026-01-28 09:06:00
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})',
            # 格式: 2026年01月28日22:41:13
            r'(\d{4}年\d{2}月\d{2}日\d{2}:\d{2}:\d{2})',
        ]

        for pattern in patterns:
            match = re.search(pattern, trace_str)
            if match:
                date_str = match.group(1)
                try:
                    # 根据不同格式解析
                    if '年' in date_str:
                        # 格式: 2026年01月28日22:41:13
                        return datetime.strptime(date_str, '%Y年%m月%d日%H:%M:%S')
                    elif len(date_str) == 16:
                        # 格式: 2026-01-28 09:06
                        return datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                    else:
                        # 格式: 2026-01-28 09:06:00
                        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    continue

        return None

    def _parse_order_datetime(self, time_str):
        """解析订单时间（发货时间、收货时间）"""
        if not time_str:
            return None

        try:
            # 尝试解析完整格式
            if len(time_str) >= 19:
                return datetime.strptime(time_str[:19], "%Y-%m-%d %H:%M:%S")
            # 尝试解析不含秒的格式
            elif len(time_str) >= 16:
                return datetime.strptime(time_str[:16], "%Y-%m-%d %H:%M")
            else:
                return None
        except Exception:
            return None

    def _check_logistics_status(self, order_status, delivery_time, receipt_time, traces, shop_name, order_no):
        """
        判断物流状态是否正常

        判断标准：
        1. 若 [订单状态] 为"已送货 待取货"且当前日期超过 [发货时间] 3天后的，则标记为"异常"
        2. 若 [物流轨迹] 包含"已签收"“已代收”，且 [订单状态] 为"已收货"，且当前日期超过收货时间2天后的，则标记为"空包/丢件"
        3. 若 [订单状态] 为"已送货"，且当前日期距离 [物流轨迹] 中提取的最新日期超过3天或空值，则标记为"丢件"

        返回: (标记状态, 标记原因)
        """
        current_time = datetime.now()
        status = "正常"
        reason = ""

        try:
            # 解析订单时间
            delivery_dt = self._parse_order_datetime(delivery_time)
            receipt_dt = self._parse_order_datetime(receipt_time)

            # 判断标准1: "已送货 待取货"超过发货时间3天
            if "已送货 待取货" in order_status and delivery_dt:
                if current_time > delivery_dt + timedelta(days=3):
                    status = "异常"
                    reason = f"已送货待取货超过3天（发货时间：{delivery_time}）"
                    return status, reason

            # 判断标准2: 轨迹包含"已签收"或"已代收"，订单状态为"已收货"，超过收货时间2天
            if "已收货" in order_status and receipt_dt:
                # 检查物流轨迹是否包含签收信息
                has_signature = False
                for trace in traces:
                    if isinstance(trace, str) and ("已签收" in trace or "已代收" in trace):
                        has_signature = True
                        break

                if has_signature:
                    if current_time > receipt_dt + timedelta(days=2):
                        status = "空包/丢件"
                        reason = f"已签收但超过收货时间2天（收货时间：{receipt_time}）"
                        return status, reason

            # 判断标准3: 订单状态为"已送货"，检查最新物流轨迹日期
            if "已送货" in order_status and traces:
                latest_trace_date = None

                # 从物流轨迹中提取最新日期
                for trace in traces:
                    trace_dt = self._parse_datetime_from_trace(trace)
                    if trace_dt:
                        if latest_trace_date is None or trace_dt > latest_trace_date:
                            latest_trace_date = trace_dt

                if latest_trace_date is None:
                    # 尝试检查轨迹是否完全为空
                    if not traces:
                        status = "丢件"
                        reason = "无物流轨迹信息"
                    else:
                        # 有轨迹但无法解析日期，可能是正常情况
                        status = "正常"
                        reason = "有物流轨迹但日期格式特殊"
                elif current_time > latest_trace_date + timedelta(days=3):
                    status = "丢件"
                    reason = f"最新物流轨迹超过3天（最新轨迹时间：{latest_trace_date.strftime('%Y-%m-%d %H:%M:%S')}）"
                else:
                    # 物流轨迹在3天内，正常
                    status = "正常"
                    reason = f"物流轨迹正常（最新轨迹：{latest_trace_date.strftime('%Y-%m-%d %H:%M:%S')}）"

        except Exception as e:
            print(f"店铺 {shop_name} 订单 {order_no} 物流状态判断出错: {e}")
            status = "判断异常"
            reason = f"判断过程中发生错误: {str(e)}"

        return status, reason

    async def get_trace(self, company_code, tracking_number):
        """获取物流轨迹（带缓存）"""
        key = (company_code, tracking_number)
        if key in self._trace_cache:
            return self._trace_cache[key]

        try:
            res = await self.post('https://sso.geiwohuo.com/pfmp/delivery/expressRoute', {
                'logisticsCompanyCode': company_code,
                'trackingNumber': tracking_number,
            })

            traces = [
                f"{item.get('routTime', '')} {item.get('desc', '')}"
                for item in res.get('info', {}).get('waybillRoutDetailList', [])
            ]
            traces.sort(reverse=False)
            logisticsStatusName =res['info']['logisticsStatusName']
        except Exception:
            traces = []

        self._trace_cache[key] = traces
        return traces,logisticsStatusName

    async def get_logistics_info(self, delivery_no):
        """获取物流信息"""
        res = await self.post('https://sso.geiwohuo.com/pfmp/delivery/logisticsInfo',
                              {'sellerDeliveryNoList': [delivery_no]})

        info = res.get('info', {})
        for item in info.get('list', []):
            logistics_details = item.get('logisticsDetails') or [{}]
            detail = logistics_details[0]

            company_code = detail.get('expressCompanyCode')
            tracking_number = detail.get('expressCode')

            if company_code and tracking_number:
                return await self.get_trace(company_code, tracking_number)

        return []



async def main():
    shop_name_list=["希音全托301-yijia", "希音全托302-juyule", "希音全托303-kedi", "希音全托304-xiyue"]
    for shop_name in shop_name_list:
        s = PickupTrace(shop_name, 'shein_parcel_tracer')
        s.logger.info(f'开始爬取店铺{shop_name}的数据')
        await s.fetch_all_pages()


# if __name__ == "__main__":
#     asyncio.run(main())