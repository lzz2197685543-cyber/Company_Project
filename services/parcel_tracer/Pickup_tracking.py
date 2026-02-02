

import asyncio
import time
from datetime import datetime, timedelta
from core.base_client import TemuBaseClient
import random
from services.parcel_tracer.sql_save import DeliveryNoteStorage
from utils.webchat_send import webchat_send

"""发货单--仓库待收货"""


class DeliveryNote(TemuBaseClient):


    URL='https://seller.kuajingmaihuo.com/bgSongbird-api/supplier/deliverGoods/management/pageQueryDeliveryBatch'
    TRACE_URL='https://seller.kuajingmaihuo.com/bgSongbird-api/supplier/delivery/feedback/queryAllFeedbackRecordInfo'

    def __init__(self, shop_name, logger_name):
        super().__init__(shop_name, logger_name)
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
            redis_prefix="temu:delivery"
        )

        self.storage.create_table()

    def _mark_status(self, item):
        """
        根据规则标记包裹状态
        返回: 标记结果和原因
        """
        current_date = datetime.now()
        plat_express_status_tip = item.get("包裹状态", "")
        logistics_traces = item.get("物流轨迹", [])

        # 规则1: 若[包裹状态]为"待快递揽收"且当前日期超过[预约取货时间]后1天，则标记为"异常"
        if plat_express_status_tip == "待快递揽收":
            expect_pick_up_timestamp = item.get("预约取货时间")
            if expect_pick_up_timestamp:
                try:
                    # 将时间戳（毫秒）转换为datetime对象
                    if isinstance(expect_pick_up_timestamp, (int, float)):
                        # 如果是毫秒级时间戳，先转换为秒
                        if expect_pick_up_timestamp > 1e12:  # 判断是否为毫秒级（大于1973年）
                            expect_pick_up_time = datetime.fromtimestamp(expect_pick_up_timestamp / 1000)
                        else:
                            expect_pick_up_time = datetime.fromtimestamp(expect_pick_up_timestamp)
                    elif isinstance(expect_pick_up_timestamp, str):
                        # 尝试解析字符串格式
                        try:
                            # 尝试毫秒时间戳字符串
                            expect_pick_up_time = datetime.fromtimestamp(int(expect_pick_up_timestamp) / 1000)
                        except:
                            # 尝试标准时间字符串格式
                            expect_pick_up_time = datetime.strptime(expect_pick_up_timestamp, "%Y-%m-%d %H:%M:%S")
                    else:
                        self.logger.warning(f"无法识别的预约取货时间格式: {expect_pick_up_timestamp}")
                        return "正常", ""

                    # 计算允许的最晚时间（预约时间+1天）
                    allowed_time = expect_pick_up_time + timedelta(days=1)

                    if current_date > allowed_time:
                        # 格式化时间显示
                        formatted_time = expect_pick_up_time.strftime("%Y-%m-%d %H:%M:%S")
                        return "异常", f"已超过预约取货时间({formatted_time})1天以上"

                except Exception as e:
                    self.logger.warning(f"解析预约取货时间失败: {expect_pick_up_timestamp}, 错误: {e}")

        # 规则2: 若[物流轨迹]包含"已签收"或"已代收"且[包裹状态]为"已到仓，待仓库收货"，则标记为"空包/丢件"
        if plat_express_status_tip == "已到仓，待仓库收货":
            if logistics_traces:
                # 检查轨迹中是否包含"已签收"或"已代收"
                trace_text = " ".join(logistics_traces)
                if "已签收" in trace_text or "已代收" in trace_text:
                    return "空包/丢件", "轨迹显示已签收但状态仍为待仓库收货"

        # 规则3: 若[包裹状态]为"物流运输中"，且当前日期距离[物流轨迹]中提取的最新日期超过3天或空值，则标记为"丢件"
        if plat_express_status_tip == "物流运输中":
            if not logistics_traces:
                # 没有轨迹信息
                return "丢件", "物流运输中但无轨迹信息"

            # 从轨迹中提取最新日期
            latest_trace = logistics_traces[-1] if logistics_traces else ""

            # 尝试从轨迹中提取日期时间
            # 假设轨迹格式为: "YYYY-MM-DD HH:MM:SS 信息内容"
            try:
                # 提取前19个字符作为日期时间部分
                trace_datetime_str = latest_trace[:19]
                trace_datetime = datetime.strptime(trace_datetime_str, "%Y-%m-%d %H:%M:%S")

                # 计算时间差
                time_diff = current_date - trace_datetime

                if time_diff.days > 3:
                    last_time = trace_datetime.strftime("%Y-%m-%d %H:%M:%S")
                    return "丢件", f"最新轨迹时间({last_time})已超过{time_diff.days}天"
            except (ValueError, IndexError) as e:
                # 如果解析日期失败，检查是否没有轨迹或轨迹格式异常
                self.logger.warning(f"解析轨迹时间失败: {latest_trace}, 错误: {e}")
                return "丢件", "无法解析最新轨迹时间"

        # 默认返回正常状态
        return "正常", ""

    async def fetch_page(self,page):
        payload = {
            'pageNo': page,
            'pageSize': 100,
            'status': 1,
            'productLabelCodeStyle': 0,
            'onlyTaxWarehouseWaitApply': False,
            'onlyCanceledExpress': False,
        }
        data = await self.post(self.URL, payload,cookie_domain="kuajingmaihuo")
        return data

    async def fetch_all_pages(self):
        """获取所有页面的数据"""
        all_items=[]
        page=1

        while True:
            try:
                self.logger.info(f'正在获取第{page}页数据...')
                data=await self.fetch_page(page)

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
                if 'list' not in result:
                    self.logger.error(f"第 {page} 页没有list字段")
                    break

                current_items = result['list']

                # 如果当前页没有数据，则结束循环
                if not current_items:
                    self.logger.info(f"第 {page} 页没有数据，停止获取")
                    break

                # 解析当前页数据
                parsed_items = await self._parse(data)

                # ==============保存异常数据，并且发送数据=============
                # Redis 去重
                new_items = self.storage.filter_new_items(parsed_items)
                all_items.extend(new_items)

                # 批量入库
                self.storage.batch_insert(new_items)

                # 异常报警
                abnormal = self.storage.detect_abnormal(new_items)
                self.storage.alarm_abnormal(abnormal)

                # 打印当前页信息
                self.logger.info(f"第 {page} 页获取到 {len(parsed_items)} 条数据")

                # 检查是否为最后一页
                if len(parsed_items) <= 100:
                    self.logger.info(f"已获取最后一页，停止获取")
                    break

                page += 1

                # 添加短暂延迟，避免请求过快
                await asyncio.sleep(1)

            except Exception as e:
                print(f"获取第 {page} 页数据时发生异常: {e}")
                break

            print(f"总共获取到 {len(all_items)} 条数据")
        return all_items

    async def get_tarce_text(self, detail_json_data, max_retries=5):
        """获取物流轨迹，带重试机制"""
        for attempt in range(max_retries):
            try:
                trace_res = await self.post(self.TRACE_URL, detail_json_data,cookie_domain="kuajingmaihuo")
                traces = []

                # 检查是否被限流
                if isinstance(trace_res, dict) and trace_res.get('success') is False:
                    error_msg = trace_res.get('errorMsg', '')
                    if '访问人数过多' in error_msg or '稍后重试' in error_msg:
                        if attempt < max_retries - 1:
                            self.logger.info(f"第{attempt + 1}次请求被限流，{error_msg}，等待后重试...")
                            continue  # 继续下一次尝试
                        else:
                            self.logger.info(f"请求已重试{max_retries}次仍被限流，放弃获取")
                            return []

                # 原有的轨迹解析逻辑
                if isinstance(trace_res, dict) and 'result' in trace_res:
                    result_data = trace_res.get('result', {})
                    express_trace_info = result_data.get('expressTraceInfoVO', {})
                    trace_list = express_trace_info.get('traceDetailInfoList', [])

                    for idx, i in enumerate(trace_list):
                        time_str = i['time'].replace('.000', '')
                        trace = f"{time_str} {i['info']}"
                        traces.append(trace)

                    traces.sort(reverse=False)
                    return traces
                else:
                    # 响应格式不符合预期
                    if attempt < max_retries - 1:
                        self.logger.info(f"第{attempt + 1}次响应格式异常，准备重试...")
                        continue
                    else:
                        self.logger.info(f"重试{max_retries}次后仍无法获取有效响应")
                        return []

            except Exception as e:
                self.logger.info(f"第{attempt + 1}次获取轨迹时发生异常: {e}")
                if attempt == max_retries - 1:
                    self.logger.info(f"已重试{max_retries}次，放弃获取")
                    return []

        return []

    async def _parse(self, json_data):
        items=[]
        try:
            for i in json_data["result"]["list"]:
                # 获取物流轨迹
                detail_json_data = {
                    'deliveryBatchSn': i['expressBatchSn'],
                    'expressCompanyId': 100000000006,
                    'expressDeliverySn': i['expressDeliverySn'],
                }
                traces = await self.get_tarce_text(detail_json_data, max_retries=5)
                # 最新轨迹
                latest_trace = traces[-1] if traces else ""

                delivery_method = i['deliveryMethod']
                if delivery_method == 2:
                    delivery_method_str = "在线物流下单"
                elif delivery_method == 3:
                    delivery_method_str = "自行委托第三方物流"
                else:
                    delivery_method_str = f"未知({delivery_method})"  # 兜底处理

                item = {
                    "数据抓取时间":int(time.time()*1000),
                    "店铺": self.shop_name,
                    "备货单号": i['deliveryOrderList'][0]['subPurchaseOrderSn'],
                    "包裹状态": i['platExpressStatusTip'],
                    "发货方式": delivery_method_str,  # 使用转换后的中文
                    "物流单号": i['expressCompany'] + ',' + i['expressDeliverySn'],
                    "预约取货时间": i['expectPickUpGoodsTime'],
                    "物流轨迹": traces,
                    "最新轨迹": latest_trace
                }
                # 根据规则标记状态
                status, reason = self._mark_status(item)
                item["标记状态"] = status
                if reason:
                    item["标记原因"] = reason
                # self.logger.info(item)
                items.append(item)
            return items
        except Exception as e:
            self.logger.error("解析数据时发生异常:",e)


async def main():
    s = DeliveryNote("103-Temu全托管",'temu_parcel_tracer')
    skcs = await s.fetch_all_pages()

# if __name__ == '__main__':
#     asyncio.run(main())