from services.parcel_tracer.Pickup_tracking import DeliveryNote
from services.parcel_tracer.stockin_manager import Stockin_Manager
from utils.dingding_doc import DingTalkTokenManager, upload_multiple_records, test_delete_records
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
from datetime import datetime
import asyncio
import time
import pymysql

logger = get_logger('temu_parcel_tracer')

db = pymysql.connect(
    host="localhost",
    user="root",
    password="1234",
    database="py_spider",
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

config_delivery = {
    "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
    "sheet_id": 'temu-发货揽收轨迹及丢件',
    "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
}

config_stockin = {
    "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
    "sheet_id": 'temu-入库情况',
    "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
}


def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"


async def delete_data():
    """删除钉钉表中的数据"""
    logger.info('----------------开始删除钉钉表中(temu-发货揽收轨迹及丢件)的数据---------------------')
    test_delete_records(config_delivery, logger)
    logger.info('----------------开始删除钉钉表中(temu-入库情况)的数据---------------------')
    test_delete_records(config_stockin, logger)


async def up_delivery_data(table_data):
    """上传发货揽收轨迹及丢件数据"""
    logger.info('----------------开始上传(temu-发货揽收轨迹及丢件)的数据---------------------')
    upload_multiple_records(config_delivery, table_data, logger)


async def up_stockin_data(table_data):
    """上传入库情况数据"""
    logger.info('----------------开始上传(temu-入库情况)的数据---------------------')
    upload_multiple_records(config_stockin, table_data, logger)


async def fetch_and_upload_delivery_data(shop_name):
    """爬取并上传发货揽收轨迹数据"""
    logger.info(f'----------------开始爬取店铺{shop_name}--仓库待收货的数据---------------------')
    s_delivery = DeliveryNote(shop_name, 'temu_parcel_tracer')
    delivery_items = await s_delivery.fetch_all_pages()

    # 构造表数据
    table_data = prepare_delivery_table_data(delivery_items)

    # 立即上传数据
    # await up_delivery_data(table_data)

    return len(delivery_items)


async def fetch_and_upload_stockin_data(shop_name):
    """爬取并上传入库情况数据"""
    logger.info(f'----------------开始爬取店铺{shop_name}--入库的数据---------------------')
    s_stockin = Stockin_Manager(shop_name, 'temu_parcel_tracer')
    stockin_items = await s_stockin.fetch_all_pages()

    # 构造表数据
    table_data = prepare_stockin_table_data(stockin_items)

    # 立即上传数据
    # await up_stockin_data(table_data)

    return len(stockin_items)


def prepare_delivery_table_data(delivery_items):
    """构造第一个钉钉表数据（揽收丢件表）"""
    records = []
    for item in delivery_items:
        record = {
            "数据抓取日期": item['数据抓取时间'],
            "店铺": item.get("店铺", ""),
            "备货单号": item.get("备货单号", ""),
            "包裹状态": item.get("包裹状态", ""),
            "发货方式": item.get("发货方式", ""),
            "物流单号": item.get("物流单号", ""),
            "预约取货时间": item.get("预约取货时间", ""),
            "物流轨迹": " | ".join(item.get("物流轨迹", [])) if item.get("物流轨迹") else "",
            "标记状态": item.get('标记状态', ''),
            "标记原因": item.get('标记原因', '')
        }
        records.append(record)
    return records


def prepare_stockin_table_data(stockin_items):
    """构造第二个钉钉表数据（入库差异表）"""
    records = []
    for item in stockin_items:
        if int(item.get("送货数", 0)) == int(item.get("入库数", 0)):
            continue

        record = {
            "数据爬取日期": item['数据抓取时间'],
            "店铺": item.get("店铺", ""),
            "备货单号": item.get("备货单号", ""),
            "送货数量": item.get("送货数", 0),
            "入库数量": item.get("入库数", 0),
            "交接时间": item.get("交接时间", ""),
            "收货时间": item.get("收货时间", "")
        }
        records.append(record)
    return records


def query_shop_abnormal_data_from_db(shop_name):
    """查询单个门店的异常数据"""
    try:
        cursor = db.cursor()

        # 查询该门店的发货异常数据
        cursor.execute("""
            SELECT * FROM `temu_delivery_note_record` 
            WHERE DATE(create_time) = CURDATE() 
            AND shop_name = %s 
            AND mark_status != '正常'
        """, (shop_name,))
        delivery_abnormals = cursor.fetchall()

        # 查询该门店的入库异常数据
        cursor.execute("""
            SELECT * FROM `temu_purchase_stock_record` 
            WHERE DATE(create_time) = CURDATE() 
            AND shop_name = %s
        """, (shop_name,))
        stockin_abnormals = cursor.fetchall()

        cursor.close()

        return delivery_abnormals, stockin_abnormals

    except Exception as e:
        logger.error(f"查询店铺 {shop_name} 异常数据失败: {e}")
        return [], []


def build_shop_abnormal_message(shop_name, delivery_abnormals, stockin_abnormals):
    """构建单个门店的异常消息"""
    message_parts = []

    # 添加标题
    title = f"🏪 **【{shop_name}】异常数据报告**\n"
    title += f"报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    message_parts.append(title)

    # 发货异常部分
    if delivery_abnormals:
        delivery_summary = f"📦 **发货异常**: {len(delivery_abnormals)}条\n"

        # 按标记状态分类
        status_stats = {}
        for row in delivery_abnormals:
            status = row['mark_status']
            status_stats[status] = status_stats.get(status, 0) + 1

        if status_stats:
            delivery_summary += "   状态分类:\n"
            for status, count in status_stats.items():
                delivery_summary += f"     • {status}: {count}条\n"

        # 详细列表（最多显示5条）
        if delivery_abnormals:
            delivery_summary += "\n   📋 异常详情:\n"
            for i, row in enumerate(delivery_abnormals[:]):
                delivery_summary += f"     {i + 1}. {row['purchase_order_sn']}\n"
                delivery_summary += f"       状态: {row['package_status']} -> {row['mark_status']}\n"
                if row['mark_reason']:
                    delivery_summary += f"       原因: {row['mark_reason'][:30]}...\n"


        message_parts.append(delivery_summary)

    # 入库异常部分
    if stockin_abnormals:
        # 过滤出入库数≠送货数的记录
        stockin_differences = []
        for row in stockin_abnormals:
            deliver_qty = int(row.get('deliver_quantity', 0))
            receive_qty = int(row.get('receive_quantity', 0))
            if deliver_qty != receive_qty:
                stockin_differences.append(row)

        if stockin_differences:
            stockin_summary = f"\n📥 **入库差异**: {len(stockin_differences)}条\n"

            # 详细列表（最多显示5条）
            stockin_summary += "   📋 差异详情:\n"
            for i, row in enumerate(stockin_differences[:]):
                deliver_qty = int(row.get('deliver_quantity', 0))
                receive_qty = int(row.get('receive_quantity', 0))
                diff = deliver_qty - receive_qty

                stockin_summary += f"     {i + 1}. {row['purchase_order_sn']}\n"
                stockin_summary += f"       送货: {deliver_qty} 入库: {receive_qty} 差异: {diff}\n"



            message_parts.append(stockin_summary)

    # 如果没有异常数据
    if not delivery_abnormals and not stockin_differences:
        message_parts.append("✅ 本次巡检未发现异常数据！")

    return "\n".join(message_parts)


def send_shop_messages(shop_data):
    """发送单个门店的消息"""
    for shop_name, delivery_count, stockin_count, message in shop_data:
        if "未发现异常数据" not in message:
            # 发送到钉钉群
            ding_bot_send('me', message)
            logger.info(f"已发送 {shop_name} 的异常消息")
            # 添加短暂延迟，避免发送过快
            time.sleep(1)


async def main():
    total_start = time.perf_counter()

    shop_name_list = [
        "2106-Temu全托管", "2105-Temu全托管", "2108-Temu全托管",
        "2107-Temu全托管", "2102-Temu全托管",
        "1108-Temu全托管", "1107-Temu全托管", "1106-Temu全托管",
        "1105-Temu全托管", "2103-Temu全托管",
        "112-Temu全托管", "151-Temu全托管家居",
        "1104-Temu全托管", "1102-Temu全托管",
        "1103-Temu全托管", "1101-Temu全托管",
        "2101-Temu全托管KA", "110-Temu全托管KA",
        "109-Temu全托管KA", "108-Temu全托管",
        "107-Temu全托管", "106-Temu全托管",
        "105-Temu全托管", "104-Temu全托管",
        "103-Temu全托管", "102-Temu全托管",
        "101-Temu全托管"
    ]

    # 1. 第一阶段：爬取所有店铺数据并保存到数据库
    logger.info("🚀 开始第一阶段：爬取所有店铺数据、保存到数据库并将数据上传到钉钉表中")
    for shop_name in shop_name_list:
        # 爬取并上传发货揽收轨迹数据
        delivery_count = await fetch_and_upload_delivery_data(shop_name)
        logger.info(f'{shop_name} 发货揽收轨迹数据爬取完成，共{delivery_count}条记录')

        # 爬取并上传入库情况数据
        stockin_count = await fetch_and_upload_stockin_data(shop_name)
        logger.info(f'{shop_name} 入库情况数据爬取完成，共{stockin_count}条记录')

        # 添加延迟避免请求过快
        await asyncio.sleep(2)

    logger.info("✅ 所有店铺数据爬取完成")

    # 2. 第二阶段：按门店查询异常数据并发送消息
    logger.info("🚀 开始第二阶段：按门店查询异常数据并发送消息")

    shop_data = []
    for shop_name in shop_name_list:
        try:
            # 查询该门店的异常数据
            delivery_abnormals, stockin_abnormals = query_shop_abnormal_data_from_db(shop_name)

            # 构建该门店的异常消息
            message = build_shop_abnormal_message(shop_name, delivery_abnormals, stockin_abnormals)

            # 统计发货异常数量
            delivery_count = len(delivery_abnormals)

            # 统计入库差异数量
            stockin_differences = []
            for row in stockin_abnormals:
                deliver_qty = int(row.get('deliver_quantity', 0))
                receive_qty = int(row.get('receive_quantity', 0))
                if deliver_qty != receive_qty:
                    stockin_differences.append(row)
            stockin_count = len(stockin_differences)

            shop_data.append((shop_name, delivery_count, stockin_count, message))

            logger.info(f"已处理 {shop_name}: 发货异常{delivery_count}条，入库差异{stockin_count}条")

        except Exception as e:
            logger.error(f"处理店铺 {shop_name} 时发生错误: {e}")
            # 即使某个店铺失败，继续处理其他店铺
            continue

    # 3. 发送所有门店的消息
    logger.info("🚀 开始第三阶段：发送各门店异常消息")
    send_shop_messages(shop_data)

    # 4. 发送总结消息
    # total_delivery_abnormal = sum(item[1] for item in shop_data)
    # total_stockin_abnormal = sum(item[2] for item in shop_data)
    # total_abnormal_shops = sum(1 for item in shop_data if item[1] > 0 or item[2] > 0)

    # summary_message = (
    #     f"🎯 Temu揽收丢件任务完成\n"
    #     f"总耗时：{format_seconds(time.perf_counter() - total_start)}\n"
    #     f"处理店铺数：{len(shop_name_list)}个\n"
    #     f"异常店铺数：{total_abnormal_shops}个\n"
    #     f"总发货异常：{total_delivery_abnormal}条\n"
    #     f"总入库差异：{total_stockin_abnormal}条\n"
    #     f"已向每个异常门店发送独立报告"
    # )
    #
    # ding_bot_send('me', summary_message)
    logger.info("总结消息已发送")
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(time.perf_counter() - total_start)}")


if __name__ == "__main__":
    asyncio.run(main())