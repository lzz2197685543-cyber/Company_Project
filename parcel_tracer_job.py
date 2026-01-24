from services.parcel_tracer.pending_receiving_manager import DeliveryNote
from services.parcel_tracer.stockin_manager import Stockin_Manager
from utils.dingding_doc import DingTalkTokenManager, upload_multiple_records, test_delete_records
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
import asyncio
import time

logger = get_logger('temu_parcel_tracer')

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

"""Temu揽收丢件"""


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
    await up_delivery_data(table_data)

    return len(delivery_items)


async def fetch_and_upload_stockin_data(shop_name):
    """爬取并上传入库情况数据"""
    logger.info(f'----------------开始爬取店铺{shop_name}--入库的数据---------------------')
    s_stockin = Stockin_Manager(shop_name, 'temu_parcel_tracer')
    stockin_items = await s_stockin.fetch_all_pages()

    # 构造表数据
    table_data = prepare_stockin_table_data(stockin_items)

    # 立即上传数据
    await up_stockin_data(table_data)

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
        # 只保留入库数≠送货数的记录
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
    for shop_name in shop_name_list:

        # 先删除旧数据
        # await delete_data()

        # 爬取并上传发货揽收轨迹数据（完成后立即上传）
        delivery_count = await fetch_and_upload_delivery_data(shop_name)
        logger.info(f'发货揽收轨迹数据爬取完成，共{delivery_count}条记录，已上传')

        # 爬取并上传入库情况数据（完成后立即上传）
        stockin_count = await fetch_and_upload_stockin_data(shop_name)
        logger.info(f'入库情况数据爬取完成，共{stockin_count}条记录，已上传')

        logger.info('所有数据爬取和上传完成')

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")
    ding_bot_send('me', 'Temu揽收丢件任务结束')


if __name__ == "__main__":
    asyncio.run(main())