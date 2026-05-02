# temu_sale_main.py 开头添加
import sys
from pathlib import Path
import os


# 原来的import
import time
from utils.logger import get_logger
from utils.dingding_doc import upload_multiple_records
import pandas as pd
from datetime import datetime
from services.temu_sale.temu_sale_data import Temu  # 现在应该能找到这个模块了
import asyncio
from utils.dingtalk_bot import ding_bot_send


from pathlib import Path
CONFIG_PATH = Path(__file__).resolve().parent/ "data" / "sale"


def build_records():
    # 获取当前年月日，格式为 YYYYMMDD
    current_date = datetime.now().strftime("%Y%m%d")
    filename = f"{CONFIG_PATH}/temu_sale_{current_date}.csv"
    shein_df=pd.read_csv(filename)

    records = []
    # 修复：解包 iterrows() 返回的元组
    for index, row in shein_df.iterrows():
        # 检查指定字段是否都为0
        if (row['今日销量'] == 0 and
                row['近7天销量'] == 0 and
                row['近30天销量'] == 0 and
                row['平台库存'] == 0 and
                row['在途库存'] == 0):
            continue  # 跳过这条记录

        record = {
            "平台": row['平台'],
            "商品名称": row['商品名称'],
            "抓取数据日期": row['抓取数据日期'],
            "今日销量": row['今日销量'],
            "近7天销量": row['近7天销量'],
            "近30天销量": row['近30天销量'],
            "平台库存": row['平台库存'],
            "在途库存": row['在途库存'],
            "sku": str(row['sku']) if not pd.isna(row['sku']) else "",
            "店铺": row['店铺'],
        }
        records.append(record)
    # 将记录转换为DataFrame
    filtered_df = pd.DataFrame(records)

    # 保存处理后的数据
    output_filename = f"{CONFIG_PATH}/temu_sale_filtered_{current_date}.csv"
    filtered_df.to_csv(output_filename, index=False, encoding='utf-8-sig')

    return records



async def temu_run():
    temu = Temu('temu_sale_main')
    await temu.get_all_page()


if __name__ == '__main__':

    logger = get_logger('temu_sale_main')
    logger.info('程序开始启动')

    config = {
        "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL", # 文档ID
        "sheet_id": "销量与库存-日更",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"    # 操作人ID
    }

    # 记录总时间开始
    total_start_time = time.time()

    logger.info('---------------------------------开始爬取temu数据-----------------------------------')
    asyncio.run(temu_run())

    logger.info('---------------------------------开始匹配sku数据-----------------------------------')
    records=build_records()
    # print(records)

    logger.info('---------------------------------开始上传数据-----------------------------------')
    upload_multiple_records(config, records,logger)

    logger.info('数据上传成功')


    time.sleep(3)

    # 计算总时间
    total_time = time.time() - total_start_time

    logger.info(f"⏱️  总耗时: {total_time:.2f} 秒")
    ding_bot_send('me',f'temu_sale任务完成,总耗时: {total_time:.2f} 秒')

