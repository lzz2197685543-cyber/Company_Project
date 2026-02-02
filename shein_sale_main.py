from servies.sale.shein_sale_data import  Shein_Sale
from utils.dingding_doc import DingTalkSheetDeleter,DingTalkSheetUploader,DingTalkTokenManager
import pandas as pd
from datetime import datetime
from pathlib import Path
from utils.logger import get_logger
import asyncio
import time
from utils.dingtalk_bot import ding_bot_send

"""跑Shein销售数据"""

logger = get_logger("shein_sale_data")

def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"

def upload_multiple_records(config,records):
    """
    批量上传多条记录的完整示例
    """
    # 配置参数（请替换为实际值）


    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建上传器（不再需要手动传入access_token）
    uploader = DingTalkSheetUploader(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    print(f"准备上传 {len(records)} 条记录...")

    # 批量上传，每批50条，批次间延迟0.2秒，失败时重试2次
    results = uploader.upload_batch_records(records, batch_size=50, delay=0.2, max_retries=2)

    # 分析结果
    successful_batches = [r for r in results if r.get("success")]
    failed_batches = [r for r in results if not r.get("success")]

    print(f"\n上传统计:")
    print(f"总批次: {len(results)}")
    print(f"成功批次: {len(successful_batches)}")
    print(f"失败批次: {len(failed_batches)}")

    if failed_batches:
        print(f"\n失败详情:")
        for i, failed in enumerate(failed_batches):
            print(f"  批次 {i + 1}: {failed.get('message', '未知错误')}")

    return results

def build_records():
    # 获取当前年月日，格式为 YYYYMMDD
    current_date = datetime.now().strftime("%Y%m%d")
    out_dir = Path(__file__).resolve().parent / "data" / "sale"
    filename = f"{out_dir}/shein_sale_{current_date}.csv"
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
            "商品名称": row['商品名称'],
            "抓取数据日期": row['抓取数据日期'],
            "今日销量": row['今日销量'],
            "近7天销量": row['近7天销量'],
            "近30天销量": row['近30天销量'],
            "平台库存": row['平台库存'],
            "平台": row['平台'],
            "在途库存": row['在途库存'],
            "sku": str(row['sku']) if not pd.isna(row['sku']) else "",
            "店铺": row['店铺'],
        }
        records.append(record)

    return records


async def main():
    total_start = time.perf_counter()
    logger.info('程序开始启动')
    logger.info(f'--------------------------------开始爬取数据------------------------------------')
    name_list = ["希音全托301-yijia", "希音全托302-juyule", "希音全托303-kedi", "希音全托304-xiyue"]
    for shop_name in name_list:
        logger.info(f'---------------------------------开始爬取{shop_name}数据-----------------------------------')
        shein = Shein_Sale(shop_name)
        await shein.get_all_page()

    config = {
        "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",  # 文档ID
        "sheet_id": "销量与库存-日更",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"  # 操作人ID
    }

    logger.info('---------------------------------开始匹配sku数据-----------------------------------')
    records = build_records()
    # print(records)

    logger.info('---------------------------------开始上传数据-----------------------------------')
    upload_multiple_records(config, records)
    logger.info('数据上传成功')

    ding_bot_send('me', 'Shein的销售任务完成')
    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")


if __name__ == '__main__':
    asyncio.run(main())