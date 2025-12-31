import asyncio
from modules.smt_goods import SMTGoodsSpider
from modules.smt_stock import SMTStockSpider
from utils.dingding_doc import DingTalkSheetDeleter,DingTalkSheetUploader,DingTalkTokenManager
import pandas as pd
from datetime import datetime
from pathlib import Path
from utils.logger import get_logger
import asyncio
import time

"""跑smt销售数据"""

logger = get_logger("tk_sale_data")
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

    logger.info(f"准备上传 {len(records)} 条记录...")

    # 批量上传，每批50条，批次间延迟0.2秒，失败时重试2次
    results = uploader.upload_batch_records(records, batch_size=50, delay=0.2, max_retries=2)

    # 分析结果
    successful_batches = [r for r in results if r.get("success")]
    failed_batches = [r for r in results if not r.get("success")]

    logger.info(f"\n上传统计:")
    logger.info(f"总批次: {len(results)}")
    logger.info(f"成功批次: {len(successful_batches)}")
    logger.info(f"失败批次: {len(failed_batches)}")

    if failed_batches:
        logger.info(f"\n失败详情:")
        for i, failed in enumerate(failed_batches):
            logger.info(f"  批次 {i + 1}: {failed.get('message', '未知错误')}")

    return results

def test_delete_records(config):

    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建删除器
    deleter = DingTalkSheetDeleter(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    print('开始删除数据')
    # 删除所有记录（谨慎使用！）
    # 注意：这里使用confirm=False，不会实际执行删除
    delete_all_result = deleter.delete_all_records(
        batch_size=50,
        delay=0.2,
        confirm=True  # 设置为True才会实际删除
    )
    print(f"删除所有记录结果: {delete_all_result.get('message')}")

    return deleter


def simple_match(shop_name):
    current_date = datetime.now().strftime("%Y%m%d")

    # 读取文件
    out_dir = Path(__file__).resolve().parent / "data" / "sale"
    sku_df = pd.read_csv(f'{out_dir}/{shop_name}_goods_{current_date}.csv')  # 货号ID,sku
    main_df = pd.read_csv(f'{out_dir}/{shop_name}_stock_{current_date}.csv')  # 平台,店铺,货号ID,商品名称,...

    sku_df['货号ID'] = sku_df['货号ID'].astype(str)
    main_df['货号ID'] = main_df['货号ID'].astype(str)

    # 使用merge合并数据
    result_df = pd.merge(
        main_df,
        sku_df,
        on='货号ID',
        how='left'
    )

    records=[]
    # 修复：解包 iterrows() 返回的元组
    for index, row in result_df.iterrows():
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

    logger = get_logger(name='smt_sale_main')
    logger.info('程序开始启动')

    config = {
        "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL", # 文档ID
        "sheet_id": "销量与库存-日更",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"    # 操作人ID
    }

    logger.info('---------------------------------开始删除数据-----------------------------------')
    test_delete_records(config)

    shop_name_list = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']
    # shop_name_list=['SMT214']
    for shop_name in shop_name_list:
        logger.info(f'---------------------------------开始爬取店铺--{shop_name}--库存数据-----------------------------------')
        spider_socket = SMTStockSpider(shop_name)
        await spider_socket.run()

        logger.info(f'---------------------------------开始爬取店铺--{shop_name}--商品数据-----------------------------------')
        spider_goods = SMTGoodsSpider(shop_name)
        await spider_goods.run()

        await asyncio.sleep(1)
        logger.info('---------------------------------开始匹配sku数据-----------------------------------')
        records=simple_match(shop_name)

        logger.info('---------------------------------开始上传数据-----------------------------------')
        upload_multiple_records(config, records)

        logger.info(f'{shop_name}数据上传成功')

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")






if __name__ == "__main__":
    asyncio.run(main())
