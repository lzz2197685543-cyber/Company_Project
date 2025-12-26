
import time
from logger_config import SimpleLogger
from dingding_doc import DingTalkSheetDeleter,DingTalkSheetUploader,DingTalkTokenManager
import pandas as pd
from datetime import datetime
from shein_sale_data import Shein


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
    filename = f"./data/data/shein_sale_{current_date}.csv"
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


def run_shein_sale():
    name_list = ["希音全托301-yijia", "希音全托302-juyule", "希音全托303-kedi", "希音全托304-xiyue"]
    for shop_name in name_list:
        print(f'开始爬取店铺---{shop_name}---的数据')
        shein = Shein(shop_name)
        shein.get_all_page()



if __name__ == '__main__':

    logger = SimpleLogger(name='run')
    logger.info('程序开始启动')

    config = {
        "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL", # 文档ID
        "sheet_id": "销量与库存-日更",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"    # 操作人ID
    }

    # 记录总时间开始
    total_start_time = time.time()

    print('---------------------------------开始爬取shein数据-----------------------------------')
    run_shein_sale()

    print('---------------------------------开始匹配sku数据-----------------------------------')
    records=build_records()
    print(records)

    print('---------------------------------开始上传数据-----------------------------------')
    upload_multiple_records(config, records)

    print(f'数据上传成功')
    logger.info('数据上传成功')


    time.sleep(3)


    # 计算总时间
    total_time = time.time() - total_start_time

    # 输出统计信息
    print(f"\n{'=' * 60}")
    print(f"🎉 所有店铺处理完成！")
    print(f"{'=' * 60}")
    print(f"📊 统计信息:")

    print(f"⏱️  总耗时: {total_time:.2f} 秒")
    logger.info(f"⏱️  总耗时: {total_time:.2f} 秒")

    print(f"⏱️  开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(total_start_time))}")
    logger.info(f"⏱️  开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(total_start_time))}")
    print(f"⏱️  结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
    logger.info(f"⏱️  结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")