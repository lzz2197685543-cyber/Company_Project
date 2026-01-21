from auth.new_temu_browser import BrowserManager
from auth.new_temu_login import GeekBILogin
from api.temu_filter_automation import OfferFilterAutomation
import asyncio
from utils.logger import get_logger
from utils.dingding_doc import DingTalkTokenManager,DingTalkSheetUploader
from storage.temu_data_process import TemuDataProcessor


def upload_multiple_records(config, records,logger):
    """
    批量上传多条记录 - 修复NaN问题版
    """
    token_manager = DingTalkTokenManager()
    uploader = DingTalkSheetUploader(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    logger.info(f"准备上传 {len(records)} 条记录...")

    # 关键修复：处理NaN值
    import math

    def fix_nan(obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            # 大时间戳转为字符串
            elif obj > 1e12:
                from datetime import datetime
                try:
                    return datetime.fromtimestamp(obj / 1000).strftime("%Y-%m-%d")
                except:
                    return str(obj)
        return obj

    # 预处理所有记录
    processed_records = []
    for record in records:
        new_record = {}
        for key, value in record.items():
            if isinstance(value, dict):
                new_record[key] = {k: fix_nan(v) for k, v in value.items()}
            else:
                new_record[key] = fix_nan(value)
        processed_records.append(new_record)

    logger.info(f"已完成数据预处理，修复了NaN和Infinity值")

    # 上传
    results = uploader.upload_batch_records(processed_records, batch_size=50, delay=0.2, max_retries=2)

    # 分析结果
    successful = [r for r in results if r.get("success")]

    logger.info(f"\n上传统计:")
    logger.info(f"总批次: {len(results)}")
    logger.info(f"成功批次: {len(successful)}")
    logger.info(f"失败批次: {len(results) - len(successful)}")

    # 如果还有失败，保存这些记录
    if len(successful) < len(results):
        failed_records = []
        for i, result in enumerate(results):
            if not result.get("success"):
                start_idx = i * 50
                end_idx = min(start_idx + 50, len(processed_records))
                failed_records.extend(processed_records[start_idx:end_idx])

        if failed_records:
            import json
            import os
            from datetime import datetime

            os.makedirs("failed_records", exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = os.path.join("failed_records", f"final_failed_{timestamp}.json")

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(failed_records, f, ensure_ascii=False, indent=2)

            logger.info(f"仍有 {len(failed_records)} 条记录失败，已保存到: {filepath}")

    return results

async def main():
    config = {
        "base_id": "KGZLxjv9VG03dPLZt4B3yZgjJ6EDybno",
        "sheet_id": "电商平台选品1",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    """主函数 - 使用方式1：手动管理浏览器"""
    # 创建浏览器管理器
    browser_manager = BrowserManager(headless=False)
    logger=get_logger('Temu_New')


    try:
        # 启动浏览器
        page = await browser_manager.start(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1366, "height": 768}
        )

        # ================创建登录实例================
        client = GeekBILogin(page)
        await client.login()

        # =================数据爬取==================
        # 条件筛选,监听，处理数据
        offer_filter = OfferFilterAutomation(page,logger)
        # 1️⃣ 条件筛选（不监听）
        await offer_filter.get_offer_filter()

        # 2️⃣ 搜索（监听第 1 页）
        all_items = []

        # 首次搜索
        items = await offer_filter.do_search()
        all_items.extend(items)

        # 循环翻页
        while not offer_filter.should_stop:
            next_items = await offer_filter.next_page()
            if not next_items:
                break
            all_items.extend(next_items)

        # 保存所有数据
        offer_filter.save_batch(all_items)

        logger.info('---------------------------------开始去重数据-----------------------------------')
        processor = TemuDataProcessor()

        # 筛选新数据
        new_data = processor.filter_new_data()

        logger.info('---------------------------------开始构建上传的数据-----------------------------------')
        records = processor.build_records(new_data)

        # 将上传的数据保存到数据库
        processor.import_csv_to_product_monitor(new_data)

        logger.info('---------------------------------开始上传数据-----------------------------------')
        upload_multiple_records(config, records,logger)

        logger.info(f'数据上传成功')

    finally:
            # 关闭浏览器
            await browser_manager.close()

if __name__ == '__main__':
    asyncio.run(main())