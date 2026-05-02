from auth.new_temu_browser import BrowserManager
from auth.new_temu_login import GeekBILogin
from api.temu_filter_automation import OfferFilterAutomation
import asyncio
from util.logger import get_logger
from util.dingding_doc import upload_multiple_records
from storage.temu_data_process import TemuDataProcessor



# 修改后的 main 函数（只输出改动部分）

async def main():
    config = {
        "base_id": "KGZLxjv9VG03dPLZt4B3yZgjJ6EDybno",
        "sheet_id": "电商平台选品1",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    browser_manager = BrowserManager(headless=False)
    logger = get_logger('Temu_New')

    try:
        page = await browser_manager.start(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1366, "height": 768}
        )

        client = GeekBILogin(page)
        await client.login()

        page_urls = [
            'https://www.geekbi.com/data/goods/hot-sale',
            # 'https://www.geekbi.com/data/goods/day-sale-rise',
            # 'https://www.geekbi.com/data/goods/blue-ocean-hot-sale',
            # 'https://www.geekbi.com/data/goods/hot-sale-new',
            # 'https://www.geekbi.com/data/goods/new-mall-hot-sale',
            # 'https://www.geekbi.com/data/goods/big-sale-new'
        ]
        offer_filter = OfferFilterAutomation(page, logger)

        for url in page_urls:
            await offer_filter.get_offer_filter(url)

            # 首次搜索 → 立即保存
            items = await offer_filter.do_search()
            if items:
                offer_filter.save_batch(items)

            # 循环翻页 → 每页立即保存
            while not offer_filter.should_stop:
                next_items = await offer_filter.next_page()
                if not next_items:
                    break
                if next_items:
                    offer_filter.save_batch(next_items)

        # 去重、上传等后续逻辑保持不变
        logger.info('---------------------------------开始去重数据-----------------------------------')
        processor = TemuDataProcessor()
        new_data = processor.filter_new_data()

        logger.info('---------------------------------开始构建上传的数据-----------------------------------')
        records = processor.build_records(new_data)

        processor.import_csv_to_product_monitor(new_data)

        logger.info('---------------------------------开始上传数据-----------------------------------')
        upload_multiple_records(config, records, logger)

        logger.info(f'数据上传成功')

    finally:
        await browser_manager.close()

if __name__ == '__main__':
    asyncio.run(main())