from auth.new_temu_browser import BrowserManager
from auth.new_temu_login import GeekBILogin
from api.temu_filter_automation import OfferFilterAutomation
import asyncio
from util.logger import get_logger
from util.dingding_doc import upload_multiple_records
from storage.temu_data_process import TemuDataProcessor



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