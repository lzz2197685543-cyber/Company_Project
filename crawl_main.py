import asyncio
from core.browser import BrowserManager
from core.new_temu_login import (GeekBILogin)
from modules.crawler.temu_offer_filter import OfferFilterAutomation
from storage.product_dao import ProductDAO
from utils.logger import get_logger
from storage.db import Database


async def main():
    job='auto_listing'
    db = Database()
    db.init_db()
    print("✅ 数据库初始化完成")

    logger = get_logger(job)
    browser_manager = BrowserManager(headless=False)

    try:
        page = await browser_manager.start(
            user_agent="Mozilla/5.0",
            viewport={"width": 1366, "height": 768}
        )

        # 登录极鲸云
        client = GeekBILogin(page,job)
        await client.login()

        # 爬虫---爬取数据
        page_urls = [
            'https://www.geekbi.com/data/goods/hot-sale',
            'https://www.geekbi.com/data/goods/day-sale-rise',
            # 'https://www.geekbi.com/data/goods/blue-ocean-hot-sale',
            # 'https://www.geekbi.com/data/goods/hot-sale-new',
            # 'https://www.geekbi.com/data/goods/new-mall-hot-sale',
            # 'https://www.geekbi.com/data/goods/big-sale-new'
        ]

        offer_filter = OfferFilterAutomation(page, logger)

        for url in page_urls:
            await offer_filter.get_offer_filter(url)

            # 首页
            items = await offer_filter.do_search()
            ProductDAO.insert_products(items,logger)

            # 翻页
            while not offer_filter.should_stop:
                next_items = await offer_filter.next_page()
                if not next_items:
                    break

                ProductDAO.insert_products(next_items,logger)


    finally:
        await browser_manager.close()


if __name__ == '__main__':
    asyncio.run(main())