# main.py
import asyncio
from playwright.async_api import async_playwright

from auth.login import MaiJiaLogin
from api.offer_api_capture import OfferApiCapture
from api.filter_automation import OfferFilterAutomation
from utils.page_helpers import close_popup_if_exists
from storage.csv_writer import CSVWriter


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # 1️⃣ 登录
        login = MaiJiaLogin(
            phone="18929089237",
            password="lxz2580hh"
        )
        await login.login(page)

        # 2️⃣ 进入类目库页面
        await page.goto(
            "https://www.dianleida.net/1688/competeShop/category/library/",
            wait_until="networkidle"
        )

        await asyncio.sleep(3)

        # ⭐ 先关弹窗
        await close_popup_if_exists(page)

        # 3️⃣ 自动设置筛选条件
        filter_bot = OfferFilterAutomation(page)
        await filter_bot.apply_all(
            category_name="玩具",
            min_price="3",
            min_sale_volume="10000",
            province="广东",
            shangxin_days=90,
            page_size=180
        )

        # 4️⃣ 捕获真实查询请求
        capturer = OfferApiCapture(page)
        req = await capturer.capture_once()

        # print("\n========== 最终请求 ==========")
        print("URL:", req["url"])
        print("PAYLOAD:", req["payload"])

        # 捕获 offerSearch 请求并打印响应
        # response_json = await filter_bot.capture_offer_request()

        # 4️⃣ CSV
        csv_writer = CSVWriter("data/offers_1688.csv")

        # 5️⃣ 翻页抓取
        await filter_bot.crawl_pages(total_pages=10, csv_writer=csv_writer)



        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
