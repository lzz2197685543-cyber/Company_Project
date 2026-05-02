import asyncio
from playwright.async_api import async_playwright
import os
from auth.login import DianLeiDaLogin
from api.filter_automation import OfferFilterAutomation
from util.page_helpers import close_popup_if_exists,close_btn_if_exists
from util.dingding_doc import upload_multiple_records
from storage.data_process import DataProcessor  # 导入 DataProcessor 类
from util.logger import get_logger

logger = get_logger('1688_main')

async def main():
    async with async_playwright() as p:
        # 获取当前工作目录
        current_path = os.getcwd()

        # 设置下载路径为当前路径下的 data 文件夹
        download_path = os.path.join(current_path, 'data')

        # 如果 data 文件夹不存在，创建它
        if not os.path.exists(download_path):
            os.makedirs(download_path)

        # 启动浏览器并指定下载路径
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            accept_downloads=True  # 启用下载功能
        )
        page = await context.new_page()

        # 设置下载路径
        # page.on("download", lambda download: download.save_as(os.path.join(download_path, download.suggested_filename)))

        # 1️⃣ 登录
        login = DianLeiDaLogin(
            phone="18929089237",
            password="lxz2580hh"
        )
        await login.login(page)

        # 2️⃣ 进入类目库页面
        await page.goto(
            "https://www.dianleida.net/1688/competeShop/category/library/",
            wait_until="networkidle"
        )

        # 3️⃣ 先关弹窗
        await close_btn_if_exists(page)
        await close_popup_if_exists(page)

        # 4️⃣ 自动设置筛选条件
        filter_bot = OfferFilterAutomation(page)
        await filter_bot.apply_all(
            category_name="玩具",
            min_price="3",
            min_sale_volume="10000",
            province="广东",
            shangxin_days=90,
        )

        # 5️⃣ 点击查询按钮
        print("✅ 设置筛选条件完成，点击查询按钮...")
        await page.click('button.dld-button.primary:has-text("开始查询")')  # 点击查询按钮

        # 等待查询完成，确保数据加载完成（可以根据实际页面情况调整）
        await page.wait_for_selector(".list-item-library-container", timeout=100000)  # 根据实际页面内容修改选择器

        await asyncio.sleep(2)

        # 6️⃣ 点击导出按钮
        print("✅ 查询完成，点击导出按钮...")
        await page.click('div.batch-btn.el-popover__reference')  # 点击导出按钮

        await  asyncio.sleep(1)

        # 7️⃣ 点击立即导出按钮
        print("✅ 导出弹窗显示，点击立即导出按钮...")

        async with page.expect_download(timeout=60_000) as download_info:
            await page.click('text=立即导出')

        download = await download_info.value

        save_path = os.path.join(download_path, download.suggested_filename)
        await download.save_as(save_path)

        print(f"✅ 文件已下载到: {save_path}")

        # 关闭浏览器
        await browser.close()

        # ---------------------------- 数据处理部分 ---------------------------------
        print('---------------------------------数据处理-----------------------------------')
        # 这里是调用 DataProcessor 类来进行数据处理
        data_processor = DataProcessor()
        recoders=data_processor.execute()

        # ---------------------------- 上传数据部分 ---------------------------------
        print('---------------------------------上传数据-----------------------------------')
        upload_multiple_records(config, recoders,logger)  # 执行上传操作


if __name__ == "__main__":
    config = {
        "base_id": "KGZLxjv9VG03dPLZt4B3yZgjJ6EDybno",
        "sheet_id": "电商平台选品1",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    print('---------------------------------开始导出数据-----------------------------------')
    asyncio.run(main())
