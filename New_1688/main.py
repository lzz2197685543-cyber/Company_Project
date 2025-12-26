import asyncio
from playwright.async_api import async_playwright
import os
from auth.login import MaiJiaLogin
from api.offer_api_capture import OfferApiCapture
from api.filter_automation import OfferFilterAutomation
from utils.page_helpers import close_popup_if_exists
from dingding_doc import DingTalkSheetUploader, DingTalkTokenManager
from storage.data_process import DataProcessor  # 导入 DataProcessor 类

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
        page.on("download", lambda download: download.save_as(os.path.join(download_path, download.suggested_filename)))

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

        # 3️⃣ 先关弹窗
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
        await page.wait_for_selector(".list-item-library-container", timeout=10000)  # 根据实际页面内容修改选择器

        # 6️⃣ 点击导出按钮
        print("✅ 查询完成，点击导出按钮...")
        await page.click('div.batch-btn.el-popover__reference')  # 点击导出按钮

        await  asyncio.sleep(1)

        # 7️⃣ 点击立即导出按钮
        print("✅ 导出弹窗显示，点击立即导出按钮...")
        # await page.click('div.dfc.btn.mt-10 div[data-v-1e70f651=""]')  # 点击立即导出按钮

        # 等弹窗里的“立即导出”按钮出现
        await page.wait_for_selector('text=立即导出', timeout=15000)
        # 再点击
        await page.click('text=立即导出')

        await asyncio.sleep(15)

        # 等待导出操作完成（根据实际情况调整等待时间或监听下载事件）
        await page.wait_for_timeout(3000)  # 等待 3 秒，确保导出操作完成
        print("✅ 导出操作完成")

        # 关闭浏览器
        await browser.close()

        # ---------------------------- 数据处理部分 ---------------------------------
        print('---------------------------------数据处理-----------------------------------')
        # 这里是调用 DataProcessor 类来进行数据处理
        data_processor = DataProcessor()
        recoders=data_processor.execute()

        # ---------------------------- 上传数据部分 ---------------------------------
        print('---------------------------------上传数据-----------------------------------')
        upload_multiple_records(config, recoders)  # 执行上传操作

def upload_multiple_records(config, records):
    """
    批量上传多条记录的完整示例
    """
    # 配置参数（请替换为实际值）
    token_manager = DingTalkTokenManager()
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

if __name__ == "__main__":
    config = {
        "base_id": "KGZLxjv9VG03dPLZt4B3yZgjJ6EDybno",
        "sheet_id": "电商平台选品1",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    print('---------------------------------开始导出数据-----------------------------------')
    asyncio.run(main())
