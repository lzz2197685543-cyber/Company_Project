import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from pathlib import Path
from utils.cookie_manager import CookieManager  # 假设这是你登录模块
from utils.logger import get_logger



COOKIE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "cookies"
STORAGE_STATE = COOKIE_DIR / "yicai_storage.json"


class YiCaiImageSearch:
    def __init__(self, headless=True):
        self.headless = headless
        self.cookie_manager = CookieManager()
        self.logger=get_logger('search_factory')

    async def ensure_login(self):
        """确保 storage_state 存在，否则调用登录模块"""
        if not STORAGE_STATE.exists():
            self.logger.info("⚠️ storage_state 不存在，开始登录...")
            await self.cookie_manager.refresh()
            self.logger.info("✅ 登录完成，storage_state 已生成")
        else:
            await self.cookie_manager.refresh()
            self.logger.info("✅ storage_state 存在")

    async def get_fid(self, image_path: str) -> str:
        await self.ensure_login()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)

            # ⭐ 复用登录态
            context = await browser.new_context(
                storage_state=str(STORAGE_STATE)
            )

            page = await context.new_page()

            try:
                # 1️⃣ 打开首页，测试登录是否有效
                await page.goto("https://www.essabuy.com/", timeout=100000)

            except PlaywrightTimeoutError:
                self.logger.info("⚠️ 登录态失效，重新登录...")
                await self.cookie_manager.refresh()
                context = await browser.new_context(
                    storage_state=str(STORAGE_STATE)
                )
                page = await context.new_page()
                await page.goto("https://www.essabuy.com/", timeout=10000)

            # 点击上传按钮
            await page.locator(".iconfont.icon-image").click()


            # 上传图片
            await page.set_input_files('input[type="file"]', image_path)

            # 等 fid 出现
            await page.wait_for_url(lambda url: "search-by-image" in url and "fid=" in url, timeout=15000)
            url = page.url
            fid = url.split("fid=")[1].split("&")[0]

            await browser.close()
            return fid


# -------------------------
async def run():
    searcher = YiCaiImageSearch(headless=True)
    fid = await searcher.get_fid(r"D:\sd14\Factory_sourcing\data\img\basketball.png")
    print("fid =", fid)

# asyncio.run(run())
