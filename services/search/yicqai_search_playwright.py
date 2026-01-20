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

    async def get_fid(self, image_path: str, max_retry: int = 3) -> str:
        await self.ensure_login()

        for attempt in range(1, max_retry + 1):
            self.logger.info(f"🔁 第 {attempt}/{max_retry} 次尝试获取 fid")

            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=self.headless)

                    context = await browser.new_context(
                        storage_state=str(STORAGE_STATE)
                    )
                    page = await context.new_page()

                    # 1️⃣ 打开首页
                    await page.goto(
                        "https://www.essabuy.com/",
                        timeout=60000,
                        wait_until="domcontentloaded"
                    )

                    # 2️⃣ 点击上传按钮
                    await page.locator(".iconfont.icon-image").click(timeout=5000)

                    # 3️⃣ 上传图片
                    await page.set_input_files(
                        'input[type="file"]',
                        image_path
                    )

                    # 4️⃣ 等待 URL 出现 fid
                    await page.wait_for_url(
                        lambda url: "search-by-image" in url and "fid=" in url,
                        timeout=15000
                    )

                    # 5️⃣ 解析 fid
                    url = page.url
                    fid = url.split("fid=")[1].split("&")[0]

                    if fid:
                        self.logger.info(f"✅ 成功获取 fid：{fid}")
                        await browser.close()
                        return fid

                    raise RuntimeError("URL 中未解析到 fid")

            except (PlaywrightTimeoutError, Exception) as e:
                self.logger.warning(f"⚠️ 第 {attempt} 次失败：{e}")

                # 登录态可能失效，刷新一次
                if attempt < max_retry:
                    self.logger.info("🔄 刷新登录态后重试")
                    await self.cookie_manager.refresh()

                await asyncio.sleep(2)  # 稍微缓一下，避免频繁请求

            finally:
                try:
                    await browser.close()
                except Exception:
                    pass

        # 3 次都失败
        raise RuntimeError("❌ 重试 3 次后仍未获取到 fid")


# -------------------------
async def run():
    searcher = YiCaiImageSearch(headless=True)
    fid = await searcher.get_fid(r"D:\sd14\Factory_sourcing\data\img\basketball.png")
    print("fid =", fid)

# asyncio.run(run())
