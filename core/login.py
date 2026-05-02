# core/login.py (非上下文管理器版本)
import json
import asyncio
from pathlib import Path
from utils.logger import get_logger
from utils.config_loader import get_shop_config
from core.browser import BrowserManager

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"
COOKIE_DIR.mkdir(parents=True, exist_ok=True)


class SellerSpriteLogin:
    def __init__(self, job, headless=False):
        cfg = get_shop_config("sellersprite")
        self.phone = cfg['account']
        self.password = cfg['password']
        self.logger = get_logger(job)
        self.headless = headless
        self.browser_manager = None
        self.page = None
        self.cookie_file = COOKIE_DIR / "sellersprite_cookie.json"
        self.initialized = False

    async def init_browser(self):
        """初始化浏览器"""
        try:
            if not self.initialized:
                self.browser_manager = BrowserManager(headless=self.headless)
                self.page = await self.browser_manager.start(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={"width": 1366, "height": 768}
                )
                self.initialized = True
                self.logger.info("浏览器初始化成功")
        except Exception as e:
            self.logger.error(f"浏览器初始化失败: {e}")
            raise

    async def close(self):
        """关闭浏览器"""
        if self.browser_manager:
            await self.browser_manager.close()
            self.initialized = False
            self.logger.info("浏览器已关闭")

    async def login(self):
        """执行登录"""
        try:
            # 确保浏览器已初始化
            await self.init_browser()

            await self.page.goto(
                "https://www.sellersprite.com/w/user/login?callback=%2Fv3%2Fproduct-research",
                wait_until="domcontentloaded"
            )

            # 等待页面加载
            await asyncio.sleep(2)

            # 账号登录
            await self.page.click('a[href="#pills-account"]')
            await self.page.wait_for_selector('#pills-account.show.active')

            await self.page.locator('#pills-account input[placeholder*="手机号"]:visible').fill(self.phone)
            await self.page.locator('#pills-account input[placeholder*="密"]:visible').fill(self.password)
            await self.page.locator('#pills-account button[type="submit"]:visible').click()

            # 等待登录成功
            await asyncio.sleep(3)

            # 验证登录状态
            try:
                user_name_element = await self.page.wait_for_selector('.user-name', timeout=10000)
                user_name = await user_name_element.inner_text()
                if user_name != "未登录":
                    self.logger.info("✅ 登录成功")
                    return True
                else:
                    self.logger.error('登录失败：用户名显示为未登录')
                    return False
            except Exception as e:
                self.logger.error(f'验证登录状态失败: {e}')
                return False

        except Exception as e:
            self.logger.error(f'登录失败: {e}')
            return False

    async def save_cookies(self):
        """保存cookies到文件"""
        try:
            if not self.page:
                self.logger.error("page对象不存在，无法保存cookies")
                return False

            cookies = await self.page.context.cookies()
            cookies_dict = {c["name"]: c["value"] for c in cookies}

            with open(self.cookie_file, "w", encoding="utf-8") as f:
                json.dump(cookies_dict, f, ensure_ascii=False, indent=2)

            self.logger.info(f"✅ Cookies已保存到: {self.cookie_file}")
            # await asyncio.sleep(60)
            return True
        except Exception as e:
            self.logger.error(f"保存cookies失败: {e}")
            return False


async def main():
    # 手动管理浏览器生命周期
    login = SellerSpriteLogin('amazon_goods_monitor', headless=False)
    try:
        # 登录
        if await login.login():
            # 获取cookie
            await login.save_cookies()

            await asyncio.sleep(60)
    finally:
        # 确保关闭浏览器
        await login.close()


# if __name__ == '__main__':
#     asyncio.run(main())