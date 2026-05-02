# core/login.py (非上下文管理器版本)
import json
import asyncio
from pathlib import Path
from utils.logger import get_logger
from utils.config_loader import get_shop_config
from core.browser import BrowserManager

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"
COOKIE_DIR.mkdir(parents=True, exist_ok=True)

class DianLeiDaLogin:
    def __init__(self, job, headless=False):
        cfg = get_shop_config("dianleida")
        self.phone = cfg['account']
        self.password = cfg['password']
        self.logger = get_logger(job)
        self.headless = headless
        self.browser_manager = None
        self.page = None
        self.cookie_file = COOKIE_DIR / "dianleida_headers.json"
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
                "https://www.dianleida.net/",
                wait_until="domcontentloaded"
            )

            # 等待页面加载
            await asyncio.sleep(2)

            # 账号登录

            await self.page.click('text=密码登录')

            # 等输入框
            await self.page.wait_for_selector('input[name="userPhone"]', timeout=15000)

            # 输入账号密码
            await self.page.fill('input[name="userPhone"]', self.phone)
            await self.page.fill('input[name="password"]', self.password)

            # 点击登录
            await self.page.click('button:has-text("登录")')


            # 等待登录成功
            await asyncio.sleep(3)

            return True

            # # 验证登录状态
            # try:
            #     # 等登录成功标志
            #     await self.page.wait_for_selector('text=进入工作台', timeout=15000)
            #
            #     return True
            # except Exception as e:
            #     self.logger.error(f'验证登录状态失败: {e}')
            #     return False

        except Exception as e:
            self.logger.error(f'登录失败: {e}')
            return False




async def main():
    # 手动管理浏览器生命周期
    login = DianLeiDaLogin('ali1688_goods_monitor', headless=False)
    try:
        # 登录
        await login.login()

    finally:
        # 确保关闭浏览器
        await login.close()

#
# if __name__ == '__main__':
#     asyncio.run(main())