import asyncio
from utils.config_loader import get_shop_config
from utils.logger import get_logger
from core.browser import BrowserManager

from pathlib import Path

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"

# 确保目录存在
COOKIE_DIR.mkdir(parents=True, exist_ok=True)


class LingXingERPLogin:
    def __init__(self,job,page=None):
        cfg = get_shop_config()
        self.username = cfg["username"]
        self.password = cfg["password"]
        self.logger = get_logger(job)

        self.page = page


    async def wait_for_element(self, selector, timeout=10000, state="visible"):
        """等待元素出现"""
        try:
            await self.page.wait_for_selector(
                selector,
                timeout=timeout,
                state=state
            )
            return True
        except Exception as e:
            self.logger.error(f"等待元素失败: {selector}")
            return False

    async def login(self):
        try:
            # 1️⃣ 打开登录页
            self.logger.info("正在打开登录页面...")
            await self.page.goto(
                "https://erp.lingxing.com/",
                wait_until="domcontentloaded",
                timeout=30000
            )


            self.logger.info("等待登录表单加载...")

            # 等待用户名输入框
            username_locator = self.page.get_by_role("textbox", name="手机号/用户名/邮箱")
            await username_locator.wait_for(state="visible", timeout=15000)

            # 输入用户名
            self.logger.info("输入用户名...")
            await username_locator.click()
            await username_locator.fill(self.username)

            # 等待密码输入框
            password_locator = self.page.get_by_role("textbox", name="密码")
            await password_locator.wait_for(state="visible", timeout=10000)

            # 输入密码
            self.logger.info("输入密码...")
            await password_locator.click()

            await password_locator.fill(self.password)


            # 等待登录按钮
            self.logger.info("等待登录按钮...")
            login_button = self.page.get_by_role("button", name="登录")
            await login_button.wait_for(state="visible", timeout=10000)


            # 点击登录按钮
            self.logger.info("点击登录按钮...")
            await login_button.click()

            # 等待登录完成 - 这里可以等待登录后出现的特定元素
            # 例如：等待用户头像、菜单等元素出现

            self.logger.info("等待登录成功...")
            current_url = self.page.url
            # 等待导航或特定元素出现
            if "erp.lingxing.com/erp/home" in current_url:
                self.logger.info("✓ URL验证通过：已进入成功页面")

            await self.page.wait_for_load_state("networkidle", timeout=10000)

            self.logger.info("领星ERP-跨境电商管理系统 登录成功")


        except Exception as e:
            raise e



async def main():
    """主函数 - 使用方式1：手动管理浏览器"""
    # 创建浏览器管理器
    browser_manager = BrowserManager(headless=False)
    try:
        # 启动浏览器
        page = await browser_manager.start(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1366, "height": 768}
        )

        # 创建登录实例
        client = LingXingERPLogin('lingxing',page)
        await client.login()



    finally:
        # 关闭浏览器
        await browser_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
