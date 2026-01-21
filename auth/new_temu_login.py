import asyncio
import json
from auth.new_temu_browser import BrowserManager
from utils.logger import get_logger
from utils.config_loader import get_shop_config
from auth.captcha.detector import YunmaCaptchaProcessor
from utils.page_helpers import temu_close_popup_if_exists


class GeekBILogin:
    def __init__(self, page=None):
        cfg = get_shop_config("geekbi")
        self.phone = cfg['account']
        self.password = cfg['password']
        self.logger = get_logger('GeekBILogin')
        self.captcha_processor = YunmaCaptchaProcessor(self.logger)
        self.page = page


    async def login(self):
        """主登录流程"""
        try:
            # 访问网站
            await self.page.goto("https://www.geekbi.com/")
            await self.page.wait_for_load_state("domcontentloaded")

            # 选择手机号登录
            await self.page.locator(".arco-tabs-tab-title", has_text="手机号登录").click()

            # 输入账号密码
            await self.page.fill('input[placeholder*="手机号"]', self.phone)
            await self.page.fill('input[placeholder*="密码"]', self.password)
            await self.page.click('button[type="submit"]')

            # 等待页面响应
            await asyncio.sleep(2)

            # ===========验证码处理=============

            # 检测是什么验证码
            captcha_type = await self.captcha_processor.wait_and_identify_captcha(self.page)

            if captcha_type:
                self.logger.info(f"检测到验证码类型: {captcha_type}")

                # 获取验证码详细信息
                captcha_info = await self.captcha_processor.handle_captcha(self.page, captcha_type)
                print(captcha_info)

                # 将信息发送到云码平台并且处理验证码
                await self.captcha_processor.send_to_yunma_api(self.page,captcha_info)

            await asyncio.sleep(3)

            # 登录进行之后点叉掉弹窗来的页面框
            await temu_close_popup_if_exists(self.page)



        except Exception as e:
            self.logger.error(f"登录过程异常: {e}")
            raise


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
        client = GeekBILogin(page)
        await client.login()

    finally:
        # 关闭浏览器
        await browser_manager.close()


async def main_with_context_manager():
    """主函数 - 使用方式2：使用上下文管理器"""
    async with BrowserManager(headless=False) as browser:
        page = browser.page
        client = GeekBILogin(page)
        await client.login()


async def main_simple():
    """主函数 - 使用方式3：简化版本（不含验证码处理）"""
    async with BrowserManager(headless=False) as browser:
        page = browser.page

        cfg = get_shop_config()
        logger = get_logger('GeekBILogin')

        try:
            await page.goto("https://www.geekbi.com/", wait_until="domcontentloaded")
            await page.locator(".arco-tabs-tab-title", has_text="手机号登录").click()
            await page.fill('input[placeholder*="手机号"]', cfg['account'])
            await page.fill('input[placeholder*="密码"]', cfg['password'])
            await page.click('button[type="submit"]')

            # 等待人工确认
            await asyncio.get_event_loop().run_in_executor(
                None, input, "检查页面后按回车关闭: "
            )

        except Exception as e:
            logger.error(f"登录异常: {e}")
            await page.screenshot(path="login_error.png")
            raise


# if __name__ == "__main__":
#     asyncio.run(main())