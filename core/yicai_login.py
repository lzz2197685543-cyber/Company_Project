import asyncio
import json
from playwright.async_api import async_playwright
from utils.logger import get_logger
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from utils.dingtalk_bot import ding_bot_send
from utils.config_loader import get_shop_config
from pathlib import Path

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"


# 确保目录存在
COOKIE_DIR.mkdir(parents=True, exist_ok=True)


class YiCaiLogin:
    def __init__(self):
        self.logger = get_logger('search_factory')
        cfg=get_shop_config('yicai')
        self.account=cfg['account']
        self.password=cfg['password']

        self.playwright = None
        self.browser = None
        self.page = None

    async def init_browser(self) -> bool:
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(headless=False)
            self.context = await self.browser.new_context()
            self.page = await self.context.new_page()
            return True
        except Exception as e:
            self.logger.error("初始化浏览器失败", exc_info=True)
            return False

    async def login(self):
        try:
            # 1️⃣ 打开登录页
            await self.page.goto(
                "https://www.essabuy.com/login?",
                wait_until="domcontentloaded",
                timeout=20000
            )

            # 等手机号输入框出现并可交互
            phone_input = self.page.get_by_role("textbox", name="手机号码")
            await phone_input.wait_for(state="visible")
            await phone_input.click()
            await phone_input.fill(self.account)


            # 密码
            password_input = self.page.get_by_role("textbox", name="密码")
            await password_input.wait_for(state="visible")
            await password_input.click()
            await password_input.fill(self.password)

            # 记住密码
            remember_checkbox = self.page.get_by_role("checkbox", name="记住密码")
            await remember_checkbox.wait_for(state="visible")
            await remember_checkbox.check()

            # 登录按钮
            login_button = self.page.get_by_role("button", name="登录")
            await login_button.wait_for(state="visible")
            await login_button.click()

            locator = self.page.locator(".category-btn > .flex-item-ver-center")
            await locator.wait_for(state="visible")

            return True

        except Exception as e:
            self.logger.exception(f"登录异常：{e}")
            raise

    async def save_cookies(self):
        cookies = await self.page.context.cookies()

        cookie_dict = {c["name"]: c["value"] for c in cookies}

        with open(f'{COOKIE_DIR}/yicai_cookies.json', "w", encoding="utf-8") as f:
            json.dump(cookie_dict, f, ensure_ascii=False, indent=2)

        # ⭐ 给 Playwright 用
        await self.context.storage_state(
            path=f"{COOKIE_DIR}/yicai_storage.json"
        )

        self.logger.info("✅ Cookie & storage_state 已保存")
        return True


    # -----------失败可以重新登录------------
    async def run_once(self):
        if not await self.init_browser():
            raise Exception("browser 失败")

        if not await self.login():
            raise Exception("login 失败")

        if not await self.save_cookies():
            raise Exception("save_cookies 失败")

        return True

    # ----------- 总流程 -----------
    async def run(self,max_retry=3):
        for attempt in range(1,max_retry+1):
            self.logger.info(f"YiCai---第 {attempt} 次登录尝试")

            try:
                result = await self.run_once()
                if result:
                    self.logger.info(f"YiCai - 登录成功（第 {attempt} 次）")
                    return True

            except Exception as e:
                self.logger.error(
                    f"YiCai - 第 {attempt} 次失败: {e}",
                    exc_info=True
                )

            finally:
                await self.close()

            if attempt < max_retry:
                self.logger.info(f"YiCai - 准备重试，等待 3 秒...")
                await asyncio.sleep(3)

        self.logger.error(f"YiCai- 登录失败，已达到最大重试次数 {max_retry}")
        ding_bot_send('me',f"YiCai- 登录失败，已达到最大重试次数 {max_retry}")
        return False

    async def close(self):
        try:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            self.logger.error(f'浏览器关闭失败')


async def main():
    t = YiCaiLogin()
    await t.run()

#
# if __name__ == "__main__":
#     asyncio.run(main())
