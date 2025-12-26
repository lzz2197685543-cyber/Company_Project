import json
import asyncio
import requests
from playwright.async_api import async_playwright
import os
from datetime import datetime
from utils.config_loader import get_shop_config
from utils.logger import get_logger
from pathlib import Path
COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"
# 确保目录存在
COOKIE_DIR.mkdir(parents=True, exist_ok=True)



class TKLogin:
    start_api = "http://127.0.0.1:6873/api/v1/browser/start"
    stop_api = "http://127.0.0.1:6873/api/v1/browser/stop"

    def __init__(self, name, account):
        self.name = name
        self.hub_id = str(account["hubId"])
        cred = account["credentials"]
        self.username = cred["username"]
        self.password = cred["password"]

        self.logger = get_logger(f"login")
        self.debug_port = None
        self.playwright = None
        self.browser = None
        self.page = None

    # ----------- 浏览器 -----------
    async def start_browser(self):
        try:
            res = requests.post(
                self.start_api,
                json={"containerCode": self.hub_id},
                timeout=10
            ).json()

            self.logger.info(f"{self.name} - start_api 返回: {res}")

            if res.get("code") != 0:
                self.logger.error(f'{self.name} - 启动失败: {res.get("msg")}')
                return False

            self.debug_port = res.get("data", {}).get("debuggingPort")
            if not self.debug_port:
                self.logger.error(f"{self.name} - 未获取到 debuggingPort")
                return False

            self.logger.info(f"{self.name} - 浏览器启动成功, 调试端口: {self.debug_port}")
            return True

        except Exception as e:
            self.logger.error(f"{self.name} - 启动异常: {e}")
            return False

    async def stop_browser(self):
        try:
            requests.post(
                self.stop_api,
                json={"containerCode": self.hub_id},
                timeout=10
            )
        except Exception:
            pass

    async def connect(self):
        try:
            await asyncio.sleep(1)  # 等浏览器完全启动

            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.connect_over_cdp(
                f"http://127.0.0.1:{self.debug_port}"
            )

            try:
                ctx = self.browser.contexts[0]
                self.page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            except Exception:
                ctx = await self.browser.new_context()
                self.page = await ctx.new_page()

            self.logger.info(f"{self.name} - 已连接浏览器")
            return True

        except Exception as e:
            self.logger.error(f"{self.name} - 连接失败: {e}")
            return False

    # ----------- 业务 -----------
    async def is_logged_in(self) -> bool:
        try:
            await self.page.wait_for_selector(
                "div:has-text('商家中心')",
                timeout=5_000
            )
            return True
        except:
            return False

    async def open_login_page(self):
        try:
            await self.page.goto(
                "https://seller.tiktokshopglobalselling.com/homepage?shop_region=GB",
                wait_until="domcontentloaded",
                timeout=20_000
            )

            # 网络兜底
            await self.page.wait_for_selector("body", timeout=5_000)

            if await self.is_logged_in():
                self.logger.info(f"{self.name} - 已是登录状态")
                return True

            self.logger.info(f"{self.name} - 当前为未登录状态")
            return True

        except Exception as e:
            self.logger.error(f"{self.name} - 打开页面失败: {e}")
            return False

    async def login(self):
        self.logger.info(f"{self.name} - 开始执行登录流程")

        # 再兜一层，防止重复登录
        if await self.is_logged_in():
            self.logger.info(f"{self.name} - 无需登录")
            return True

        try:
            await self.page.wait_for_load_state("domcontentloaded")

            await self.page.get_by_role(
                "textbox", name="请输入你的手机号"
            ).fill(self.username)

            await self.page.get_by_role(
                "textbox", name="请输入您的密码"
            ).fill(self.password)

            await self.page.get_by_role(
                "button", name="登录"
            ).click()

            # ⭐ 登录成功唯一判断
            await self.page.wait_for_selector(
                "div:has-text('商家中心')",
                timeout=30_000
            )

            self.logger.info(f"{self.name} - 登录成功")
            return True

        except Exception as e:
            self.logger.error(f"{self.name} - 登录失败: {e}")
            return False

    async def save_cookies(self):
        try:
            context = self.page.context
            cookies = await context.cookies()

            cookies_dict = {
                c["name"]: c["value"] for c in cookies
            }

            cookie_data = {
                "shop_name": self.name,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "cookies": cookies_dict,
            }

            path = COOKIE_DIR / f"{self.name}_cookies.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(cookie_data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"{self.name} - cookies 已保存")
            return True

        except Exception as e:
            self.logger.error(f"{self.name} - 保存 cookies 失败: {e}")
            return False

    # -----------失败可以重新登录------------
    async def run_once(self):
        if not await self.start_browser():
            raise Exception("start_browser 失败")

        if not await self.connect():
            raise Exception("connect 失败")

        if not await self.open_login_page():
            raise Exception("open_login_page 失败")

        if not await self.login():
            raise Exception("login 失败")

        if not await self.save_cookies():
            raise Exception("save_cookies 失败")

        return True

    # ----------- 总流程 -----------
    async def run(self, max_retry=1):
        self.logger.info(f"--------------------{self.name} ------------------------ 开始登录...")
        for attempt in range(1, max_retry + 1):
            self.logger.info(f"{self.name} - 第 {attempt} 次登录尝试")

            try:
                result = await self.run_once()
                if result:
                    self.logger.info(f"{self.name} - 登录成功（第 {attempt} 次）")
                    return True

            except Exception as e:
                self.logger.error(
                    f"{self.name} - 第 {attempt} 次失败: {e}",
                    exc_info=True
                )

            finally:
                await self.close()

            if attempt < max_retry:
                self.logger.info(f"{self.name} - 准备重试，等待 3 秒...")
                await asyncio.sleep(3)

        self.logger.error(f"{self.name} - 登录失败，已达到最大重试次数 {max_retry}")
        return False

    async def close(self):
        try:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        finally:
            await self.stop_browser()


# async def main():
#     name_list = ["TK全托1401店", "TK全托408-LXZ", "TK全托407-huidan", "TK全托406-yuedongwan", "TK全托405-huanchuang",
#                  "TK全托404-kedi", "TK全托403-juyule", "TK全托401-xiyue", "TK全托402-quzhi", "TK全托1402店"]
#     for name in name_list:
#         account = get_shop_config(name)
#
#         t = TKLogin(name, account)
#         await t.run()
#
#
# if __name__ == "__main__":
#     asyncio.run(main())



