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



class TemuLogin:
    start_api = "http://127.0.0.1:6873/api/v1/browser/start"
    stop_api = "http://127.0.0.1:6873/api/v1/browser/stop"

    def __init__(self, name, account):
        self.name = name
        self.hub_id = str(account["hubId"])
        cred = account["credentials"]
        self.username = cred["username"]
        self.password = cred["password"]

        self.logger = get_logger(f"Temu-{name}")
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
    async def open_login_page(self):
        try:
            await self.page.goto(
                "https://seller.kuajingmaihuo.com/login",
                wait_until="domcontentloaded",
                timeout=15000
            )
            return True
        except Exception as e:
            self.logger.error(f"{self.name} - 打开登录页失败: {e}")
            return False

    async def login(self):
        self.logger.info(f"--------------------{self.name} ------------------------ 开始登录...")

        try:
            await self.page.get_by_text("账号登录", exact=True).wait_for(timeout=10000)
            await self.page.get_by_text("账号登录", exact=True).click()

            await self.page.wait_for_selector("#usernameId", timeout=10000)
            await self.page.wait_for_selector("#passwordId", timeout=10000)

            await self.page.fill("#usernameId", self.username)
            await self.page.fill("#passwordId", self.password)

            await self.page.get_by_test_id("beast-core-button").click()

            # 同意并登录（可能不存在）
            try:
                await self.page.get_by_role(
                    "button", name="同意并登录"
                ).click(timeout=5000)
            except Exception:
                pass
            #
            await self.page.wait_for_load_state("networkidle", timeout=20000)
            #
            # if "seller.kuajingmaihuo.com" not in self.page.url:
            #     raise Exception("登录后未进入卖家后台")
            #
            # try:
            #     cancel_btn = self.page.get_by_role("button", name="取消")
            #     await cancel_btn.click(timeout=1000)
            #
            #     # 等弹窗从 DOM 中消失
            #     await cancel_btn.wait_for(state="detached", timeout=1000)
            #
            #     self.logger.info(f"{self.name} - 取消弹窗已关闭")
            # except Exception:
            #     self.logger.error(f"{self.name} - 未出现取消按钮，跳过")
            #
            # try:
            #     await self.page.locator("text=进入").first.click(timeout=1000)
            #     self.logger.info(f"{self.name} - 已点击进入")
            # except Exception:
            #     self.logger.error(f"{self.name} - 未出现进入按钮，跳过")
            #
            # self.logger.info(f"{self.name} - 登录成功")
            return True

        except Exception as e:
            self.logger.error(f"{self.name} - 登录失败: {e}")
            return False

    async def save_cookies(self):
        self.logger.info(f"{self.name} - 等待 agentseller Cookie 生效")

        # 1️⃣ 进入 agentseller，触发 SSO + Cookie
        await self.page.goto(
            "https://agentseller.temu.com/",
            wait_until="domcontentloaded",
            timeout=20000
        )

        await self.page.wait_for_selector('[data-testid="beast-core-icon-down"]',state="visible")

        if "/authentication?" in self.page.url:
            try:
                # 1️⃣ 精准定位“可点击”的商家中心
                btn = self.page.locator("div.authentication_goto__bprbG")
                await btn.wait_for(state="visible", timeout=15000)

                # 2️⃣ 点击并捕获授权窗口
                async with self.page.expect_popup(timeout=15000) as popup_info:
                    await btn.click()

                auth_page = await popup_info.value
                await auth_page.wait_for_load_state("domcontentloaded", timeout=15000)

                # 3️⃣ 点击“确认授权并前往”
                confirm_btn = auth_page.get_by_text("确认授权并前往", exact=True)
                await confirm_btn.wait_for(state="visible", timeout=15000)
                await confirm_btn.click()

                self.logger.info("授权流程完成")
                await self.page.wait_for_load_state("load")

            except Exception as e:
                self.logger.error(f"授权流程失败: {e}")
                raise

        #  等页面真正“稳定”
        await self.page.wait_for_load_state("load")

        await asyncio.sleep(3)


        # 获取 Cookie（正确方式）
        cookies = await self.page.context.cookies(
            "https://agentseller.temu.com"
        )

        if not cookies:
            self.logger.error(f"{self.name} - 未获取到 agentseller Cookie")
            return False

        cookies_dict = {c["name"]: c["value"] for c in cookies}

        cookie_data = {
            "shop_name": self.name,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cookies": cookies_dict,
        }

        path = COOKIE_DIR / f"{self.name}_cookies.json"

        with open(path, "w", encoding="utf-8") as f:
            json.dump(cookie_data, f, ensure_ascii=False, indent=2)

        self.logger.info(f"{self.name} - Cookie 已保存: {path}")

        # 4️⃣ 关键：这里明确告诉你——可以安全关闭浏览器了
        self.logger.info(f"{self.name} - Cookie 获取完成，准备关闭浏览器")
        return True

    # ----------- 总流程 -----------
    async def run(self):
        try:
            if not await self.start_browser():
                return False
            if not await self.connect():
                return False
            if not await self.open_login_page():
                return False
            if not await self.login():
                return False
            return await self.save_cookies()

        finally:
            await self.close()

    async def close(self):
        try:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        finally:
            await self.stop_browser()


async def main():
    name_list = ["1104-Temu全托管"]
    for name in name_list:
        account = get_shop_config(name)
        print(account)

        t = TemuLogin(name, account)
        await t.run()


if __name__ == "__main__":
    asyncio.run(main())



