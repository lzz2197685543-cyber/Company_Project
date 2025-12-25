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



class TemuLogin_F:
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
                await auth_page.wait_for_load_state("load", timeout=15000)

                await asyncio.sleep(3)

                # -------- 授权方式一：确认授权并前往 --------
                confirm_btn_1 = auth_page.get_by_text("确认授权并前往", exact=True)

                if await confirm_btn_1.count() > 0:
                    self.logger.info("命中授权方式一：确认授权并前往")
                    await confirm_btn_1.first.click()

                else:
                    # -------- 授权方式二：勾选复选框 + 授权登录 --------
                    self.logger.info("命中授权方式二：勾选复选框 + 授权登录")

                    # ① 不点 input，点可视的 checkbox 外壳
                    checkbox_ui = auth_page.locator('div[class*="CBX_square"]')

                    if await checkbox_ui.count() > 0:
                        await checkbox_ui.first.click()
                        self.logger.info("已点击授权复选框（UI 容器）")
                    else:
                        self.logger.info("未发现授权复选框 UI，可能已默认勾选")

                    # ② 再点“授权登录”
                    confirm_btn = auth_page.get_by_text("授权登录", exact=True)
                    if await confirm_btn.count() > 0:
                        await confirm_btn.first.click()
                        self.logger.info("已点击授权登录")
                        await self.page.wait_for_load_state("load")
                        await asyncio.sleep(10)
                    else:
                        self.logger.info("未发现授权登录按钮")

                self.logger.info("授权流程完成")
                await self.page.wait_for_load_state("load")

                await asyncio.sleep(3)

            except Exception as e:
                self.logger.error(f"授权流程失败: {e}")
                raise

        await self.page.goto('https://seller.kuajingmaihuo.com/labor/bill')


        #  等页面真正“稳定”
        await self.page.wait_for_load_state("load")

        await asyncio.sleep(3)

        TARGET_API_KEYWORD = "api/merchant/fund/detail/pageSearch"

        # 1️⃣ 准备监听接口
        async with self.page.expect_response(
                lambda r: TARGET_API_KEYWORD in r.url and r.status == 200,
                timeout=15_000
        ) as resp_info:

            # 2️⃣ 点击「查询」按钮
            query_btn = self.page.get_by_role("button", name="查询")
            await query_btn.wait_for(state="visible")
            await query_btn.scroll_into_view_if_needed()
            await query_btn.click()

        # 3️⃣ 接口已经命中
        response = await resp_info.value
        print("接口 URL:", response.url)
        print("状态码:", response.status)

        # 4️⃣ 此时 cookie 一定是最新的
        cookies = await self.page.context.cookies()
        cookies_dict = {c["name"]: c["value"] for c in cookies}

        cookie_data = {
            "shop_name": self.name,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cookies": cookies_dict,
        }

        path = COOKIE_DIR / f"{self.name}_financial_cookies.json"

        with open(path, "w", encoding="utf-8") as f:
            json.dump(cookie_data, f, ensure_ascii=False, indent=2)

        self.logger.info(f"{self.name} - Cookie 已保存: {path}")

        # 4️⃣ 关键：这里明确告诉你——可以安全关闭浏览器了
        self.logger.info(f"{self.name} - Cookie 获取完成，准备关闭浏览器")
        return True

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
    async def run(self, max_retry=3):
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
#     name_list = ['102-Temu全托管']
#     for name in name_list:
#         account = get_shop_config(name)
#         print(account)
#
#         t = TemuLogin_F(name, account)
#         await t.run()
#
#
# if __name__ == "__main__":
#     asyncio.run(main())



