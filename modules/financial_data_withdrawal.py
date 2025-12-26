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

    async def wait_wallet_frame(self, page, timeout=30_000):
        """
        等待钱包 iframe 出现并返回 frame
        """
        loop = asyncio.get_event_loop()
        end_time = loop.time() + timeout / 1000

        while loop.time() < end_time:
            for frame in page.frames:
                if frame.url and "business_wallet" in frame.url:
                    return frame
            await asyncio.sleep(0.3)

        raise TimeoutError("等待钱包 iframe 超时")

    async def goto_month_left_panel(self,frame, year: int, month: int):
        left_panel = frame.locator(".arco-panel-date").nth(0)
        header = left_panel.locator(".arco-picker-header-value")

        for _ in range(24):
            text = (await header.inner_text()).strip()
            y, m = map(int, text.split("-"))

            if y == year and m == month:
                return

            if (y, m) < (year, month):
                await left_panel.locator(
                    ".arco-picker-header-icon:has(.arco-icon-right)"
                ).click()
            else:
                await left_panel.locator(
                    ".arco-picker-header-icon:has(.arco-icon-left)"
                ).click()

            await frame.wait_for_timeout(120)

        raise RuntimeError("左 panel 无法切换到目标月份")

    async def click_day_left_panel(self,frame, day: int):
        left_panel = frame.locator(".arco-panel-date").nth(0)

        await left_panel.locator(
            ".arco-picker-cell-in-view .arco-picker-date-value",
            has_text=str(day)
        ).first.click()

    async def select_range_left_only(
            self,
            frame,
            start_date: str,
            end_date: str
    ):
        from datetime import datetime

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # 打开日历
        await frame.locator(".arco-picker-range-wrapper").click()
        await frame.wait_for_selector(".arco-panel-date")

        # 👉 开始日期（左 panel）
        await self.goto_month_left_panel(frame, start.year, start.month)
        await self.click_day_left_panel(frame, start.day)

        # 👉 结束日期（仍然左 panel）
        await self.goto_month_left_panel(frame, end.year, end.month)
        await self.click_day_left_panel(frame, end.day)

    async def download_withdrawal(self):
        await self.page.goto('https://seller.tiktokshopglobalselling.com/seller-wallet/full-service?shop_region=GB',wait_until="domcontentloaded",
                timeout=20_000)

        self.logger.info("等待钱包 iframe 加载...")
        frame = await self.wait_wallet_frame(self.page)

        # 等待输入出现
        # 等 RangePicker 容器出现


        # 再点搜索
        # await wallet_frame.get_by_role("button", name="搜索").click()

        await asyncio.sleep(5)

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

        if not await self.download_withdrawal():
            raise Exception("download_withdrawal")

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


async def main():
    name_list = ["TK全托1401店", "TK全托408-LXZ", "TK全托407-huidan", "TK全托406-yuedongwan", "TK全托405-huanchuang",
                 "TK全托404-kedi", "TK全托403-juyule", "TK全托401-xiyue", "TK全托402-quzhi", "TK全托1402店"]
    for name in name_list:
        account = get_shop_config(name)

        t = TKLogin(name, account)
        await t.run()


if __name__ == "__main__":
    asyncio.run(main())