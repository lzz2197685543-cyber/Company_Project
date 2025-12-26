import json
import asyncio
import requests
from playwright.async_api import async_playwright
import os
from datetime import datetime
from utils.config_loader import get_shop_config
from utils.logger import get_logger
from pathlib import Path
import time

FINANCIAL_DIR = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "financial"
)
FINANCIAL_DIR.mkdir(parents=True, exist_ok=True)



class TKLoginDownloadData:
    start_api = "http://127.0.0.1:6873/api/v1/browser/start"
    stop_api = "http://127.0.0.1:6873/api/v1/browser/stop"

    def __init__(self, name, account,start_date,end_date):
        self.name = name
        self.hub_id = str(account["hubId"])
        cred = account["credentials"]
        self.username = cred["username"]
        self.password = cred["password"]

        self.logger = get_logger(f"login")

        self.start_date = start_date
        self.end_date = end_date
        self.month_str = end_date[:7]
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

            # self.logger.info(f"{self.name} - start_api 返回: {res}")

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

    async def clear_arco_range_if_needed(self,frame):
        """
        如果 RangePicker 里已有值，先点击清空叉号
        """
        clear_icon = frame.locator(".arco-picker-clear-icon")

        if await clear_icon.count() > 0:
            try:
                await clear_icon.first.click(force=True)
                await frame.wait_for_timeout(300)  # 等内部状态更新
            except Exception:
                pass

    async def input_arco_range(self,frame):
        # 等 RangePicker 出现
        await frame.wait_for_selector(".arco-picker-range")

        # 开始日期
        start_input = frame.locator(
            '.arco-picker-range input[placeholder="开始日期"]'
        )
        await start_input.wait_for(state="visible")
        await start_input.click()
        await start_input.fill(self.start_date)
        await frame.wait_for_timeout(150)

        # 结束日期
        end_input = frame.locator(
            '.arco-picker-range input[placeholder="结束日期"]'
        )
        await end_input.click()
        await end_input.fill(self.end_date)

        # 触发 Arco 内部校验（关键）
        await end_input.press("Enter")
        await frame.wait_for_timeout(300)

    async def select_trade_type(self, frame, value: str):
        """
        Arco Select - 交易类型选择（稳定版）
        """

        select_input = frame.locator(".arco-select-view-input").nth(0)

        # 1️⃣ 等待并点击
        await select_input.wait_for(state="visible", timeout=10_000)
        await select_input.click()

        # 2️⃣ 清空旧值（非常关键）
        await select_input.press("Control+A")
        await select_input.press("Backspace")

        # 3️⃣ 输入新值
        await select_input.fill(value)

        # 4️⃣ 回车确认（触发真正选中）
        await select_input.press("Enter")

        self.logger.info(f"交易类型已选择（Enter）：{value}")

    async def export_and_save(self, frame, trade_type_name):
        """
        点击导出并保存文件到 data/financial
        """
        filename = f"{self.name}_{self.month_str}_{trade_type_name}.xlsx"
        save_path = FINANCIAL_DIR / filename

        self.logger.info(f"准备导出文件：{filename}")

        async with self.page.expect_download(timeout=30_000) as download_info:
            export_btn = frame.locator('button:has-text("导出")')
            await export_btn.wait_for(state="visible", timeout=10_000)
            await export_btn.click()

        download = await download_info.value
        await download.save_as(save_path)

        self.logger.info(f"文件已保存：{save_path}")

    async def search_and_export_by_trade_type(self,frame,trade_type_value):
        """
        选择交易类型 → 搜索 → 导出
        """
        self.logger.info(f"开始处理交易类型：{trade_type_value}")

        # 1️⃣ 选择交易类型
        await self.select_trade_type(frame, trade_type_value)

        # 2️⃣ 点击搜索
        await frame.locator('button:has-text("搜索")').click()

        # 等表格刷新（比 sleep 稳定）
        await frame.wait_for_selector("tbody", timeout=10_000)

        rows = self.page.locator("tbody tr")
        row_count = await rows.count()

        if row_count <= 1:
            self.logger.info(
                f"{self.name} - 无费用明细数据，跳过导出"
            )
            return True  # ⭐ 核心：直接结束

        # 3️⃣ 导出
        await self.export_and_save(frame, trade_type_value)

    async def reload_wallet_page(self):
        self.logger.info("刷新钱包页面...")
        await self.page.reload(wait_until="domcontentloaded", timeout=20_000)

        frame = await self.wait_wallet_frame(self.page)
        return frame

    async def process_trade_type(self, frame, trade_type_value: str):
        self.logger.info(f"开始处理交易类型：{trade_type_value}")

        # 1️⃣ 日期
        await self.clear_arco_range_if_needed(frame)
        await self.input_arco_range(frame)

        # 2️⃣ 交易类型 + 搜索 + 导出
        await self.search_and_export_by_trade_type(
            frame,
            trade_type_value=trade_type_value
        )

    """提现交易+保证金罚扣的导出"""
    async def export_wallet_financial_records(self):
        await self.page.goto(
            "https://seller.tiktokshopglobalselling.com/seller-wallet/full-service?shop_region=GB",
            wait_until="domcontentloaded",
            timeout=20_000
        )

        self.logger.info("等待钱包 iframe 加载...")
        frame = await self.wait_wallet_frame(self.page)

        # 1️⃣ 提现交易
        await self.process_trade_type(frame, "提现交易")

        # 2️⃣ 刷新页面（彻底重置 Arco 状态）
        frame = await self.reload_wallet_page()

        # 3️⃣ 保证金罚扣
        await self.process_trade_type(frame, "保证金罚扣")

        return True

    async def input_fee_date_range(self):
        # 等 RangePicker 出现
        await self.page.wait_for_selector(".arco-picker-range", timeout=10_000)

        inputs = self.page.locator(".arco-picker-range input")

        # 开始日期
        await inputs.nth(0).click()
        await inputs.nth(0).fill(self.start_date)

        # 结束日期
        await inputs.nth(1).click()
        await inputs.nth(1).fill(self.end_date)

        # 触发 Arco 内部校验（非常重要）
        await inputs.nth(1).press("Enter")

        await self.page.wait_for_timeout(300)

    """费用明细"""
    async def fetch_fee_center_details(self):
        await self.page.goto(
            "https://seller.tiktokshopglobalselling.com/finance-settlement/fee-center/detail",
            wait_until="domcontentloaded",
            timeout=20_000
        )

        # 1️⃣ 等表格真正出现（微前端 ready）
        await self.page.wait_for_selector(
            ".theme-arco-table",
            timeout=20_000
        )

        # 2️⃣ 清空并输入日期
        await self.input_fee_date_range()

        # 3️⃣ 点击搜索
        search_btn = self.page.locator(
            ".arcoS-search-form-buttons button.arco-btn-primary"
        )
        await search_btn.wait_for(state="visible", timeout=10_000)
        await search_btn.click(force=True)

        # 等搜索结果刷新
        await self.page.wait_for_timeout(1_500)

        rows = self.page.locator("tbody tr")
        row_count = await rows.count()

        if row_count <=1:
            self.logger.info(
                f"{self.name} - 无费用明细数据，跳过导出"
            )
            return True  # ⭐ 核心：直接结束

        export_btn = self.page.locator('button:has-text("导出数据")')
        await export_btn.wait_for(state="visible", timeout=10_000)
        await export_btn.click(force=True)

        self.logger.info(
            f"{self.name} - 费用明细导出任务已提交（{self.start_date} ~ {self.end_date}）"
        )

        return True

    async def download_latest_finished_task(self,save_path: Path,timeout: int = 300):
        await self.page.goto(
            "https://seller.tiktokshopglobalselling.com/download-center?shop_region=GB",
            wait_until="domcontentloaded",
            timeout=20_000
        )

        start = time.time()

        while time.time() - start < timeout:
            rows = self.page.locator("tbody tr")
            count = await rows.count()

            if count == 0:
                await asyncio.sleep(3)
                await self.page.reload()
                continue

            # 第一行 = 最新任务
            row = rows.first
            status = await row.locator("td").nth(4).inner_text()

            if "已完成" in status:
                download_btn = row.locator("td").nth(5).locator("text=下载")

                async with self.page.expect_download(timeout=30_000) as d:
                    await download_btn.click()

                download = await d.value
                await download.save_as(save_path)

                self.logger.info(f"文件已保存为：{save_path}")
                return True

            self.logger.info("最新任务未完成，等待中...")
            await asyncio.sleep(3)
            await self.page.reload()

        raise TimeoutError("下载中心等待任务完成超时")

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

        if not await self.fetch_fee_center_details():
            raise Exception('fetch_fee_center_details 失败')

        # 2️⃣ 去下载中心，等待并下载最新完成任务
        save_path = (
                FINANCIAL_DIR
                / f"{self.name}_{self.month_str}_费用明细.xlsx"
        )

        if not await self.download_latest_finished_task(save_path):
            raise Exception("download_latest_finished_task 失败")

        if not await self.export_wallet_financial_records():
            raise Exception("export_wallet_financial_records 失败")

        return True


    # ----------- 总流程 -----------
    async def run(self, max_retry=3):
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
#     start_date = "2025-11-01"
#     end_date = "2025-12-26"
#     name_list = ["TK全托1401店", "TK全托408-LXZ", "TK全托407-huidan", "TK全托406-yuedongwan", "TK全托405-huanchuang",
#                  "TK全托404-kedi", "TK全托403-juyule", "TK全托401-xiyue", "TK全托402-quzhi", "TK全托1402店"]
#     for name in name_list:
#         account = get_shop_config(name)
#
#         t = TKLoginDownloadData(name, account,start_date,end_date)
#         await t.run()
#
#
# if __name__ == "__main__":
#     asyncio.run(main())