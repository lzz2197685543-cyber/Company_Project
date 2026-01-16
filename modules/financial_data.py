import json
import asyncio
import requests
from playwright.async_api import async_playwright
import os
from datetime import datetime,timedelta
from utils.config_loader import get_shop_config
from utils.logger import get_logger
from pathlib import Path
from utils.dingtalk_bot import ding_bot_send
import re




class ShopeeLogin_FinancialData:
    start_api = "http://127.0.0.1:6873/api/v1/browser/start"
    stop_api = "http://127.0.0.1:6873/api/v1/browser/stop"

    def __init__(self, name, account,month_str):
        self.month_str = month_str
        self.name = name
        self.hub_id = str(account["hubId"])
        cred = account["credentials"]
        self.username = cred["username"]
        self.password = cred["password"]

        self.logger = get_logger(f"financial_data")
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

    # ----------- 业务 ----------
    async def login(self):
        self.logger.info(f"{self.name} - 开始执行登录流程")
        await self.page.goto(
            "https://seller.scs.shopee.cn/login",
            wait_until="domcontentloaded",
            timeout=20_000
        )

        # 输入账号
        username_input = self.page.get_by_role("textbox", name="请输入").first
        await username_input.wait_for(state="visible", timeout=15_000)
        await username_input.click()
        await username_input.fill(self.username)

        # 输入密码
        password_input = self.page.locator("input[type='password']")
        await password_input.wait_for(state="visible", timeout=15_000)
        await password_input.click()
        await password_input.fill(self.password)

        # 点击登录
        login_btn = self.page.get_by_role("button", name="登录", exact=True)
        await login_btn.wait_for(state="visible", timeout=15_000)
        await login_btn.click()

        await self.page.wait_for_url(
            "**/home/**",
            timeout=20_000
        )

        self.logger.info(f"{self.name} - 登录成功")
        return True

    # 清除弹框
    async def close_popup_after_countdown(self):
        close_btn = self.page.locator(
            'button.ssc-button.popup-button'
        ).first

        # 1️⃣ 等弹窗出现
        try:
            await close_btn.wait_for(state="visible", timeout=3000)
        except:
            return

        self.logger.info(f"{self.name} - 检测到倒计时弹窗，等待关闭按钮可点击")

        # 2️⃣ 等倒计时结束（按钮可点）
        await self.page.wait_for_function(
            """() => {
                const btn = document.querySelector('button.ssc-button.popup-button');
                return btn && !btn.classList.contains('ssc-btn-disabled') && !btn.disabled;
            }""",
            timeout=8000
        )

        # 3️⃣ 点击关闭
        await close_btn.click()

        # 4️⃣ 等弹窗隐藏（而不是 detached）
        await close_btn.wait_for(state="hidden", timeout=5000)

        self.logger.info(f"{self.name} - 弹窗已关闭")


    # 判断是否有数据
    async def has_statement_data(self) -> bool:
        page = self.page

        # 等表格区域出现
        table = page.locator("table").first
        await table.wait_for(state="visible", timeout=20_000)

        # 判断 tbody 行数
        rows = page.locator("tbody tr")
        count = await rows.count()

        if count < 3:
            self.logger.info(f"{self.name} - 当前条件下无对账单数据，跳过导出")
            return False

        self.logger.info(f"{self.name} - 检测到 {count} 条对账单数据")
        return True


    async def has_table_data_by_total(self) -> bool:
        """
        通过分页组件中的“总计”判断是否有数据
        """
        page = self.page

        total_text_locator = page.locator(
            ".ssc-react-pagination-total-text"
        ).first

        try:
            await total_text_locator.wait_for(
                state="visible",
                timeout=5_000
            )

            text = (await total_text_locator.inner_text()).strip()
            self.logger.info(f"分页总计文本: {text}")

            # 提取数字
            match = re.search(r"(\d+)", text)
            if not match:
                return False

            total = int(match.group(1))
            return total > 0

        except Exception:
            self.logger.warning("未找到分页总计文本，默认认为有数据")
            return True

    # 只填年月，然后自动获取到这个月的1号和最后一号
    def get_month_date_range(self, month_str: str) -> dict:
        year, month = map(int, month_str.split("-"))

        start = datetime(year, month, 1)
        if month == 12:
            end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        else:
            end = datetime(year, month + 1, 1) - timedelta(seconds=1)

        return {
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "start_datetime": start.strftime("%Y-%m-%d %H:%M:%S"),
            "end_datetime": end.strftime("%Y-%m-%d %H:%M:%S"),
        }

    # 设置日期
    async def set_statement_date_range(self,start_date: str,end_date: str,input_index: int = 0 ): # 默认等价于 .first
        page = self.page

        # ===== 清空日期（如果存在）=====
        clear_btn = page.locator(
            '.ssc-react-range-picker-trigger-extra-clear'
        ).nth(input_index)

        try:
            if await clear_btn.is_visible():
                await clear_btn.click()
                await page.wait_for_timeout(500)
        except Exception:
            pass

        # ===== 开始日期 =====
        start_input = page.get_by_role(
            "textbox",
            name="开始日期"
        ).nth(input_index)

        await start_input.wait_for(state="visible", timeout=15_000)
        await start_input.click()
        await start_input.fill(start_date)
        await start_input.press("Enter")

        # ===== 结束日期 =====
        end_input = page.get_by_role(
            "textbox",
            name="结束日期"
        ).nth(input_index)

        await end_input.wait_for(state="visible", timeout=15_000)
        await end_input.click()
        await end_input.fill(end_date)
        await end_input.press("Enter")

        self.logger.info(
            f"{self.name} - 日期区间已设置[{input_index}]: {start_date} ~ {end_date}"
        )

    # 等待导出完成
    async def wait_export_success_toast(self, timeout: int = 10_000) -> bool:
        page = self.page

        toast = page.get_by_text(
            re.compile(r"(Exported Successfully|导出成功|已成功|成功导出)"),
            exact=False
        )

        try:
            await toast.first.wait_for(state="visible", timeout=timeout)
            self.logger.info("导出成功（toast 已出现）")
            return True
        except Exception:
            self.logger.warning("未捕获到导出成功 toast")
            return False

    # 切换到每页100
    async def ensure_page_size_100(self, *, scope=None):
        """
        确保当前表格分页为 100 条/页（Shopee 专用稳健版）
        :param scope: 可选，限定查找范围（如表格容器）
        """
        page = self.page
        root = scope or page

        # 1️⃣ 找到所有 selector
        selectors = root.locator(".ssc-react-select-selector")

        count = await selectors.count()
        if count == 0:
            self.logger.warning("未找到分页 selector，跳过 100 条/页设置")
            return

        for i in range(count):
            selector = selectors.nth(i)

            # 2️⃣ 只处理可见的
            if not await selector.is_visible():
                continue

            # 3️⃣ 已经是 100 条/页，直接跳过
            text = (await selector.inner_text()).strip()
            if "100" in text:
                self.logger.info("已是 100 条/页，跳过设置")
                return

            # 4️⃣ 点击 selector
            await selector.click()

            # 5️⃣ 等下拉框出现并点击 100
            option = page.get_by_text("100 条/页", exact=True)

            await option.wait_for(state="visible", timeout=5_000)
            await option.click()

            self.logger.info("已切换为 100 条/页")
            return

        self.logger.warning("未找到可见的分页 selector，跳过 100 条/页设置")

    # 查询、导出、下载
    async def export_common(self,table_row_name: str,export_btn_name: str,file_name: str,month_str: str,geshi:str):
        page = self.page

        # ===== 切换 100 条/页 =====
        table_container = page.locator(".ssc-react-table").first

        # 1️⃣ 稳定切换分页
        await self.ensure_page_size_100(scope=table_container)

        # ===== 查询 =====
        query_btn = page.get_by_role("button", name="查询").first
        await query_btn.wait_for(state="visible", timeout=10_000)
        await query_btn.click()

        # ===== 是否有数据 =====
        # if hasattr(self, "has_statement_data"):
        #     if not await self.has_statement_data():
        #         return True
        await page.wait_for_timeout(1000)

        # ===== 判断是否有数据 =====
        if not await self.has_table_data_by_total():
            self.logger.info("分页总计为 0，跳过导出")
            return True

        await page.wait_for_timeout(1000)

        # ===== 全选 =====
        checkbox_row = page.get_by_role(
            "row",
            name=table_row_name
        )
        await checkbox_row.click(position={"x": 20, "y": 20})

        # ===== 导出 =====
        export_btn = page.get_by_role("button", name=export_btn_name).first
        await export_btn.wait_for(state="visible", timeout=10_000)
        await export_btn.click()

        # ===== 等导出完成（toast）=====
        if hasattr(self, "wait_export_success_toast"):
            await self.wait_export_success_toast()

        await asyncio.sleep(1)

        # ===== 打开导出历史 =====
        history_btn = page.locator(
            ".src-common-component-export-history-index__export-button-wrapper--nElac"
        ).first
        await history_btn.wait_for(state="visible", timeout=10_000)
        await history_btn.click()

        # ===== 下载 =====
        async with page.expect_download() as download_info:
            async with page.expect_popup():
                download_btn = page.get_by_role("button", name="Download").first
                await download_btn.wait_for(state="visible", timeout=10_000)
                await download_btn.click()

        download = await download_info.value

        FINANCIAL_DIR = (
                Path(__file__).resolve().parent.parent
                / "data"
                / "financial"
                / (str(self.month_str.split("-")[1])+'月份')
                / "shopee"
        )
        FINANCIAL_DIR.mkdir(parents=True, exist_ok=True)

        save_path = FINANCIAL_DIR / f"{self.name}_{month_str}_{file_name}.{geshi}"
        await download.save_as(save_path)

        self.logger.info(f"{self.name} - 下载完成: {save_path}")
        return True

    ### 获取对账单数据
    async def statement_data(self):
        self.logger.info(f"{self.name} - 开始获取对账单数据")

        await self.page.goto(
            "https://seller.scs.shopee.cn/payment/payout-report/list",
            wait_until="domcontentloaded",
            timeout=20_000
        )

        # ===== 1️⃣ 处理倒计时弹窗 =====
        await self.close_popup_after_countdown()

        # ===== 2️⃣ 设置日期 =====
        date_range = self.get_month_date_range(self.month_str)

        start_date=date_range["start_date"]
        end_date=date_range["end_date"]

        await self.set_statement_date_range(
            start_date=start_date,
            end_date=end_date,
            input_index=0,
        )

        # ===== 3️⃣ 导出 =====
        await self.export_common(table_row_name="创建日期 对账单ID", export_btn_name="导出",month_str=self.month_str,file_name="对账单",geshi='xlsx')

        return True

    ### 获取付款单数据
    async def payment_data(self):
        self.logger.info(f"{self.name} - 开始获取对账单数据")

        await self.page.goto(
            "https://seller.scs.shopee.cn/payment/payment-request/list",
            wait_until="domcontentloaded",
            timeout=20_000
        )
        # ===== 1️⃣ 处理倒计时弹窗 =====
        await self.close_popup_after_countdown()

        # ===== 2️⃣ 设置日期 =====
        date_range = self.get_month_date_range(self.month_str)

        start_date = date_range["start_date"]
        end_date = date_range["end_date"]

        await self.set_statement_date_range(
            start_date=start_date,
            end_date=end_date,
            input_index=1
        )

        # ===== 3️⃣ 导出 =====
        await self.export_common(table_row_name="付款单ID Payee ID", export_btn_name="导出对账信息", month_str=self.month_str,
                                 file_name="付款单",geshi='zip')

        return True

    #### 获取调整单数据
    async def adjustment_data(self):
        self.logger.info(f"{self.name} - 开始获取调整单数据")

        await self.page.goto(
            "https://seller.scs.shopee.cn/payment/adjustment-list",
            wait_until="domcontentloaded",
            timeout=20_000
        )
        # ===== 1️⃣ 处理倒计时弹窗 =====
        await self.close_popup_after_countdown()

        # ===== 2️⃣ 设置日期 =====
        date_range = self.get_month_date_range(self.month_str)

        start_date = date_range["start_datetime"]
        end_date = date_range["end_datetime"]

        await self.set_statement_date_range(
            start_date=start_date,
            end_date=end_date,
            input_index=0
        )

        # ===== 3️⃣ 导出 =====
        await self.export_common(table_row_name="调整ID 状态 调整类型 调整原因 金额 (CNY", export_btn_name="导出",
                                 month_str=self.month_str,
                                 file_name="调整单", geshi='zip')

        return True


    # -----------失败可以重新登录------------
    async def run_once(self):
        if not await self.start_browser():
            raise Exception("start_browser 失败")

        if not await self.connect():
            raise Exception("connect 失败")

        if not await self.login():
            raise Exception("login 失败")

        if not await self.statement_data():
            raise Exception("对账单下载失败")

        if not await self.payment_data():
            raise Exception('付款单下载失败')

        if not await self.adjustment_data():
            raise Exception('调整单下载失败')

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
        ding_bot_send('me',f"{self.name} - financial任务登录失败，已达到最大重试次数 {max_retry}")
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
#     name_list = ["虾皮全托1501店", "虾皮全托507-lxz","虾皮全托506-kedi", "虾皮全托505-qipei",
#                  "虾皮全托504-huanchuang","虾皮全托503-juyule","虾皮全托502-xiyue","虾皮全托501-quzhi"]
#     name_list=["虾皮全托1501店"]
#     month_str="2025-12"
#     for name in name_list:
#         account = get_shop_config(name)
#
#         t = ShopeeLogin_FinancialData(name, account,month_str)
#         await t.run()
#
#
# if __name__ == "__main__":
#     asyncio.run(main())
#


