from playwright.async_api import async_playwright
import json
from pathlib import Path
from datetime import datetime,timedelta
import requests
import asyncio
from utils.logger import get_logger
from utils.cookie_manager import get_shop_config
import math
import re
from utils.dingtalk_bot import ding_bot_send

FINANCIAL_DIR = Path(__file__).resolve().parent.parent / "data" / "financial" /"smt"
FINANCIAL_DIR.mkdir(parents=True, exist_ok=True)

class SMT_FinancialData:
    def __init__(self, shop_name,month_str):
        self.month_str=month_str
        self.shop_name = shop_name
        cfg = get_shop_config(shop_name)
        self.channel_id = cfg["channelId"]
        self.cloud_account_id = cfg["cloud_account_id"]
        self.username = cfg["account"]
        self.password = cfg["password"]

        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        self.logger = get_logger("financial_data")

    # ----------- 浏览器 -----------
    def start_cloud_browser(self) -> str:
        url = "http://localhost:50213/api/v2/browser/start"
        resp = requests.get(
            url,
            params={"account_id": self.cloud_account_id},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()["data"]["ws"]['puppeteer']

    def stop_cloud_browser(self):
        """
        通过云浏览器 API 关闭浏览器实例
        """
        url = "http://localhost:50213/api/v2/browser/stop"
        try:
            resp = requests.get(
                url,
                params={"account_id": self.cloud_account_id},
                timeout=10
            )
            resp.raise_for_status()
            print(f"[{self.shop_name}] 云浏览器已关闭")
        except Exception as e:
            print(f"[{self.shop_name}] 关闭云浏览器失败: {e}")

    async def start_browser(self):
        ws_endpoint = self.start_cloud_browser()
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.connect_over_cdp(ws_endpoint)
        self.context = self.browser.contexts[0]
        self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()
        await self.page.bring_to_front()

    # ----------- 业务 ----------
    async def login(self) -> bool:
        if not self.page:
            raise RuntimeError("browser not started, call start_browser() first")

        login_url = (
            "https://login.aliexpress.com/user/seller/login"
            f"?bizSegment=CSP&channelId={self.channel_id}"
        )

        if "login" not in self.page.url:
            await self.page.goto(login_url, wait_until="domcontentloaded")

        # 输入账号
        user_input = self.page.locator('#loginName')
        await user_input.wait_for(state='visible', timeout=15_000)
        await user_input.fill(self.username)

        # 输入密码
        password_input = self.page.locator('#password')
        await password_input.wait_for(state='visible', timeout=15_000)
        await password_input.fill(self.password)

        await self.page.click('button[type="button"]:has-text("登录")')

        try:
            await self.page.wait_for_url("**/m_apps/**", timeout=300000)
            self.logger.info(f"{self.shop_name} 登录成功")
            return True
        except Exception:
            self.logger.error(f"{self.shop_name} 登录超时")
            return False

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
        }

    # 输入日期
    async def pick_date_range_by_input(
            self,
            start_date: str,
            end_date: str,
    ):
        """
        通过输入框方式选择 Ant Design RangePicker 日期
        start_date / end_date 格式：YYYY-MM-DD
        """

        # 1️⃣ 等待 loading 消失（非常关键）
        await self.page.wait_for_selector(
            '.ant-spin-spinning',
            state='detached',
            timeout=30_000
        )

        # 2️⃣ 点击整个日期范围选择框（不是 svg）
        picker = self.page.locator('div.ant-picker-range')
        await picker.wait_for(state="visible", timeout=20_000)
        await picker.scroll_into_view_if_needed()
        await picker.click(force=True)

        # 3️⃣ 定位两个 input
        inputs = self.page.locator('div.ant-picker-range input')
        await inputs.first.wait_for(state="visible", timeout=20_000)

        start_input = inputs.nth(0)
        end_input = inputs.nth(1)

        # 4️⃣ 输入开始日期
        await start_input.click(force=True)
        await start_input.fill(start_date)
        await start_input.press("Enter")

        # 给 AntD 一点反应时间
        await self.page.wait_for_timeout(300)

        # 5️⃣ 输入结束日期
        await end_input.click(force=True)
        await end_input.fill(end_date)
        await end_input.press("Enter")

        # 再给 AntD 一点时间触发内部 onChange
        await self.page.wait_for_timeout(500)

    ### 1.历史账单导出
    async def get_history_bill(self):
        self.logger.info(f"{self.shop_name} - 开始获取财务动账数据")

        # 1️⃣ 进入 AliExpress 结算管理
        await self.page.goto(
            f"https://csp.aliexpress.com/m_apps/funds-manage/financial_aechoice?channelId={self.channel_id}",
            wait_until="domcontentloaded",
            timeout=20_000
        )

        # 2️⃣ 点击【提现】
        withdraw_btn = self.page.get_by_role("button", name="提现")
        await withdraw_btn.wait_for(state="visible", timeout=15_000)

        if not await withdraw_btn.is_enabled():
            raise Exception("提现按钮不可点击")

        # 3️⃣ 捕获新页面（Alipay）
        async with self.context.expect_page() as p:
            await withdraw_btn.click()

        new_page = await p.value
        await new_page.wait_for_load_state("domcontentloaded")

        # 4️⃣ 切换 page
        self.page = new_page

        # 5️⃣ 等待跳转到最终 Alipay 业务页（非常关键）
        await self.page.wait_for_url(
            "**/global.alipay.com/**",
            timeout=30_000
        )

        self.logger.info(f"已切换到 Alipay 页面: {self.page.url}")


        # 点击账务动账
        bill_menu = self.page.locator(
            'a.abMenu-item:has-text("账务动账")'
        )

        await bill_menu.wait_for(state="visible", timeout=20_000)
        await bill_menu.click()

       # 输入日期
        date_range=self.get_month_date_range(self.month_str)
        start_date = date_range["start_date"]
        end_date = date_range["end_date"]
        await self.pick_date_range_by_input(
            start_date=start_date,
            end_date=end_date
        )

        # 给 AntD 一点时间触发查询
        await self.page.wait_for_timeout(500)

        # 点击搜索
        search_btn=self.page.locator('.ant-btn.ant-btn-primary')
        await search_btn.wait_for(state="visible", timeout=20_000)
        await search_btn.click()

        await asyncio.sleep(1)

        # 点击下载搜索结果
        download_btn=self.page.locator('.downloadSearchBtn___3NvAY')
        await download_btn.wait_for(state="visible", timeout=20_000)

        file_name = f"{self.shop_name}_{self.month_str.split('-')[1]}_财务动账.zip"
        save_path = FINANCIAL_DIR / file_name

        async with self.page.expect_download(timeout=60_000) as download_info:
            await download_btn.click()

        download = await download_info.value
        await download.save_as(save_path)

        self.logger.info(
            f"{self.shop_name} - 财务动账下载完成: {save_path}"
        )
        return True


    # 计算日期所在的位置
    async def click_month_by_number(self, month: int):
        # 1️⃣ 算 row（你已经验证过）
        row = math.ceil(month / 3) + 1

        # 2️⃣ 在这一行里，用文本找 month
        month_locator = self.page.locator(
            f".next-row:nth-child({row}) .next-btn-helper:nth-child(1)",
            has_text=str(month)
        )

        await month_locator.wait_for(state="visible", timeout=10_000)
        await month_locator.scroll_into_view_if_needed()
        await month_locator.click(force=True)

    # 点击年份与月份
    async def pick_month_by_str(self, month_str: str):
        """
        month_str: '2025-12'
        """
        target_year, target_month = map(int, month_str.split("-"))

        # 1️⃣ 打开日历（不用 nth-child，点输入框里的 icon）
        calendar_icon = self.page.locator(
            "div:nth-child(8) .next-input-inner .next-icon"
        )
        await calendar_icon.wait_for(state="visible", timeout=20_000)
        await calendar_icon.click()

        # 2️⃣ 切换到【月】视图（按钮文本是“月”）
        month_tab = self.page.locator(
            ".next-btn:nth-child(3) > span"
        )

        await month_tab.wait_for(state="visible", timeout=20_000)
        await month_tab.click()

        # 3️⃣ 锁定【当前日历面板】——通过“202X年”
        # 获取当前的年份，与我们想要的年份进行对比
        year_label = self.page.locator('div:nth-child(1) > div:nth-child(1) > span:nth-child(2)')

        await year_label.wait_for(state="visible", timeout=10_000)

        # 先把年份切对
        while True:
            text = await year_label.inner_text()
            year = int(text.replace("年", "").strip())

            if year == target_year:
                break

            if year > target_year:
                await self.page.locator('i.next-icon-arrow-double-left').click()
            else:
                await self.page.locator('i.next-icon-arrow-double-right').click()

            await self.page.wait_for_timeout(200)

        # 年份对了再点月份
        await self.click_month_by_number(target_month)

        # await asyncio.sleep(2)

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

    ### 2.其他收支结算导出
    async def get_other_bill(self):
        self.logger.info(f"{self.shop_name} - 开始获取其他收支结算数据")

        await self.page.goto(
            f"https://csp.aliexpress.com/m_apps/funds-manage/financial_aechoice?channelId={self.channel_id}",
            wait_until="domcontentloaded",
            timeout=20_000
        )

        # 等页面主区域稳定（防止刚进页面就点）
        withdraw_btn = self.page.get_by_role("button", name="提现")

        await withdraw_btn.wait_for(state="visible", timeout=15_000)

        # 选择日期
        await self.pick_month_by_str(self.month_str)

        # await asyncio.sleep(3)

        # 点击导出明细
        export_btn=self.page.locator('div:nth-child(8) > .finacleExport:nth-child(2)')
        await export_btn.wait_for(state="visible", timeout=15_000)
        await export_btn.click()

        # 等待导出成功
        await self.wait_export_success_toast()

        # 点击下线
        download_btn=self.page.locator('.first .next-btn-helper')
        await download_btn.wait_for(state="visible", timeout=15_000)

        file_name = f"{self.shop_name}_{self.month_str.split('-')[1]}_其他收支结算.xlsx"
        save_path = FINANCIAL_DIR / file_name

        async with self.page.expect_download(timeout=60_000) as download_info:
            await download_btn.click()

        download = await download_info.value
        await download.save_as(save_path)

        self.logger.info(
            f"{self.shop_name} - 其他收支结算下载完成: {save_path}"
        )

        return True


    # -----------失败可以重新登录------------
    async def run_once(self):
        try:
            # 必须先启动
            await self.start_browser()

            if not await self.login():
                raise Exception("登录失败")


            # 其他收支结算
            if not await self.get_other_bill():
                raise Exception('获取其他收支结算失败')

            # 历史账单
            if not await self.get_history_bill():
                raise Exception("获取动账财务数据失败")

        finally:
            # ✅ 无论成功失败，统一关
            await self.close()

    # ----------- 总流程 -----------
    async def run(self, max_retry=3):
        self.logger.info(f"--------------------{self.shop_name} ------------------------ 开始登录...")
        for attempt in range(1, max_retry + 1):
            try:
                self.logger.info(f"{self.shop_name} - 第 {attempt} 次登录尝试")
                await self.run_once()
                self.logger.info(f"{self.shop_name} - 登录成功")
                return True
            except Exception as e:
                self.logger.error(f"{self.shop_name} - 第 {attempt} 次失败: {e}", exc_info=True)
                self.stop_cloud_browser()
                if attempt < max_retry:
                    await asyncio.sleep(3)

        self.logger.error(f"{self.shop_name} - 登录失败，已达到最大重试次数 {max_retry}")
        ding_bot_send('me',f"{self.shop_name} - financial任务登录失败，已达到最大重试次数 {max_retry}")
        return False

    async def close(self):
        try:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        finally:
            self.stop_cloud_browser()


# async def main():
#     shop_name_list = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']
#     month_str='2025-12'
#     for name in shop_name_list:
#         t = SMT_FinancialData(name,month_str)
#         await t.run()
#
#
# if __name__ == "__main__":
#     asyncio.run(main())

