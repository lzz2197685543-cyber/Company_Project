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
# 文件保存目录
FINANCIAL_DIR = Path(__file__).resolve().parent.parent / "data" / "financial" / "temu"
FINANCIAL_DIR.mkdir(parents=True, exist_ok=True)

"""跑temu财务数据"""

class Temu_Financial_Data:
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

    async def login_verify(self):
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

                    # 再次输入账号和密码，因为有的环境多个账号在使用，所以我们需要再次填写账号跟密码
                    await auth_page.fill("#usernameId", "")  # 可选：先清一次
                    await auth_page.fill("#usernameId", self.username)

                    await auth_page.fill("#passwordId", "")
                    await auth_page.fill("#passwordId", self.password)

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


        #  等页面真正“稳定”
        await self.page.wait_for_load_state("load")

        await asyncio.sleep(3)



        return True

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

    # 我这个日历只处理一个月的情况
    async def process_calendar(self):
        """
        选择目标年月（self.month_str: 'YYYY-MM'）的日期范围
        """
        start_date=self.get_month_date_range(self.month_str)['start_date']
        end_date=self.get_month_date_range(self.month_str)['end_date']

        target_year, target_month = map(int, self.month_str.split("-"))

        # 等待日历面板出现
        await self.page.wait_for_selector('div.RPR_outerPickerWrapper_5-117-0', timeout=5000)

        # ------------------ 选择年份 ------------------
        while True:
            # 获取当前显示的年份
            year_text = await self.page.locator(
                '.IPT_inputBlock_5-117-0 .IPT_mirror_5-117-0'
            ).nth(1).text_content()
            current_year = int(year_text.replace("年", "").strip())

            if current_year == target_year:
                break

            # 点击年份下拉
            await self.page.locator('.IPT_suffixWrapper_5-117-0 span').nth(1).click()
            await self.page.wait_for_timeout(200)

            # 点击目标年份
            year_li_locator = self.page.locator(
                f'ul[data-testid="beast-core-rangePicker-select-list-0"] li span:text-is("{target_year}年")'
            )
            await year_li_locator.click()

            # 等待目标年份被选中
            selected_li = self.page.locator(
                f'ul[data-testid="beast-core-rangePicker-select-list-0"] li[data-checked="true"] span:text-is("{target_year}年")'
            )
            await selected_li.wait_for(state="visible", timeout=5000)

        # ------------------ 选择月份 ------------------
        month_locator = self.page.locator('.RPR_dateText_5-117-0:visible').first
        previous_month = None

        while True:
            month_text = await month_locator.text_content()
            current_month = int(month_text.replace("月", "").strip())

            if current_month == target_month:
                break

            if previous_month == current_month:
                # 如果上次和本次相同，强制等待 DOM 更新
                await asyncio.sleep(0.3)

            previous_month = current_month

            if current_month > target_month:
                await self.page.locator('.RPR_panelHeader_5-117-0 svg').nth(0).click()
            else:
                await self.page.locator('.RPR_panelHeader_5-117-0 svg').nth(1).click()

            await month_locator.wait_for(state="visible")

        # ------------------ 选择日期 ------------------
        start_day=int(start_date.split('-')[-1])
        end_day=int(end_date.split('-')[-1])

        # 定位日历 tbody
        tbody_locator = self.page.locator(
            'div.RPR_tableWrapper_5-117-0 table[data-testid="beast-core-rangePicker-table"] tbody'
        ).nth(0)
        await tbody_locator.wait_for(state="visible", timeout=5000)

        # 点击开始日期
        start_locator = tbody_locator.locator(
            f'div.RPR_cell_5-117-0[title="{start_day}"]:not(.RPR_outOfMonth_5-117-0):not(.RPR_inRange_5-117-0)'
        ).first
        await start_locator.click()

        await asyncio.sleep(0.3)

        # 点击结束日期
        end_locator = tbody_locator.locator(
            f'div.RPR_cell_5-117-0[title="{end_day}"]:not(.RPR_outOfMonth_5-117-0):not(.RPR_inRange_5-117-0)'
        ).first
        await end_locator.click()

        await asyncio.sleep(0.3)

        # 点击“确认”按钮
        confirm_btn = self.page.locator(
            'button[data-testid="beast-core-button"] >> text=确认'
        )
        await confirm_btn.click()

    # 等待加载出来，并下载
    async def load_and_download(self):
        # 等待导出历史列表出现
        history_list=self.page.locator('.export-history_list__5Eto0').first
        await history_list.wait_for(state="visible", timeout=80_000)

        # 取第一个导出记录
        first_item=history_list.locator('.export-history_right__YGHPV div').first

        # ===== 1. 下载卖家中心文件（在当前页面直接下载）=====
        self.logger.info(f"{self.name} - 准备下载: 下载账务明细(卖家中心)")

        filename = f"{self.name}_{self.month_str.split('-')[1]}_卖家中心.xlsx"
        final_path = FINANCIAL_DIR / filename

        # ⭐ 已存在直接跳过
        if final_path.exists():
            self.logger.info(f"⏭️ 文件已存在，跳过下载: {final_path}")
        else:
            download_seller_span = first_item.locator(
                'span:has-text("下载账务明细(卖家中心)")'
            ).first
            await download_seller_span.wait_for(state="visible", timeout=80_000)

            async with self.page.expect_download(timeout=50_000) as download_info:
                await download_seller_span.click()

            download = await download_info.value
            await download.save_as(final_path)
            self.logger.info(f"✅ 文件下载完成: {final_path}")

        await asyncio.sleep(1)

        # ===== 2. 下载全球/欧区/美国文件（需要打开新页面）=====
        async def download_with_new_page(selector_text, file_prefix):
            """
            专门处理需要打开新页面的下载
            """
            filename = f"{self.name}_{self.month_str.split('-')[1]}_{file_prefix}.xlsx"
            final_path = FINANCIAL_DIR / filename

            # ⭐ 已存在直接跳过
            if final_path.exists():
                self.logger.info(f"⏭️ 文件已存在，跳过下载: {final_path}")
                return True

            self.logger.info(f"{self.name} - 准备下载: {selector_text}")

            download_element = first_item.locator(
                f'span:has-text("{selector_text}")'
            ).first
            await download_element.wait_for(state="visible", timeout=80_000)

            pages_before = len(self.page.context.pages)

            await download_element.click()
            await asyncio.sleep(2)

            pages = self.page.context.pages
            new_page = pages[-1]
            await new_page.bring_to_front()

            try:
                await new_page.wait_for_load_state("networkidle")

                async with new_page.expect_download(timeout=80_000) as download_info:
                    pass

                download = await download_info.value
                await download.save_as(final_path)
                self.logger.info(f"✅ 文件下载完成: {final_path}")
                return True

            except Exception as e:
                self.logger.error(f"新页面下载失败: {str(e)}")
                return False

            finally:
                await new_page.close()
                await self.page.bring_to_front()
                await asyncio.sleep(1)

        # 下载其他三个文件
        download_tasks = [
            ("下载财务明细(全球)", "全球"),
            ("下载财务明细(欧区)", "欧区"),
            ("下载财务明细(美国)", "美国")
        ]

        results = []
        for selector_text, file_prefix in download_tasks:
            result = await download_with_new_page(selector_text, file_prefix)
            results.append(result)

        # 返回总体结果
        all_success = all(results)

        if all_success:
            self.logger.info(f"✅ {self.name} - 所有财务数据下载完成")
        else:
            self.logger.warning(f"⚠️ {self.name} - 部分财务数据下载失败")

        return all_success

    # 查询，导出，下载
    async def search_export_download(self):
        # 1.点击“查询”按钮
        query_btn = self.page.locator(
            'div[data-testid="beast-core-grid-col-wrapper"] button[data-testid="beast-core-button"] >> text=查询'
        )
        await query_btn.click()

        await asyncio.sleep(1.2)

        # 2.判断是否有数据
        # 定位元素
        total_text_locator = self.page.locator('li.PGT_totalText_5-117-0')
        await total_text_locator.wait_for(state="visible", timeout=5000)

        # 获取文本，例如 "共有 43 条"
        total_text = await total_text_locator.text_content()
        total_text = total_text.strip()

        # 提取数字
        match = re.search(r'(\d+)', total_text)
        total_count = int(match.group(1)) if match else 0

        # 判断是否有数据
        if total_count > 0:
            self.logger.info(f"有数据，总共 {total_count} 条")
        else:
            self.logger.info("没有数据,直接跳过，不导出了")
            return True

        # 3.点击“导出”按钮
        export_btn = self.page.locator(
            'div[data-testid="beast-core-box"] button[data-testid="beast-core-button"] >> text=导出'
        )
        await export_btn.click()

        await asyncio.sleep(0.3)

        # 4.选择“导出列表 + 账务详情”
        radio = self.page.locator(
            'div[data-testid="beast-core-grid-col-wrapper"] label >> text=导出列表 + 账务详情'
        )
        await radio.click()

        await asyncio.sleep(0.3)

        # 5.点击确认按钮
        confirm_btn = self.page.locator(
            'div[style*="text-align: right"] button'
        ).nth(0)
        await confirm_btn.click()

        await asyncio.sleep(0.3)

        # 检查是否已创建导出任务
        # 检查是否有可下载的数据
        toast = self.page.get_by_text(
            '数据导出成功',
            exact=False
        )

        try:
            # 等待 toast 出现，表示有可下载数据
            await toast.first.wait_for(state="visible", timeout=12000)
            self.logger.info("检测到导出数据成功，准备下载报表")

            # 继续下载所有报表
            await self.load_and_download()

            self.logger.info("全部报表下载完成")

        except Exception:
            # 没有检测到 toast，说明没有数据
            self.logger.info("未检测到可导出数据，结束操作，不下载报表")
            ding_bot_send('me',f"{self.name}---未检测到可导出数据，结束操作，不下载报表")
            return

    ### 下载财务数据
    async def download_financial_data(self):
        self.logger.info(f"{self.name} - 进入财务页面")

        await self.page.goto(
            "https://seller.kuajingmaihuo.com/labor/bill",
            wait_until="domcontentloaded",
            timeout=20000
        )

        # 等待页面加载完成
        await self.page.wait_for_load_state("networkidle")

        date_input = self.page.locator('[data-testid="beast-core-rangePicker-htmlInput"]')
        await date_input.wait_for(state="visible", timeout=10_000)
        await date_input.click()

        # 选择日期
        await self.process_calendar()
        await asyncio.sleep(0.3)

        # 查询，导出，下载
        await self.search_export_download()

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

        if not await self.login_verify():
            raise Exception("login_verify 失败")

        if not await self.download_financial_data():
            raise Exception('download_financial_data 失败')

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
        ding_bot_send('me',f"{self.name} - 在financial任务中登录失败，已达到最大重试次数 {max_retry}")
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
    name_list = ['102-Temu全托管']
    month_str='2025-2'
    for name in name_list:
        account = get_shop_config(name)
        # print(account)

        t = Temu_Financial_Data(name, account,month_str)
        await t.run()


# if __name__ == "__main__":
#     asyncio.run(main())



