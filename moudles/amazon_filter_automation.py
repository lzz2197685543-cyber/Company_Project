from pathlib import Path
import asyncio
import playwright
import time
from utils.page_helpers import next_btn,close_btn
from datetime import datetime, timedelta
import os
import csv
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parent.parent / "data"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
# print(CONFIG_DIR)
class AmazonFilterAutomation:
    def __init__(self,page,logger):
        self.page=page
        self.logger=logger
        self.should_stop = False

    # ---------------- 接口监听 + 解析 ----------------
    async def wait_and_parse_goods_search(self, action, timeout=5000, interface_name="list"):
        """
        action: 一个 async lambda，里面只做一件事（点搜索 / 点下一页）
        interface_name: 要监听的接口类型，默认为 "list"
        """
        try:
            # 根据接口类型设置匹配条件
            if interface_name == "list":
                predicate = lambda r: (
                        "/analysis/list" in r.url
                        and "listSummary" not in r.url
                        and r.status == 200
                )
            elif interface_name == "listSummary":
                predicate = lambda r: (
                        "listSummary" in r.url
                        and r.status == 200
                )
            else:
                predicate = lambda r: (
                        interface_name in r.url
                        and r.status == 200
                )

            async with self.page.expect_response(predicate, timeout=timeout) as resp_info:
                await action()

            response = await resp_info.value
            json_data = await response.json()
            print(f"监听到 {interface_name} 接口响应")
            return self.parse_data(json_data)
        except TimeoutError:
            print(f"等待 {interface_name} 接口超时")
            return None

    # ---------------- 条件筛选 + 首次搜索 ----------------
    async def get_offer_filter(self):
        page = self.page
        await page.goto(
            "https://erp.lingxing.com/erp/msupply/replenishmentAdvice",  # 修正这里
            wait_until="domcontentloaded",
            timeout=30000
        )
        await asyncio.sleep(2)
        # 看看是否有下一条
        await next_btn(self.page)
        await asyncio.sleep(2)
        await close_btn(self.page)

        await asyncio.sleep(2)
        self.logger.info('开始筛选条件')

        # 将每页的数据换成200条/页
        select_count_btn=page.locator(
            "#ak-table-list > div.el-pagination > span.el-pagination__sizes > div > div > span.el-input__suffix > span > i")
        await select_count_btn.wait_for(state="visible", timeout=3000)
        await select_count_btn.click()

        await page.locator('text=200条/页').click()


        # 筛选我们要的店铺
        select_shop_btn=page.locator('#supplyApp > div > div.ak-search-wrapper > div:nth-child(5) > div.el-input.el-input--small.el-input--prefix.el-input--suffix > span.el-input__suffix > span > i')
        await select_shop_btn.wait_for(state="visible",timeout=3000)
        await select_shop_btn.click()

        shop_names=['BAKAM账号-UK','BAKAM账号-US','Kidzbuddy账号-UK','Kidzbuddy账号-US','Meemazi-UK','Meemazi-US','Ninigai-CA','Ninigai-UK','Ninigai-US','YYDeek账号-UK','YYDeek账号-US']

        await self.select_multiple_shops(shop_names)

    async def select_multiple_shops(self, shop_names):
        """选择多个店铺"""
        for shop_name in shop_names:
            # 这里不需要 await，直接调用 locator()
            item = self.page.locator(f'li.el-select-dropdown__item:has-text("{shop_name}")')

            # 使用 count() 检查元素是否存在
            count = await item.count()
            if count > 0:
                await item.click()
                self.logger.info(f"选择了店铺: {shop_name}")
                # 可能需要等待一下让下拉框状态更新
                await asyncio.sleep(0.25)
            else:
                self.logger.error(f"警告: 未找到店铺 {shop_name}")


    # ---------- 点击确认 ----------
    async def do_confirm(self):
        page=self.page

        confirm_btn=page.locator('body > div.el-select-dropdown.el-popper.is-multiple > div.custom-multi-footer > span.btn > button.el-button.el-button--primary.el-button--mini.is-plain.is-round > span')
        await confirm_btn.wait_for(state="visible", timeout=10000)

        self.logger.info("🔍 点击确认，监听接口")
        await asyncio.sleep(1)

        items = await self.wait_and_parse_goods_search(
            action=lambda: confirm_btn.click()
        )
        return items

    # ---------- 点击下一页 ----------
    async def next_page(self):
        page=self.page

        if self.should_stop:
            return []

        next_btn=page.locator('#ak-table-list > div.el-pagination > button.btn-next > i')

        # 不可点 = 到最后一页
        if not await next_btn.is_enabled():
            self.logger.info("📄 已到最后一页")
            self.should_stop = True
            return []

        self.logger.info("➡️ 点击下一页，监听接口")

        items = await self.wait_and_parse_goods_search(
            action=lambda: next_btn.click()
        )
        # ✅ 等一小段时间，确保页面渲染完成
        await self.page.wait_for_timeout(500)
        return items

    def parse_data(self,json_data):
        """解析数据"""
        items=[]
        if not json_data or 'data' not in json_data or 'list' not in json_data['data']:
            return items

        if not json_data['data']['list']:
            self.logger.info(f'没有更多数据了')
            self.should_stop = True
            return items

        for i in json_data['data']['list']:
            try:
                info = i['displayInfo']["productList"]
                if info:
                    productName = info[0]['productName']
                    sku = info[0]['sku']
                else:
                    productName = ''
                    sku = ''

                date = i['suggestInfo'].get('outStockDate', '')
                if date:
                    out_stock_date = datetime.strptime(str(date), "%Y-%m-%d").date()
                    today_date = datetime.now().date()

                    # 计算天数差（只考虑日期部分，忽略时间）
                    days_difference = (out_stock_date - today_date).days

                item = {
                    "账号": i['displayInfo']['storeList'][0],
                    "品名": productName,
                    "sku": sku,
                    '可售天数': i['suggestInfo']['availableSaleDaysFba'],
                    "断货时间": days_difference,
                }
                items.append(item)
            except Exception as e:
                self.logger.error(f'解析单个商品数据出错:{e}')
        return items


    def save_batch(self, items):
        """批量保存数据到CSV文件"""
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"{CONFIG_DIR}/amazon_stock_{current_date}.csv"

        # 检查文件是否存在
        file_exists = os.path.exists(filename)

        # 使用追加模式写入
        with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
            f_csv = csv.DictWriter(f, fieldnames=items[0].keys())
            if not file_exists:
                f_csv.writeheader()
            f_csv.writerows(items)
        self.logger.info('保存成功')

        return filename










