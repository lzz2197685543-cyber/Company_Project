import time
from urllib.parse import quote
from datetime import datetime, timedelta
import csv
import os
from pathlib import Path
import asyncio
import playwright

CONFIG_DIR = Path(__file__).resolve().parent.parent / "data"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)


class OfferFilterAutomation:
    def __init__(self, page, logger):
        self.page = page
        self.logger = logger
        self.should_stop = False
        self.mode_dict = {
            1: "全托",
            2: "半托"
        }

    # ---------------- 接口监听 + 解析 ----------------
    async def wait_and_parse_goods_search(self, action, timeout=5000):
        """
        action: 一个 async lambda，里面只做一件事（点搜索 / 点下一页）
        """
        try:
            async with self.page.expect_response(
                    lambda r: (
                            "api/v1/temu/goods/search" in r.url
                            and r.status == 200
                    ),
                    timeout=timeout
            ) as resp_info:
                await action()

            response = await resp_info.value
            json_data = await response.json()
            return self.parse_data(json_data)

        except playwright._impl._errors.TimeoutError:
            self.logger.info("接口未触发响应，可能已经到最后一页或没有数据")
            self.should_stop = True
            return []

    """条件筛选 + 首次搜索"""
    async def get_offer_filter(self):
        self.logger.info('开始筛选条件')
        page = self.page

        # -------关闭可能存在的弹窗-------
        close_btn = page.locator(".arco-icon.arco-icon-close.close-icon")
        if await close_btn.count() > 0:
            await close_btn.first.wait_for(state="visible")
            await close_btn.first.click()

        # ---------- 品类一级 ----------
        category_input = page.get_by_role("textbox", name="请选择品类")
        await category_input.wait_for(state="visible")
        await category_input.fill("玩具与游戏")
        await category_input.press("Enter")
        await page.get_by_text("玩具与游戏").nth(1).click()

        # ---------- 品类二级 ----------
        sub_category_input = page.locator("#catIds").get_by_role("textbox")
        await sub_category_input.fill("婴儿玩具")
        await sub_category_input.press("Enter")
        await page.get_by_text("婴儿玩具").nth(1).click()

        await page.get_by_text("品类 玩具与游戏婴儿玩具").wait_for(state="visible")

        # ---------- 上架时间 ----------
        await page.locator("#onSaleTime > div > div > div > div.arco-picker-input.arco-picker-input-active > input").click()
        await page.get_by_role("button", name="最近三个月").click()

        # ---------- 月销量 ----------
        month_sold_min = page.locator("#monthSold").get_by_role("spinbutton", name="最低")
        await month_sold_min.fill("300")
        await month_sold_min.press("Enter")

        # ---------- 每页 100 条 ----------
        await page.locator('.arco-select-view-value').nth(4).click()
        page_size_100 = page.get_by_text("100 条/页", exact=True)
        await page_size_100.click()

        await asyncio.sleep(2)

    # ---------- 搜索 ----------
    async def do_search(self):
        page = self.page

        search_btn = page.get_by_role("button", name="搜索", exact=True)
        await search_btn.wait_for(state="visible", timeout=10000)

        self.logger.info("🔍 点击搜索，监听接口")

        items = await self.wait_and_parse_goods_search(
            action=lambda: search_btn.click()
        )

        return items

    async def next_page(self):
        page = self.page

        if self.should_stop:
            return []

        next_btn = page.locator('.arco-pagination-item.arco-pagination-item-next')

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
        await self.page.wait_for_timeout(300)
        return items

    def parse_data(self, json_data):
        """解析数据"""
        items = []
        if not json_data or 'data' not in json_data or 'list' not in json_data['data']:
            return items

        # 检查是否没有数据了
        if not json_data['data']['list']:
            self.logger.info(f'没有更多数据了')
            self.should_stop = True
            return items

        for i in json_data['data']['list']:
            try:
                item = {
                    '发现日期': int(time.time() * 1000),
                    '来源平台': 'temu',
                    '商品ID': i.get('goodsId', ''),
                    '图片': i.get('thumbnail', ''),
                    '产品名称': i.get('goodsName', ''),
                    '上架日期': i.get('createTime', ''),
                    '总销量': i.get('sold', 0),
                    '月销量': i.get('monthSold', 0),
                    '托管模式': self.mode_dict.get(i.get('hostingMode', 1), '未知'),
                    '在售站点': i.get('site', {}).get('cnName', ''),
                    '产品链接': f'https://www.temu.com/search_result.html?search_key={i["goodsId"]}&search_method=user&region={i["regionId"]}&regionCnName={quote(i["site"]["cnName"])}',
                    '类目': i.get('catItems', [{}])[1].get('catName', '') if i.get('catItems') else ''
                }

                items.append(item)
            except Exception as e:
                self.logger.error(f'解析单个商品数据出错:{e}')

        return items

    def save_batch(self, items):
        """批量保存数据到CSV文件"""
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"{CONFIG_DIR}/temu_get_new_{current_date}.csv"

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
