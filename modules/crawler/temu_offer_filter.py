import time
import json
import asyncio
import playwright
from models.product import Product

from modules.scheduler.sub_category_scheduler import select_sub_categories,select_categories




class OfferFilterAutomation:
    def __init__(self, page, logger):
        self.page = page
        self.logger = logger
        self.should_stop = False

    # ---------------- 接口监听 + 解析 ----------------
    async def wait_and_parse_goods_search(self, action, timeout=5000, retry=3):
        for i in range(retry):
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

                text = await response.text()

                print(text[:200])

                if not text:
                    raise Exception("空响应")

                try:
                    json_data = json.loads(text)
                except Exception:
                    self.logger.error(f"❌ JSON解析失败: {text[:200]}")
                    raise Exception("JSON解析失败")

                return self.parse_data(json_data)

            except playwright._impl._errors.TimeoutError:
                self.logger.warning(f"⏱️ 第{i + 1}次超时")

            except Exception as e:
                self.logger.error(f"❌ 第{i + 1}次失败: {e}")

            # 👉 重试当前页（关键）
            if i < retry - 1:
                self.logger.info("🔁 重试当前页")

                try:
                    # 方法1：点当前页（优先）
                    active = self.page.locator(".arco-pagination-item-active")
                    if await active.count() > 0:
                        await active.click()
                    else:
                        pass
                        # 方法2：兜底刷新
                        # await self.page.reload()

                except Exception as e:
                    self.logger.error(f"重试点击失败: {e}")
                    await self.page.reload()

                await asyncio.sleep(2)

        # 👉 全部失败
        self.logger.error("🚨 当前页多次失败，停止翻页")
        self.should_stop = True
        return []


    # ---------------- 条件筛选 ----------------
    async def get_offer_filter(self, page_url: str = None):
        self.should_stop = False
        page = self.page

        if page_url:
            await page.goto(page_url)

        self.logger.info(f'开始筛选页面: {page_url}')

        # 关闭弹窗
        close_btn = page.locator(".arco-icon.arco-icon-close.close-icon")
        if await close_btn.count() > 0:
            await close_btn.first.click()

        # ---------- 一级类目 ----------
        await select_categories(page)

        # ---------- 二级类目（优化为循环） ----------
        await select_sub_categories(page)


        # ---------- 每页100 ----------
        await page.locator('.arco-select-view-value').nth(4).click()
        await page.get_by_text("100 条/页", exact=True).click()

        await asyncio.sleep(1)

        # ---------- 展开筛选 ----------
        await page.locator(".filter-btn-wrap div div button").nth(0).click()
        await asyncio.sleep(0.5)

        # ---------- 价格 ----------
        price_input = page.locator("#supplyPrice input").nth(0)
        await price_input.fill("3")
        await price_input.press("Enter")

    # ---------------- 搜索 ----------------
    async def do_search(self):
        self.should_stop = False

        search_btn = self.page.get_by_role("button", name="搜索", exact=True)
        await search_btn.wait_for(state="visible")

        self.logger.info("🔍 搜索第一页")

        items = await self.wait_and_parse_goods_search(
            action=lambda: search_btn.click()
        )

        return items

    # ---------------- 翻页 ----------------
    async def next_page(self):
        if self.should_stop:
            return []

        next_btn = self.page.locator('.arco-pagination-item-next')

        if not await next_btn.is_enabled():
            self.logger.info("📄 已到最后一页")
            self.should_stop = True
            return []

        self.logger.info("➡️ 下一页")

        items = await self.wait_and_parse_goods_search(
            action=lambda: next_btn.click()
        )

        await self.page.wait_for_timeout(1000)
        return items

    # ---------------- 数据解析（核心） ----------------
    def parse_data(self, json_data):
        items = []

        if not json_data or 'data' not in json_data:
            return items

        data_list = json_data['data'].get('list', [])

        if not data_list:
            self.logger.info('没有更多数据')
            self.should_stop = True
            return items

        for i in data_list:
            try:
                product = Product(
                    goods_id=i.get('goodsId', ''),
                    name=i.get('goodsName', ''),
                    category=[j.get('catName', '') for j in i.get('catItems', [{}]) if j.get('catLevel') == 1][0],
                    sub_category=(i.get('catItems', [{}])[0].get('catName', '') if i.get('catItems') else ''),
                    month_sale=i.get('monthSold', 0),
                    source="temu"
                )

                items.append(product)

            except Exception as e:
                self.logger.error(f'解析错误: {e}')

        return items