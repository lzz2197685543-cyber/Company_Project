# api/filter_automation.py
import asyncio
import time


class OfferFilterAutomation:
    def __init__(self, page,logger):
        self.page = page
        self.logger = logger

    """选择搜索方式"""
    async def set_search_choice(self, search_name: str):
        """
        选择类目（示例：玩具）
        """
        self.logger.info(f"🧩 选择搜索方式: {search_name}")

        # 1️⃣ 点击精准搜索框
        await self.page.click("#__layout > div > div.compete-shop-main > div.content.main > div > div.header > div.dfcjsb.mt-15.mb-18 > div:nth-child(2) > div > div > div > div:nth-child(1) > div > input")

        # 2️⃣ 选择商品ID
        category_locator = self.page.locator(
            f"li.el-select-dropdown__item:has-text('{search_name}')"
        )

        await category_locator.click()

    """填入商品id"""

    async def fill_goods_id(self, goods_id):
        self.logger.info(f'搜索的商品id是:{str(goods_id)}')
        await asyncio.sleep(1)
        await self.page.locator("input[placeholder='顿号隔开搜索多个1688商品ID']").fill(str(goods_id))

    async def apply_all(self,search_name,goods_id,):
        await self.set_search_choice(search_name)
        await self.fill_goods_id(goods_id)










