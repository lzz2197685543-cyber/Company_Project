# api/filter_automation.py
import asyncio
import time


class OfferFilterAutomation:
    def __init__(self, page):
        self.page = page

    """选择玩具类目"""
    async def set_category(self, category_name: str):
        """
        选择类目（示例：玩具）
        """
        print(f"🧩 选择类目: {category_name}")

        # 1️⃣ 打开类目选择器
        await self.page.click("div.select-category")

        # 2️⃣ 定位到类目并等待它可见
        # 修改：使用正确的选择器定位包含"玩具"文本的自定义标签
        category_locator = self.page.locator('div.custom-tree-label1 span:has-text("玩具")')
        await category_locator.wait_for(state="visible", timeout=5000)

        # 3️⃣ 定位到对应的复选框并点击
        # 修改：从找到的类目标签向上定位到对应的复选框
        checkbox_locator = category_locator.locator(
            'xpath=ancestor::div[@class="el-tree-node__content"]//label//span[@class="el-checkbox__inner"]')
        await checkbox_locator.wait_for(state="visible", timeout=5000)

        # 4️⃣ 滚动复选框到可见区域并点击
        await checkbox_locator.scroll_into_view_if_needed()
        await checkbox_locator.click()

        # 5️⃣ 点击「确认选择」
        await self.page.click('span:has-text("确认选择")')

        print("✅ 完成了类目选择操作")

    """设置批发最低价"""
    async def set_price(self, min_price: str):
        print(f"💰 设置最低价格: {min_price}")
        # 1️⃣ 定位「批发价」这个筛选块
        price_block = self.page.locator(
            '.set-item:has(span:text("批发价"))'
        )

        # 2️⃣ 定位「最低」输入框
        min_price_input = price_block.locator(
            'input[placeholder="最低"]'
        )

        # 3️⃣ 等待并填写
        await min_price_input.wait_for(state="visible", timeout=10000)
        await min_price_input.fill("3")

        # 4️⃣ 触发 change / blur（Element UI 必须）
        await min_price_input.press("Enter")

    """设置最低销售额"""
    async def set_sale_volume(self, min_volume: str):
        print(f"📈 设置最低销量: {min_volume}")
        # 1️⃣ 定位「销售额」这个筛选块
        sale_amount_block = self.page.locator(
            '.set-item:has(span:text("销售额"))'
        )

        # 2️⃣ 定位「最低」输入框
        min_sale_amount_input = sale_amount_block.locator(
            'input[placeholder="最低"]'
        )

        # 3️⃣ 等待并填写
        await min_sale_amount_input.wait_for(state="visible", timeout=10000)
        await min_sale_amount_input.fill("10000")

        # 4️⃣ 触发 change / blur
        await min_sale_amount_input.press("Enter")

    """设置地区"""
    async def set_location(self, province: str):
        print(f"📍 设置地区: {province}")

        # 1️⃣ 点击「请选择」输入框，展开下拉框
        input_locator = self.page.locator(
            '.set-item .el-cascader .el-input__inner[placeholder="请选择"]'
        )

        await input_locator.wait_for(state="visible", timeout=5000)
        await input_locator.click()  # 确保加上 await

        # 2️⃣ 定位并点击「广东」这个项
        province_node = self.page.locator(
            f'.el-cascader-node__label:text("{province}")'
        )

        # 确保元素可见并滚动到视图
        await province_node.scroll_into_view_if_needed()
        await province_node.wait_for(state="visible", timeout=5000)

        # 判断是否可以交互
        if await province_node.is_visible() and await province_node.is_enabled():
            # 点击复选框的内层元素以勾选
            checkbox_locator = self.page.locator(
                f'li:has(span.el-cascader-node__label:has-text("{province}")) span.el-checkbox__inner'
            )

            await checkbox_locator.wait_for(state="visible", timeout=5000)
            await checkbox_locator.click()  # 点击复选框

        else:
            print("⚠️ 元素不可交互，检查是否被遮挡")

        # 3️⃣ 点击空白处关闭下拉框（可选）
        await self.page.mouse.click(10, 10)

    """设置最近三个月"""
    async def set_recent_days(self, days: int):
        print(f"📍 设置上架时间: {days}")
        # 定位并点击【请选择】输入框
        select_input_locator = self.page.locator(
            '#__layout > div > div.compete-shop-main > div.content.main > div > div.header > div.screen-container-1688 > div.df.set-box-container-1688 > div.middle > div > div.df > div.ml-8 > div:nth-child(1) > div > div.dfc.mt-10 > div > div > input'
        )

        # 等待输入框可见
        await select_input_locator.wait_for(state="visible", timeout=5000)

        # 点击输入框
        await select_input_locator.click()

        # 定位到「近」输入框并填写天数
        input_locator = self.page.locator(
            '.w350.dfc .el-input.el-input--mini input.el-input__inner'
        )

        await input_locator.wait_for(state="visible", timeout=5000)
        await input_locator.fill(str(days))  # 填写天数，如 "90"

        # 定位并点击确认按钮
        confirm_button_locator = self.page.locator(
            '.w350.dfc .el-button.el-button--default.el-button--mini:has-text("确认")'
        )

        # 等待按钮可点击
        await confirm_button_locator.wait_for(state="visible", timeout=5000)
        await confirm_button_locator.click()

    async def apply_all(
        self,
        *,
        category_name: str,
        min_price: str,
        min_sale_volume: str,
        province: str,
        shangxin_days: int,
    ):
        """
        一次性设置所有筛选条件
        """
        await self.set_category(category_name)
        await self.set_price(min_price)
        await self.set_sale_volume(min_sale_volume)
        await self.set_location(province)
        await self.set_recent_days(shangxin_days)










