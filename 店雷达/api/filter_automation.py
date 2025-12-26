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
        category_locator = self.page.locator(f'span.el-tree-node__label:has-text("{category_name}")')
        await category_locator.wait_for(state="visible", timeout=5000)

        # 3️⃣ 定位到与 `span.el-tree-node__label` 同级的复选框 input 元素并点击
        checkbox_locator = category_locator.locator('xpath=./preceding-sibling::label//span[1]//span')
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
            '.set-item .dfc.mt-10 .el-input.el-input--small.el-input--suffix input[placeholder="请选择"]'
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

    """设置每页的页数"""
    async def set_page_size(self, page_size: int):
        """
        设置每页显示的条目数量
        """
        print(f"🧩 设置每页显示的数据量为: {page_size}")

        # 1️⃣ 定位到每页显示数据量的输入框
        page_size_locator = self.page.locator('.dfc .el-pagination .el-select .el-input .el-input__inner')
        await page_size_locator.wait_for(state="visible", timeout=5000)
        await page_size_locator.click()  # 点击下拉框展开

        # 2️⃣ 定位并点击选择的页数（如 30）
        page_size_option_locator = self.page.locator(f'.el-select-dropdown__item:has-text("{page_size}")')
        await page_size_option_locator.wait_for(state="visible", timeout=5000)
        await page_size_option_locator.click()  # 点击选择的页数

        # 3️⃣ 等待一段时间，确保页面设置完成
        await asyncio.sleep(1)  # 可以根据需要调整等待时间
        print(f"✅ 每页显示数据量已设置为 {page_size}")

        # 4️⃣ 确保设置完毕后，继续进行其他操作
        await asyncio.sleep(1)  # 等待页面刷新

    """点击下一页"""
    async def go_to_next_page(self):
        print("⏳ 点击【下一页】进行翻页...")

        next_btn = self.page.locator(
            '.el-pagination button.btn-next:not([disabled])'
        )

        # 等待可见
        await next_btn.wait_for(state="visible", timeout=10000)

        # 点击
        await next_btn.click()

        print("✅ 成功点击【下一页】")

        # 等接口返回（比 sleep 稳）
        await self.page.wait_for_timeout(800)

    async def has_next_page(self) -> bool:
        """
        判断是否存在可点击的【下一页】按钮
        """
        next_btn = self.page.locator(
            '.el-pagination button.btn-next:not([disabled])'
        )

        return await next_btn.count() > 0

    async def apply_all(
        self,
        *,
        category_name: str,
        min_price: str,
        min_sale_volume: str,
        province: str,
        shangxin_days: int,
        page_size:int,
    ):
        """
        一次性设置所有筛选条件
        """
        await self.set_category(category_name)
        await self.set_price(min_price)
        await self.set_sale_volume(min_sale_volume)
        await self.set_location(province)
        await self.set_recent_days(shangxin_days)
        await self.set_page_size(page_size)

    async def capture_first_page(self):
        async with self.page.expect_response(
                lambda r: "/offerSearch/queryList" in r.url and r.status == 200,
                timeout=15000
        ) as resp_info:
            await self.page.click("button:has-text('查询')")

        response = await resp_info.value
        print(response.json)
        return await response.json()

    async def next_page_and_capture(self):
        """
        点击下一页，并捕获该页的 offerSearch 响应
        """
        async with self.page.expect_response(
                lambda r: "/offerSearch/queryList" in r.url and r.status == 200,
                timeout=15000
        ) as resp_info:
            await self.go_to_next_page()

        response = await resp_info.value
        response_json = await response.json()
        print(response_json)
        return response_json

    def parse_offer_response(self, response_json):
        rows = []

        for i in response_json['result']['list']:
            rows.append({
                "发现日期": int(time.time() * 1000),
                "来源": "1688",
                "商品ID": i['id'],
                "产品名称": i['title'],
                "产品链接": f'https://detail.1688.com/offer/{i["id"]}.html',
                "类目": i['levelName'].split('>')[1] if '>' in i['levelName'] else i['levelName'],
                "上架时间": i['createTime'],
                "总销量": i['saleCount'],
                "站点": f"{i['province']} {i['city']}",
            })

        return rows

    async def crawl_pages(self, total_pages: int, csv_writer):
        print("📄 抓取第 1 页")

        first_json = await self.capture_first_page()
        rows = self.parse_offer_response(first_json)
        csv_writer.write_rows(rows)

        print(f"✅ 第 1 页写入 {len(rows)} 条")

        for page_num in range(2, total_pages + 1):
            if not await self.has_next_page():
                print("🛑 已到最后一页")
                break

            print(f"📄 抓取第 {page_num} 页")

            response_json = await self.next_page_and_capture()
            rows = self.parse_offer_response(response_json)
            csv_writer.write_rows(rows)

            print(f"✅ 第 {page_num} 页写入 {len(rows)} 条")






