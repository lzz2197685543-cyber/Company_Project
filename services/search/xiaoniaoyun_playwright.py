import asyncio
import json
import time
from pathlib import Path
from playwright.async_api import async_playwright
from utils.logger import get_logger
from utils.config_loader import get_shop_config


from services.search.xiaozhuxiong import XiaozhuxiongSearch


# cookie/storage 文件夹
COOKIE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "cookies"
COOKIE_DIR.mkdir(parents=True, exist_ok=True)

img_DIR = Path(__file__).resolve().parent.parent.parent / 'data' / 'img'
# 如果目录不存在就创建它
img_DIR.mkdir(parents=True, exist_ok=True)


class ToysAASBot:
    """
    宵鸟云自动化搜索 + 消息发送 Bot
    支持：
    - 短信验证码登录（首次手动）
    - 上传本地图片搜索
    - 获取搜索结果前10条
    - 自动进入详情页发起聊天，如果按钮存在
    """

    def __init__(self, headless: bool = False):
        self.logger = get_logger("search_factory")
        cfg = get_shop_config("xiaoniaoyun")
        self.account = cfg["account"]
        self.headless = headless

        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.storage_file = COOKIE_DIR / "xiaoniaoyun_storage.json"
        self.search = XiaozhuxiongSearch()

    # ---------------- 浏览器初始化 ----------------
    async def init_browser(self) -> bool:
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(headless=self.headless)
            self.logger.info("✅ 浏览器初始化成功")
            return True
        except Exception as e:
            self.logger.error("浏览器初始化失败", exc_info=True)
            return False

    # ---------------- 登录 ----------------
    async def login(self) -> bool:
        # 已有登录态
        # if self.storage_file.exists():
        #     self.logger.info("检测到已有登录态，直接复用 storage_state")
        #     self.context = await self.browser.new_context(storage_state=str(self.storage_file))
        #     self.page = await self.context.new_page()
        #     return True

        # 登录方法一：微信扫码登录
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        await self.page.goto("https://www.toysaas.com/home/login", wait_until="domcontentloaded")
        input('微信扫码登录，扫完之后输入ok:')
        btn = self.page.get_by_role("button", name="我知道了")

        try:
            await btn.wait_for(timeout=2000)
            await btn.click()
        except:
            pass

        # 登录方法二：验证码登录
        # 首次登录
        # self.context = await self.browser.new_context()
        # self.page = await self.context.new_page()
        # await self.page.goto("https://www.toysaas.com/home/login", wait_until="domcontentloaded")
        #
        # # 点击图标登录
        # await self.page.get_by_role("img").nth(1).click()
        #
        # # 填写手机号
        # await self.page.get_by_role("textbox", name="请输入手机号").fill(self.account)
        # await self.page.get_by_role("link", name="获取验证码").click()
        #
        # # 人工输入验证码
        # verification_code = input("请输入手机上收到的验证码: ")
        # await self.page.get_by_role("textbox", name="请输入短信验证码").fill(verification_code)
        # await self.page.get_by_role("checkbox").check()
        # await self.page.get_by_role("button", name="登录").click()
        #
        # # ⭐ 等待登录成功
        # await self.page.wait_for_selector(".is-opened .el-menu-item:nth-child(1)", timeout=60000)
        # btn = self.page.get_by_role("button", name="我知道了")
        #
        # try:
        #     await btn.wait_for(timeout=3000)
        #     await btn.click()
        # except:
        #     pass

        # 保存登录态
        await self.save_storage()
        self.logger.info("✅ 登录成功并保存 storage_state")
        return True

    # ---------------- 保存 storage_state ----------------
    async def save_storage(self):
        if self.context:
            await self.context.storage_state(path=str(self.storage_file))
            self.logger.info(f"storage_state 已保存到 {self.storage_file}")


    # ---------------- 上传图片 + 抓 search_image 接口 ----------------
    async def upload_img_and_fetch_items(self, image_path: str):
        items = []
        # 1️⃣ 先准备监听接口
        async with self.page.expect_response(
                lambda r: "api/toy/search_image" in r.url and r.status == 200,
                timeout=20000
        ) as resp_info:
            # 2️⃣ 点击上传
            await self.page.locator("img").nth(2).click()
            await self.page.locator('div.el-upload.el-upload--text').first.wait_for(timeout=15000)

            async with self.page.expect_file_chooser() as fc_info:
                await self.page.locator("div.el-upload.el-upload--text").first.click()

            file_chooser = await fc_info.value
            await file_chooser.set_files(image_path)
            self.logger.info(f"✅ 图片 {image_path} 上传完成")

        # 3️⃣ 接口真正返回
        response = await resp_info.value
        data = await response.json()

        upload_img_url = self.search.get_img_url(image_path)


        # 4️⃣ 解析数据
        for i in data.get("data", {}).get("list", [])[:10]:
            items.append({
                '平台': '宵鸟云',
                "搜图图片": upload_img_url,
                '商品名称': i.get('name'),
                '商品图片链接': i.get('main_picture'),
                '价格': i.get('exworks_price'),
                '供应商': i.get('factory_name'),
                '联系人': i.get('factory_contact'),
                '手机号': i.get('mobilephone'),
                'QQ': i.get('factory_qq', ''),
                '地址': i.get('factory_address'),
                'id': str(i.get('id')),
                '爬取数据时间': int(time.time() * 1000),
            })

        self.logger.info(f"✅ 搜索接口返回 {len(items)} 条结果")
        return items,upload_img_url

    # ---------------- 发消息 ----------------
    async def send_message(self, item_id: str, message: str):
        try:
            # 1️⃣ 确保商品被渲染（虚拟列表）
            for _ in range(10):
                if await self.page.locator(f'[id="{item_id}"]').count() > 0:
                    break
                await self.page.mouse.wheel(0, 1200)
                await asyncio.sleep(0.3)
            else:
                raise RuntimeError("商品未渲染，可能在虚拟列表外")

            # 2️⃣ 点击商品卡片（不要点 img）
            card = self.page.locator(f'[id="{item_id}"]').first
            await card.scroll_into_view_if_needed()
            await card.click()

            # 3️⃣ 等页面/弹层稳定（不要等 load）
            await self.page.wait_for_timeout(500)

            # 4️⃣ 点击「发起聊天」（最多等 5 秒）
            chat_btn = self.page.get_by_text("发起聊天", exact=True)
            try:
                await chat_btn.wait_for(timeout=5000)
                await chat_btn.click()
            except:
                self.logger.warning(f"⚠️ 商品 {item_id} 未出现「发起聊天」按钮，跳过发送消息")
                try:
                    second_close = self.page.locator(
                        "#product_dialog_body_main > div > div.el-dialog__header > button > i").nth(0)
                    if await second_close.is_visible():
                        await second_close.click()
                        self.logger.info("✅ 第二个聊天框已关闭")
                except Exception as e:
                    self.logger.warning(f"⚠️ 关闭第二个聊天框失败: {e}")
                return

            await self.page.get_by_text("发送").first.click()
            await asyncio.sleep(1)

            # 5️⃣ 等待消息输入框
            send_box = self.page.locator(".send-box")
            await send_box.wait_for(timeout=5000)
            await send_box.fill(message)

            await asyncio.sleep(1)

            # 6️⃣ 点击发送
            send_btn = self.page.locator("div.send-btn:not(.disallow)")
            await send_btn.wait_for(timeout=5000)
            await send_btn.click()

            await asyncio.sleep(1.5)

            self.logger.info(f"✅ 商品 {item_id} 已成功发送消息")

            # 关闭第一个聊天框
            try:
                first_close = self.page.locator(
                    "#app > div > div > div:nth-child(5) > div:nth-child(2) > div.chat-con > div.chat-right > div.c-header > i")
                if await first_close.is_visible():
                    await first_close.click()
                    self.logger.info("✅ 第一个聊天框已关闭")
            except Exception as e:
                self.logger.warning(f"⚠️ 关闭第一个聊天框失败: {e}")

            # 关闭第二个聊天框
            try:
                second_close = self.page.locator("#product_dialog_body_main > div > div.el-dialog__header > button > i").nth(0)
                if await second_close.is_visible():
                    await second_close.click()
                    self.logger.info("✅ 第二个聊天框已关闭")
            except Exception as e:
                self.logger.warning(f"⚠️ 关闭第二个聊天框失败: {e}")


        except Exception as e:
            self.logger.warning(f"⚠️ 商品 {item_id} 发送失败: {e}")

    # ---------------- 关闭 ----------------
    async def close(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.logger.info("浏览器已关闭")

    # ---------------- 批量处理图片 ----------------
    async def process_images(self, image_list, message):
        all_items = []  # 所有 item，用于钉钉
        sent_factories = set()  # 已发送过消息的厂名

        for img in image_list:
            items,upload_img_url = await self.upload_img_and_fetch_items(img)
            self.logger.info(f"✅ 图片 {img} 搜索完成，找到 {len(items)} 条结果")

            for item in items:
                print(item)
                factory_name = item.get('供应商') or item.get('factory_name')

                # === 发送消息：只对“新厂名”发 ===
                if factory_name and factory_name not in sent_factories:
                    self.logger.info(f"📨 给厂家发送消息：{factory_name}")
                    message = f"[查看图片]({upload_img_url})\n\n{message}"
                    await self.send_message(item_id=item['id'],message=message)
                    sent_factories.add(factory_name)
                else:
                    self.logger.info(f"⏭ 已发送过，跳过厂家：{factory_name}")

                # === 所有 item 都收集 ===
                all_items.append(item)

            # 回到首页
            await self.page.goto(
                'https://www.toysaas.com/',
                wait_until="domcontentloaded"
            )

        return all_items


# ---------------- 使用示例 ----------------
async def main():
    bot = ToysAASBot(headless=False)
    success = await bot.init_browser() and await bot.login()
    if not success:
        print("登录失败")
        return

    # 批量搜索图片并发送消息
    image_list = [
        r"D:\sd14\Factory_sourcing\data\img\basketball.png",
        r"D:\sd14\Factory_sourcing\data\img\bird.jpg",
    ]
    await bot.process_images(image_list=image_list, message="你好")
    await bot.close()


# if __name__ == "__main__":
#     asyncio.run(main())
