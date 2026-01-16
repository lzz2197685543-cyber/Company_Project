import asyncio
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from services.search.xiaoniaoyun_playwright import ToysAASBot
from utils.logger import get_logger
from utils.webchat_send import webchat_send

from datetime import datetime

QUIET_HOURS = (23, 8)


# ================= 配置 =================
COOKIE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "cookies"
STORAGE_STATE = COOKIE_DIR / "xiaoniaoyun_storage.json"
CHECK_INTERVAL = 5  # 监控间隔（秒）
# =======================================


class XiaoniaoyunMonitor:
    def __init__(self, headless=True):
        self.headless = headless
        self.bot = ToysAASBot()
        self.logger = get_logger("xiaoniaoyun_monitor")
        self.last_count = None

    async def ensure_login(self):
        """
        确保登录态可用（storage_state）
        """
        if not STORAGE_STATE.exists():
            self.logger.warning("⚠️ storage_state 不存在，开始登录...")
            success = await self.bot.init_browser() and await self.bot.login()
            if not success:
                raise RuntimeError("❌ 登录失败")
        else:
            success = await self.bot.init_browser() and await self.bot.login()
            if not success:
                raise RuntimeError("❌ 登录失败")

        self.logger.info("✅ 登录态确认完成")

    async def get_unread_count(self, page) -> int:
        """
        获取未读消息数量
        """
        badge = page.locator(
            "#index-main > div.content > div.home-navBar > div > div > div:nth-child(6) > sup"
        )

        if await badge.count() == 0:
            return 0

        text = await badge.inner_text()
        return int(text.strip())

    def is_quiet_time(self) -> bool:
        hour = datetime.now().hour
        start, end = QUIET_HOURS

        # 跨天判断
        if start < end:
            return start <= hour < end
        else:
            return hour >= start or hour < end

    async def notify(self, new_count: int):
        if self.is_quiet_time():
            self.logger.info("🌙 夜间免打扰时间，跳过通知")
            return

        msg = f"""
📬 宵鸟云新消息提醒

新增未读：{new_count - self.last_count} 条
当前未读：{new_count} 条

👉 后台地址：
https://www.toysaas.com/
        """
        webchat_send("环创-开发曾小姐", msg)
        webchat_send("环创-开发陈小姐",'有监控到消息变化')

    async def run(self):
        """
        实时监控主循环
        """
        await self.ensure_login()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(storage_state=str(STORAGE_STATE))
            page = await context.new_page()

            await page.goto("https://www.toysaas.com/", timeout=100000)
            self.logger.info("🚀 开始实时监控宵鸟云消息")

            while True:
                try:
                    count = await self.get_unread_count(page)
                    self.logger.info(f"当前未读消息：{count}")

                    # 第一次读取：只记录，不通知
                    if self.last_count is None:
                        self.last_count = count
                        self.logger.info("🔰 初始化未读数，不发送通知")
                        continue

                    if count > self.last_count and count>0:
                        await self.notify(count)

                    self.last_count = count



                except PlaywrightTimeoutError:
                    self.logger.warning("⚠️ 页面读取超时，继续监控")
                except Exception as e:
                    self.logger.exception(f"❌ 监控异常：{e}")

                await asyncio.sleep(CHECK_INTERVAL)


# ================= 启动入口 =================
# if __name__ == "__main__":
#     asyncio.run(XiaoniaoyunMonitor(headless=True).run())
