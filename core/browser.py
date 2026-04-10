from playwright.async_api import async_playwright

class BrowserManager:
    def __init__(self, headless=False):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    async def start(self, user_agent=None, viewport=None):
        """启动浏览器并创建主页面"""
        self.playwright = await async_playwright().start()
        launch_options = {
            "headless": self.headless,
            "args": ["--disable-blink-features=AutomationControlled"]
        }
        self.browser = await self.playwright.chromium.launch(**launch_options)

        context_options = {}
        if user_agent:
            context_options["user_agent"] = user_agent
        if viewport:
            context_options["viewport"] = viewport
        else:
            context_options["viewport"] = {"width": 1366, "height": 768}

        self.context = await self.browser.new_context(**context_options)
        self.page = await self.context.new_page()
        return self.page

    async def new_page(self):
        """创建新页面（共享同一个上下文，保持登录状态）"""
        return await self.context.new_page()

    async def close(self):
        """关闭浏览器"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()