import json
import asyncio
import requests
from playwright.async_api import async_playwright


from utils.logger import get_logger



class BrowserManager:
    """浏览器管理器，负责与外部浏览器服务的通信和连接管理"""

    start_api = "http://127.0.0.1:6873/api/v1/browser/start"
    stop_api = "http://127.0.0.1:6873/api/v1/browser/stop"

    def __init__(self, hub_id, name, logger=None):
        """
        :param hub_id: 容器ID
        :param name: 店铺名称，用于日志
        :param logger: 日志记录器
        """
        self.hub_id = hub_id
        self.name = name
        self.logger = logger or get_logger("browser_manager")

        self.debug_port = None
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    async def start(self):
        """启动外部浏览器"""
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

            # 等待浏览器完全启动
            await asyncio.sleep(1)

            # 连接浏览器
            return await self._connect()

        except Exception as e:
            self.logger.error(f"{self.name} - 启动异常: {e}")
            return False

    async def _connect(self):
        """连接到已启动的浏览器"""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.connect_over_cdp(
                f"http://127.0.0.1:{self.debug_port}"
            )

            # 获取或创建上下文和页面
            try:
                self.context = self.browser.contexts[0]
                self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()
            except Exception:
                self.context = await self.browser.new_context()
                self.page = await self.context.new_page()

            self.logger.info(f"{self.name} - 已连接浏览器")
            return True

        except Exception as e:
            self.logger.error(f"{self.name} - 连接失败: {e}")
            return False

    async def stop(self):
        """停止外部浏览器"""
        try:
            requests.post(
                self.stop_api,
                json={"containerCode": self.hub_id},
                timeout=10
            )
            self.logger.info(f"{self.name} - 已发送停止浏览器请求")
        except Exception as e:
            self.logger.error(f"{self.name} - 停止浏览器异常: {e}")

    async def close(self):
        """关闭Playwright连接"""
        try:
            if self.browser:
                await self.browser.close()
                self.browser = None
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            self.context = None
            self.page = None
            self.logger.info(f"{self.name} - 已关闭Playwright连接")
        except Exception as e:
            self.logger.error(f"{self.name} - 关闭连接异常: {e}")

    async def restart(self):
        """重启浏览器"""
        await self.close()
        await self.stop()
        await asyncio.sleep(2)  # 等待完全关闭
        return await self.start()

    def get_page(self):
        """获取当前页面对象"""
        return self.page

    def get_context(self):
        """获取当前上下文对象"""
        return self.context

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.close()
        await self.stop()



