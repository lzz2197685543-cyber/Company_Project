import json
import aiohttp
import asyncio
from pathlib import Path
from typing import Dict, Optional

from utils.config_loader import get_shop_config
from core.miaoshou_login import MiaoShouLogin
from core.browser import BrowserManager


COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"


class CookieManager:
    def __init__(self,job):
        self.cookie_file = COOKIE_DIR / f"miaoshou_cookies.json"
        self.job=job

    # ---------- cookie ----------
    def load_cookies(self) -> Optional[Dict[str, str]]:
        if not self.cookie_file.exists():
            return None
        data = json.loads(self.cookie_file.read_text(encoding="utf-8"))
        return data.get("cookies")

    # ---------- 刷新 ----------
    async def refresh(self):
        """主函数 - 使用方式1：手动管理浏览器"""
        # 创建浏览器管理器
        browser_manager = BrowserManager(headless=False)

        try:
            # 启动浏览器
            page = await browser_manager.start(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                viewport={"width": 1366, "height": 768}
            )

            # 创建登录实例
            client = MiaoShouLogin(page,self.job)
            ok=await client.login()

            # 登录成功后可以保持浏览器打开
            print("登录完成，浏览器将保持打开状态...")

        finally:
            # 关闭浏览器
            await browser_manager.close()


        if not ok:
            raise RuntimeError(f"[妙手] 登录失败")

    # ---------- 对外统一 ----------
    async def get_auth(self):
        cookies = self.load_cookies()

        if not cookies:
            await self.refresh()
            cookies = self.load_cookies()

        return cookies

# if __name__ == '__main__':
#     c=CookieManager("test")
#     asyncio.run(c.get_auth())
