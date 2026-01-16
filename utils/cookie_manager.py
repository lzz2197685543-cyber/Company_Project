import json
from pathlib import Path

from utils.config_loader import get_shop_config
from core.yicai_login import YiCaiLogin
import asyncio

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"

class CookieManager:
    def __init__(self):
        self.cookie_file= COOKIE_DIR / f"yicai_cookies.json"

    # ---------- cookie ----------
    def load_cookies(self):
        if not self.cookie_file.exists():
            print('cookies不存在----------')
            return None
        data = json.loads(self.cookie_file.read_text(encoding="utf-8"))
        return data

    # ---------- 刷新 ----------
    async def refresh(self):
        login = YiCaiLogin()
        ok = await login.run()
        if not ok:
            raise RuntimeError(f"[YiCai] 登录失败")

    # ---------- 对外统一 ----------
    async def get_auth(self):
        cookies = self.load_cookies()

        if not cookies:
            await self.refresh()
            cookies = self.load_cookies()

        return cookies

#
# if __name__ == '__main__':
#     c=CookieManager()
#     print(asyncio.run(c.get_auth()))