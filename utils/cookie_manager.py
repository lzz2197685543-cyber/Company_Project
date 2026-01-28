import json
from pathlib import Path
import asyncio
from core.login import LingXingERPLogin

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"

class CookieManager:
    def __init__(self,job):
        self.cookie_file = COOKIE_DIR / f"cookies.json"
        self.job = job

    # ---------- cookie ----------
    def load_cookies(self):
        if not self.cookie_file.exists():
            print('cookies不存在----------')
            return None
        data = json.loads(self.cookie_file.read_text(encoding="utf-8"))
        return data


    # ---------- 刷新 ----------
    async def refresh(self):
        login = LingXingERPLogin(self.job)
        ok = await login.run()
        if not ok:
            raise RuntimeError(f"[领星ERP-跨境电商管理系统] 登录失败")

    # ---------- 对外统一 ----------
    async def get_auth(self):
        cookies = self.load_cookies()


        if not cookies:
            await self.refresh()
            cookies = self.load_cookies()

        print(cookies)
        return cookies

# if __name__ == '__main__':
#     a=CookieManager('lingxing')
#     asyncio.run(a.get_auth())