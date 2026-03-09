import json
from pathlib import Path
from typing import Dict, Optional

from apscheduler import job

from utils.config_loader import get_shop_config
from core.login import SellerSpriteLogin
import asyncio


COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"



class CookieManager:
    def __init__(self,job):
        self.job=job
        self.cookie_file = COOKIE_DIR / f"sellersprite_cookie.json"

    # ---------- cookie ----------
    def load_cookies(self) -> Optional[Dict[str, str]]:
        if not self.cookie_file.exists():
            print('cookies不存在----------')
            return None
        data = json.loads(self.cookie_file.read_text(encoding="utf-8"))
        return data


    # ---------- 刷新 ----------
    async def refresh(self):
        l= SellerSpriteLogin(
            job=self.job,
        )
        try:
            # 登录
            await l.login()
            # 获取cookie
            ok=await l.save_cookies()
            if not ok:
                raise RuntimeError(f"[卖家精灵] 登录失败")
        finally:
            await l.close()

    # ---------- 对外统一 ----------
    async def get_auth(self):
        cookies = self.load_cookies()

        if not cookies:
            await self.refresh()
            cookies = self.load_cookies()

        return cookies

# if __name__ == '__main__':
#     c=CookieManager(job='amazon_goods_monitor')
#     asyncio.run(c.refresh())
