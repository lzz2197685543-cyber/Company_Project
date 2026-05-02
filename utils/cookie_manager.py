import json
from pathlib import Path
from typing import Dict, Optional
import asyncio

JST_LOGIN_LOCK = asyncio.Lock()

from utils.config_loader import get_account_config
from core.login import JSTLogin


COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"


class CookieManager:
    def __init__(self,job):
        self.cookie_file = COOKIE_DIR / f"jst_cookies.json"
        self.cfg = get_account_config()
        self.job = job

    # ---------- cookie ----------
    def load_cookies(self) -> Optional[Dict[str, str]]:
        if not self.cookie_file.exists():
            print('cookies不存在----------')
            return None
        data = json.loads(self.cookie_file.read_text(encoding="utf-8"))
        return data


    # ---------- 刷新 ----------

    async def refresh(self):
        async with JST_LOGIN_LOCK:

            login = JSTLogin(self.job)
            ok = await login.run()

            if not ok:
                raise RuntimeError("[JST] 登录失败")

            return True

    # ---------- 对外统一 ----------
    async def get_auth(self):
        cookies = self.load_cookies()

        if not cookies:
            await self.refresh()
            cookies = self.load_cookies()

        return cookies