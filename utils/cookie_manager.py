import json
from pathlib import Path
from typing import Dict, Optional

from utils.config_loader import get_shop_config
from modules.login import TKLogin


COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"


class CookieManager:
    def __init__(self,shop_name):
        self.shop_name=shop_name
        self.cfg = get_shop_config(shop_name)
        self.cookie_file = COOKIE_DIR / f"{shop_name}_cookies.json"

    # ---------- cookie ----------
    def load_cookies(self) -> Optional[Dict[str, str]]:
        if not self.cookie_file.exists():
            print('cookies不存在----------')
            return None
        data = json.loads(self.cookie_file.read_text(encoding="utf-8"))
        return data.get("cookies")


    # ---------- 刷新 ----------
    async def refresh(self):
        login = TKLogin(
            name=self.shop_name,
            account=self.cfg
        )
        ok = await login.run()
        if not ok:
            raise RuntimeError(f"[{self.shop_name}] 登录失败")

    # ---------- 对外统一 ----------
    async def get_auth(self):
        cookies = self.load_cookies()

        if not cookies:
            await self.refresh()
            cookies = self.load_cookies()

        return cookies