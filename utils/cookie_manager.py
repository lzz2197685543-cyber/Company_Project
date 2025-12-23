import json
import time
import hashlib
import aiohttp
from pathlib import Path
from typing import Dict, Optional

from utils.config_loader import get_shop_config
from modules.login import SimpleLogin


COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"


class CookieManager:
    def __init__(self, shop_name: str):
        self.shop_name = shop_name
        self.cookie_file = COOKIE_DIR / f"{shop_name}.json"

        cfg = get_shop_config(shop_name)
        self.channel_id = cfg["channelId"]
        self.cloud_account_id = cfg["cloud_account_id"]

        self.check_url = (
            "https://seller-acs.aliexpress.com/"
            "h5/mtop.ae.scitem.read.pagequery/1.0/"
        )

    # ---------- cookie ----------
    def load_cookies(self) -> Optional[Dict[str, str]]:
        if not self.cookie_file.exists():
            return None
        data = json.loads(self.cookie_file.read_text(encoding="utf-8"))
        return data.get("cookies_dict")

    def extract_token(self, cookies: Dict[str, str]) -> str:
        tk = cookies.get("_m_h5_tk", "")
        return tk.split("_")[0] if "_" in tk else ""

    # ---------- 校验 ----------
    async def check_cookie_valid(self, cookies: Dict[str, str]) -> bool:
        try:
            async with aiohttp.ClientSession(cookies=cookies) as session:
                async with session.get(
                    self.check_url,
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    return resp.status == 200
        except Exception:
            return False

    # ---------- 刷新 ----------
    async def refresh(self):
        login = SimpleLogin(
            shop_name=self.shop_name,
            channel_id=self.channel_id,
            cloud_account_id=self.cloud_account_id,
        )
        ok = await login.login_and_save_cookies()
        if not ok:
            raise RuntimeError(f"[{self.shop_name}] 登录失败")

    # ---------- 对外统一 ----------
    async def get_auth(self):
        cookies = self.load_cookies()

        if not cookies:
            await self.refresh()
            cookies = self.load_cookies()

        token = self.extract_token(cookies)
        if not token:
            raise RuntimeError("token 解析失败")

        return cookies, token
