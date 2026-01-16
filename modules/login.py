from playwright.async_api import async_playwright
import json
from pathlib import Path
from datetime import datetime
import requests
import asyncio
from utils.cookie_manager import get_shop_config
from utils.dingtalk_bot import ding_bot_send

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"

# ✅ AliExpress / Ali 系 cookies 白名单
COOKIE_WHITELIST = {
    'cna',
    '_ga',
    '_ga_VED1YSGNC7',
    '_ga_save',
    '_uetvid',
    'ali_apache_id',
    'lzd_cid',
    'x_router_us_f',
    'aep_usuc_f',
    'tfstk',
    '_lang',
    'xman_us_f',
    'intl_common_forever',
    'X-XSRF-TOKEN',
    'SCMLOCALE',
    'locale',
    'WDK_SESSID',
    'isg',
    'gmp_sid',
    '_bl_uid',
    '_tb_token_',
    'acs_usuc_t',
    'bx_s_t',
    'global_seller_sid',
    'xman_us_t',
    'xman_t',
    'xman_f',
    'sgcookie',
    'intl_locale',
    '_m_h5_tk',
    '_m_h5_tk_enc',
    '_baxia_sec_cookie_',
}



class SimpleLogin:
    def __init__(
        self,
        shop_name
    ):
        self.shop_name = shop_name
        cfg = get_shop_config(shop_name)
        self.channel_id = cfg["channelId"]
        self.cloud_account_id = cfg["cloud_account_id"]
        self.username = cfg["account"]
        self.password = cfg["password"]

    def filter_cookies(self,cookies_dict: dict) -> dict:
        """
        只保留白名单中的 cookies（供 requests / aiohttp 使用）
        """
        return {
            k: v
            for k, v in cookies_dict.items()
            if k in COOKIE_WHITELIST and v
        }

    def start_cloud_browser(self) -> str:
        url = "http://localhost:50213/api/v2/browser/start"
        resp = requests.get(
            url,
            params={"account_id": self.cloud_account_id},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()["data"]["ws"]['puppeteer']

    def stop_cloud_browser(self):
        """
        通过云浏览器 API 关闭浏览器实例
        """
        url = "http://localhost:50213/api/v2/browser/stop"
        try:
            resp = requests.get(
                url,
                params={"account_id": self.cloud_account_id},
                timeout=10
            )
            resp.raise_for_status()
            print(f"[{self.shop_name}] 云浏览器已关闭")
        except Exception as e:
            print(f"[{self.shop_name}] 关闭云浏览器失败: {e}")

    async def login_and_save_cookies(self) -> bool:
        COOKIE_DIR.mkdir(parents=True, exist_ok=True)

        ws_endpoint = self.start_cloud_browser()

        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(ws_endpoint)
            context = browser.contexts[0]

            page = context.pages[0] if context.pages else await context.new_page()
            await page.bring_to_front()

            login_url = (
                "https://login.aliexpress.com/user/seller/login"
                f"?bizSegment=CSP&channelId={self.channel_id}"
            )

            if "login" not in page.url:
                await page.goto(login_url, wait_until="domcontentloaded")

            user_input = page.locator('#loginName')
            await user_input.wait_for(state='visible', timeout=15_000)
            await user_input.fill(self.username)

            password_input = page.locator('#password')
            await password_input.wait_for(state='visible', timeout=15_000)
            await password_input.fill(self.password)

            await page.click('button[type="button"]:has-text("登录")')

            try:
                await page.wait_for_url("**/m_apps/**", timeout=300_000)
            except Exception:
                print(f"{self.shop_name} 登录失败")
                return False

            await page.goto(f'https://csp.aliexpress.com/m_apps/ascp/aechoice.inventory_distribution_details_management?channelId={self.channel_id}',wait_until="domcontentloaded")

            # 等待元素可见

            await asyncio.sleep(6)

            raw_cookies = await context.cookies()
            cookies_dict = {c['name']: c['value'] for c in raw_cookies}
            cookies_dict = self.filter_cookies(cookies_dict)

            # ⚠️ 检查 WDK_SESSID 是否存在
            if 'WDK_SESSID' not in cookies_dict:
                print(f"[{self.shop_name}] 登录成功，但 WDK_SESSID 不存在，cookies 无效")
                self.stop_cloud_browser()
                return False

            cookie_data = {
                "shop_name": self.shop_name,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "cookies_dict": cookies_dict,
            }

            cookie_file = COOKIE_DIR / f"{self.shop_name}.json"
            cookie_file.write_text(
                json.dumps(cookie_data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )

            self.stop_cloud_browser()
            return True



