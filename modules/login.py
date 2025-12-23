from playwright.async_api import async_playwright
import json
from pathlib import Path
from datetime import datetime
import requests

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"


class SimpleLogin:
    def __init__(
        self,
        shop_name,
        channel_id,
        cloud_account_id,   # 👈 云浏览器环境ID
    ):
        self.shop_name = shop_name
        self.channel_id = channel_id
        self.cloud_account_id = cloud_account_id

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
            # ✅ 关键：连接云浏览器
            browser = await p.chromium.connect_over_cdp(ws_endpoint)

            # ✅ 云浏览器通常已经有 context
            context = browser.contexts[0]

            # ✅ 通常已经有页面
            if context.pages:
                page = context.pages[0]
            else:
                page = await context.new_page()

            await page.bring_to_front()

            login_url = (
                "https://login.aliexpress.com/user/seller/login"
                f"?bizSegment=CSP&channelId={self.channel_id}"
            )

            # ⚠️ 如果已经在登录页，可以不跳
            if "login" not in page.url:
                await page.goto(login_url, wait_until="domcontentloaded")

            await page.click(
                'button[type="button"]:has-text("登录")')

            try:
                await page.wait_for_url("**/m_apps/**", timeout=300000)
            except Exception:
                print(f"{self.shop_name} 登录失败")
                return False

            # ✅ 获取 cookie
            cookies = await context.cookies()
            cookies_dict = {c["name"]: c["value"] for c in cookies}

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


            # 关闭云浏览器
            self.stop_cloud_browser()

            return True


