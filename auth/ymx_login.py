import asyncio
import json
from playwright.async_api import async_playwright
from pathlib import Path
from utils.config_loader import get_shop_config

CONFIG_DIR = Path(__file__).resolve().parent.parent / "data"


class MaiJiaLogin:
    def __init__(self,  headless=False):
        cfg = get_shop_config("sellersprite")
        self.phone = cfg['account']
        self.password = cfg['password']

        self.headless = headless

    async def login_and_save_cookie_dict(self, cookie_file=f"{CONFIG_DIR}/ymx_cookies_dict.json"):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto(
                "https://www.sellersprite.com/w/user/login?callback=%2Fv3%2Fproduct-research",
                wait_until="domcontentloaded"
            )

            # 账号登录
            await page.click('a[href="#pills-account"]')
            await page.wait_for_selector('#pills-account.show.active')

            await page.locator(
                '#pills-account input[placeholder*="手机号"]:visible'
            ).fill(self.phone)

            await page.locator(
                '#pills-account input[placeholder*="密"]:visible'
            ).fill(self.password)

            await page.locator(
                '#pills-account button[type="submit"]:visible'
            ).click()

            await page.wait_for_url("**/v3/**", timeout=15000)
            print("✅ 登录成功")

            # ⭐ 获取 cookies（list）
            cookies_list = await context.cookies()

            # ⭐ 转成 requests 可用的 dict
            cookies_dict = {
                c["name"]: c["value"]
                for c in cookies_list
            }

            # 保存成 json（就是你贴的那种结构）
            with open(cookie_file, "w", encoding="utf-8") as f:
                json.dump(cookies_dict, f, ensure_ascii=False, indent=2)

            print(f"🍪 cookies(dict) 已保存到 {cookie_file}")

            await browser.close()


async def main():
    client = MaiJiaLogin(headless=False)
    await client.login_and_save_cookie_dict()


# asyncio.run(main())
