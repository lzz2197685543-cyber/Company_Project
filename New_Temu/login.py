import asyncio
import json
import time
from playwright.async_api import async_playwright


class GeekBILogin:
    def __init__(self, phone, password, headless=False, auth_file="./data/authorization.json"):
        self.phone = phone
        self.password = password
        self.headless = headless
        self.authorization = None
        self.auth_file = auth_file

    async def _on_request(self, request):
        auth = request.headers.get("authorization")
        if auth and not self.authorization:
            self.authorization = auth
            print("✅ 捕获到 Authorization")

    def _save_auth(self):
        if not self.authorization:
            return

        data = {
            "authorization": self.authorization,
            "saved_at": int(time.time())
        }

        with open(self.auth_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"💾 Authorization 已保存到 {self.auth_file}")

    async def login(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            page = await context.new_page()

            page.on("request", self._on_request)

            await page.goto("https://www.geekbi.com/")
            await page.wait_for_load_state("domcontentloaded")

            await page.locator(
                ".arco-tabs-tab-title", has_text="手机号登录"
            ).click()

            await page.fill('input[placeholder*="手机号"]', self.phone)
            await page.fill('input[placeholder*="密码"]', self.password)
            await page.click('button[type="submit"]')

            # 等 token 请求出现
            await asyncio.sleep(5)

            # 保存 token
            self._save_auth()

            await browser.close()
            return self.authorization


async def main():
    client = GeekBILogin(
        phone="18929089237",
        password="lxz2580hh",
        headless=False
    )

    auth = await client.login()
    print("最终 Authorization：", auth)


# asyncio.run(main())
