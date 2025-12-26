class MaiJiaLogin:
    def __init__(self, phone, password):
        self.phone = phone
        self.password = password

    async def login(self, page):
        await page.goto(
            "https://www.dianleida.net/",
            wait_until="domcontentloaded"
        )

        # 密码登录
        await page.click('text=密码登录')

        # 等输入框
        await page.wait_for_selector('input[name="userPhone"]', timeout=15000)

        # 输入账号密码
        await page.fill('input[name="userPhone"]', self.phone)
        await page.fill('input[name="password"]', self.password)

        # 点击登录
        await page.click('button:has-text("登录")')

        # 等登录成功标志
        await page.wait_for_selector('text=进入工作台', timeout=15000)

        print("✅ 登录成功")
