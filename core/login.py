import asyncio
import json
from playwright.async_api import async_playwright
from utils.config_loader import get_account_config
from utils.logger import get_logger
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from utils.dingtalk_bot import ding_bot_send

from pathlib import Path

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"
# 确保目录存在
COOKIE_DIR.mkdir(parents=True, exist_ok=True)

# 浏览器用户数据目录 - 用于持久化登录状态
USER_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "browser_profile" / "jst"
USER_DATA_DIR.mkdir(parents=True, exist_ok=True)


class JSTLogin:
    def __init__(self, job):
        cfg = get_account_config()
        self.username = cfg["username"]
        self.password = cfg["password"]
        self.logger = get_logger(job)
        self.job = job

        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # Cookie 文件路径
        self.cookie_file = COOKIE_DIR / "jst_cookies.json"

    async def init_browser(self) -> bool:
        """初始化浏览器 - 使用持久化上下文"""
        try:
            self.playwright = await async_playwright().start()

            # 启动持久化上下文 - 这是关键修改
            # 这样浏览器会保存所有状态（localStorage、session、指纹等）
            self.context = await self.playwright.chromium.launch_persistent_context(
                str(USER_DATA_DIR),  # 用户数据目录
                headless=False,  # 保持非无头模式，更不容易被检测
                args=[
                    '--disable-blink-features=AutomationControlled',  # 禁用自动化标记
                    '--disable-features=IsolateOrigins,site-per-process',  # 减少指纹特征
                    '--disable-web-security',  # 禁用web安全（可选）
                    '--disable-features=BlockInsecurePrivateNetworkRequests',  # 避免某些限制
                ],
                viewport={'width': 1280, 'height': 800},  # 固定视口大小
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                # 使用常见UA
            )

            # 创建新页面
            self.page = await self.context.new_page()

            # 注入脚本移除自动化特征
            await self.page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                // 覆盖一些常见的检测点
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                Object.defineProperty(navigator, 'languages', {
                    get: () => ['zh-CN', 'zh']
                });

                // 模拟真实的chrome对象
                window.chrome = {
                    runtime: {}
                };
            """)

            return True
        except Exception as e:
            self.logger.error("初始化浏览器失败", exc_info=True)
            return False

    async def load_cookies(self) -> bool:
        """加载已保存的cookie到当前上下文"""
        try:
            if not self.cookie_file.exists():
                self.logger.info("Cookie文件不存在，需要手动登录")
                return False

            with open(self.cookie_file, 'r', encoding='utf-8') as f:
                cookie_dict = json.load(f)

            # 将dict格式转换为playwright所需的cookie格式
            cookies = []
            for name, value in cookie_dict.items():
                cookie = {
                    'name': name,
                    'value': value,
                    'domain': '.erp321.com',  # 根据实际域名调整
                    'path': '/',
                    'httpOnly': False,
                    'secure': True,
                    'sameSite': 'Lax'
                }
                cookies.append(cookie)

            # 添加上下文
            await self.context.add_cookies(cookies)
            self.logger.info("✅ 已加载保存的Cookie")
            return True

        except Exception as e:
            self.logger.error(f'加载cookie失败: {e}')
            return False

    async def login(self) -> bool:
        """登录流程 - 会先尝试使用cookie，失败则手动登录"""
        try:
            # 1️⃣ 先尝试加载cookies
            cookies_loaded = await self.load_cookies()

            # 2️⃣ 访问登录页
            await self.page.goto(
                "https://ww.erp321.com/login.aspx",
                wait_until="domcontentloaded",
                timeout=20000
            )

            # 3️⃣ 检查是否已经登录（通过cookies）
            # 方法：检查是否直接跳转到首页，或者是否有特定元素
            try:
                # 等待一小段时间，看看是否已经登录成功
                await self.page.wait_for_timeout(3000)

                # 尝试找首页元素（如果已经登录）
                order_menu = self.page.locator(".menuText___HLBVY").first
                if await order_menu.is_visible(timeout=3000):
                    self.logger.info("✅ 使用已保存的Cookie登录成功")
                    return True
            except:
                # 没有找到首页元素，说明cookies无效或过期，需要手动登录
                self.logger.info("Cookie无效或过期，开始手动登录流程")

            # 4️⃣ 如果cookies无效，执行手动登录流程
            # 等待账号输入框出现
            account_input = self.page.get_by_role("textbox", name="登录账号")
            await account_input.wait_for(state="visible", timeout=10000)
            await account_input.click()
            await account_input.fill(self.username)

            # 密码输入框
            password_input = self.page.get_by_role("textbox", name="登录密码")
            await password_input.wait_for(state="visible", timeout=10000)
            await password_input.click()
            await password_input.fill(self.password)

            # 勾选协议
            agreement_checkbox = self.page.get_by_role(
                "checkbox",
                name="我已阅读并同意 《用户协议》 、 《隐私政策》"
            )
            await agreement_checkbox.wait_for(state="attached", timeout=10000)

            if not await agreement_checkbox.is_checked():
                await agreement_checkbox.check()

            # 点击登录按钮
            login_btn = self.page.locator('.ant-btn.ant-btn-default.ant-btn-block.login-btn___AGd4D')
            await login_btn.wait_for(state="visible", timeout=10000)
            await login_btn.click()

            # 处理可能出现的确认对话框
            try:
                confirm_button = self.page.get_by_role("button", name="确 定")
                await confirm_button.click(timeout=3000)
            except:
                pass

            # 等待登录成功（首页菜单出现）
            order_menu = self.page.locator(".menuText___HLBVY").first
            await order_menu.wait_for(state="visible", timeout=15000)

            self.logger.info("✅ JST-ERP 手动登录成功")

            # 登录成功后保存cookies
            await self.save_cookies()

            return True

        except PlaywrightTimeoutError as e:
            self.logger.error(f"登录超时：{e}")
            raise
        except Exception as e:
            self.logger.exception(f"登录异常：{e}")
            raise

    async def save_cookies(self):
        """保存cookies到文件"""
        try:
            cookies = await self.context.cookies()

            # 转换为字典格式
            cookie_dict = {
                c["name"]: c["value"]
                for c in cookies
            }

            with open(self.cookie_file, "w", encoding="utf-8") as f:
                json.dump(cookie_dict, f, ensure_ascii=False, indent=2)

            self.logger.info(f"✅ Cookie 已保存到 {self.cookie_file}")
            return True
        except Exception as e:
            self.logger.error(f'保存cookie失败: {e}')
            return False

    async def ensure_login(self) -> bool:
        """确保登录状态，如果已登录则直接返回，否则执行登录"""
        try:
            # 如果页面不存在，重新初始化
            if not self.page:
                if not await self.init_browser():
                    return False

            # 检查当前是否已登录（通过访问一个需要登录的页面或检查元素）
            try:
                await self.page.goto(
                    "https://ww.erp321.com/login.aspx",
                    wait_until="domcontentloaded",
                    timeout=10000
                )

                # 检查是否已经登录（找首页元素）
                order_menu = self.page.locator(".menuText___HLBVY").first
                if await order_menu.is_visible(timeout=3000):
                    self.logger.info("✅ 当前已保持登录状态")
                    return True
                else:
                    # 未登录，执行登录流程
                    self.logger.info("会话已过期，重新登录")
                    return await self.login()

            except Exception as e:
                self.logger.error(f"检查登录状态失败: {e}")
                return await self.login()

        except Exception as e:
            self.logger.error(f"ensure_login失败: {e}")
            return False

    async def run_once(self):
        """单次运行 - 使用ensure_login替代直接login"""
        if not await self.init_browser():
            raise Exception("browser 失败")

        if not await self.ensure_login():
            raise Exception("login 失败")

        return True

    async def run(self, max_retry=3):
        """总流程"""
        for attempt in range(1, max_retry + 1):
            self.logger.info(f"JST---第 {attempt} 次登录尝试")

            try:
                result = await self.run_once()
                if result:
                    self.logger.info(f"JST - 登录成功（第 {attempt} 次）")
                    return True


            except Exception as e:

                self.logger.error(

                    f"JST - 第 {attempt} 次失败: {e}",

                    exc_info=True

                )

                await self.close()

            finally:
                # 注意：不要在这里close，因为持久化上下文需要保持
                # 只有在彻底失败时才关闭
                pass

            if attempt < max_retry:
                self.logger.info(f"JST - 准备重试，等待 3 秒...")
                await asyncio.sleep(3)
            else:
                # 最后一次尝试也失败了，关闭浏览器
                await self.close()

        self.logger.error(f"JST- 登录失败，已达到最大重试次数 {max_retry}")
        ding_bot_send('me', f"JST- 登录失败，已达到最大重试次数 {max_retry}")
        return False

    async def close(self):
        """关闭浏览器和playwright"""
        try:
            if self.context:
                await self.context.close()
            if self.playwright:
                await self.playwright.stop()
            self.logger.info("浏览器已关闭")
        except Exception as e:
            self.logger.error(f'浏览器关闭失败: {e}')


async def main():
    t = JSTLogin('login')
    try:
        await t.run()
        # 这里可以添加你的业务逻辑
        # await t.page.goto("你的业务页面")
        # 执行操作...

        # 保持浏览器运行一段时间（模拟实际使用）
        await asyncio.sleep(2)  # 1小时后自动关闭

    finally:
        await t.close()

#
# if __name__ == "__main__":
#     asyncio.run(main())