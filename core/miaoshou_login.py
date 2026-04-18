import asyncio
import json
import ddddocr
from pathlib import Path
from datetime import datetime
from core.browser import BrowserManager
from utils.logger import get_logger
from utils.config_loader import get_shop_config
from core.base_client import BaseClient

IMG_DIR = Path(__file__).resolve().parent.parent / "data" / "img"
COOKIE_DIR=Path(__file__).resolve().parent.parent / "data" /"cookies"

# 确保目录存在
IMG_DIR.mkdir(parents=True, exist_ok=True)
COOKIE_DIR.mkdir(parents=True, exist_ok=True)


class MiaoShouLogin:
    def __init__(self, page=None,job=None):
        cfg = get_shop_config("miaoshou")
        self.phone = cfg['account']
        self.password = cfg['password']
        self.logger = get_logger(job)
        self.page = page
        self.base_client=BaseClient(job)

    async def captcha(self):
        captcha_img = self.page.locator(
            '#J_loginBox > div.recovery-form-item.J_imgCaptcha img'
        ).first
        captcha_input = self.page.locator(
            '#J_loginBox > div.recovery-form-item.J_imgCaptcha > input.captcha-text.J_inputField'
        )

        last_err = None

        for attempt in range(3):
            try:
                await captcha_img.wait_for(state="visible", timeout=10000)
                await captcha_img.scroll_into_view_if_needed()
                await self.page.wait_for_timeout(800)

                img_path = IMG_DIR / "captcha.png"
                await captcha_img.screenshot(path=str(img_path), timeout=10000)

                if not img_path.exists() or img_path.stat().st_size == 0:
                    raise ValueError("验证码图片截图为空")

                ocr = ddddocr.DdddOcr()
                with open(img_path, "rb") as f:
                    image = f.read()

                result = ocr.classification(image)
                result = (result or "").strip().replace(" ", "")

                if not result:
                    raise ValueError("验证码识别结果为空")

                self.logger.info(f"验证码识别结果: {result}")

                await captcha_input.click()
                await captcha_input.fill(result)
                return True

            except Exception as e:
                last_err = e
                self.logger.warning(f"验证码识别第 {attempt + 1}/3 次失败: {e}")

                try:
                    await captcha_img.click()
                except Exception:
                    pass

                await self.page.wait_for_timeout(1000)

        raise Exception(f"验证码识别失败: {last_err}")

    async def login(self, max_retries=3):
        """主登录流程：带重试，失败返回 False，不直接抛出中断"""
        login_url = "https://erp.91miaoshou.com/?redirect=%2Fwelcome"

        for attempt in range(max_retries):
            try:
                self.logger.info(f"妙手登录开始，第 {attempt + 1}/{max_retries} 次尝试")

                # 访问登录页：降低因 load 超时导致的失败
                await self.page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
                await self.page.wait_for_load_state("domcontentloaded")

                # 输入账号密码
                await self.page.fill('input[placeholder*="手机号/子账号/邮箱"]', self.phone)
                await self.page.fill('input[placeholder*="密码"]', self.password)

                max_captcha_retries = 6
                login_success = False

                for captcha_try in range(max_captcha_retries):
                    # 识别并输入验证码
                    await self.captcha()

                    # 点击登录按钮
                    await self.page.click('#J_loginBtn')
                    await self.page.wait_for_timeout(2000)

                    # 检查错误提示
                    error_element = self.page.locator('.error-msg').first
                    if await error_element.count() > 0:
                        error_text = await error_element.text_content() or ""
                        self.logger.warning(f"登录错误提示: {error_text}")

                        if "图形验证码不正确" in error_text:
                            self.logger.info(f"验证码错误，第 {captcha_try + 1}/{max_captcha_retries} 次重试")
                            captcha_img = self.page.locator(
                                '#J_loginBox > div.recovery-form-item.J_imgCaptcha'
                            ).locator('img').first
                            if await captcha_img.count() > 0:
                                await captcha_img.click()
                            await self.page.wait_for_timeout(1000)
                            continue

                        self.logger.error(f"登录失败: {error_text}")
                        break

                    # 没有错误提示，检查是否已登录
                    current_url = self.page.url
                    if "welcome" in current_url or "dashboard" in current_url:
                        login_success = True
                        self.logger.info("妙手登录成功")
                        break

                    await self.page.wait_for_timeout(2000)
                    current_url = self.page.url
                    if "welcome" in current_url or "dashboard" in current_url:
                        login_success = True
                        self.logger.info("妙手登录成功")
                        break

                if login_success:
                    await self._save_cookies()
                    await self.autocliam()
                    return True

                self.logger.warning(f"妙手登录未成功，第 {attempt + 1}/{max_retries} 次尝试结束")

            except Exception as e:
                self.logger.error(f"妙手登录第 {attempt + 1}/{max_retries} 次异常: {e}")
                try:
                    await self.page.wait_for_timeout(1000)
                    await self.page.reload(wait_until="domcontentloaded", timeout=30000)
                except Exception:
                    pass

        self.logger.error("妙手登录失败，已达到最大重试次数")
        return False

    async def autocliam(self):
        data = {
            'claimedPlatforms[0]': 'pddkj',
            'isAutoClaimed': '1',
        }
        await self.base_client.post('https://erp.91miaoshou.com/api/move/common_collect_box/saveClaimedPlatforms',
            payload=data)

        print("自动认领开启",data)

    async def _save_cookies(self):
        """保存当前页面的 Cookie 到文件"""
        # 获取所有 Cookie
        cookies = await self.page.context.cookies()
        # 转换为 name: value 字典
        cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

        cookie_data = {
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cookies": cookies_dict,
        }

        # 保存到 JSON 文件
        cookie_file = COOKIE_DIR / "miaoshou_cookies.json"
        with open(cookie_file, 'w', encoding='utf-8') as f:
            json.dump(cookie_data, f, ensure_ascii=False, indent=2)

        print(f"Cookie 已保存至 {cookie_file}")


async def main():
    """主函数 - 使用方式1：手动管理浏览器"""
    # 创建浏览器管理器
    browser_manager = BrowserManager(headless=False)

    try:
        # 启动浏览器
        page = await browser_manager.start(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1366, "height": 768}
        )

        # 创建登录实例
        client = MiaoShouLogin(page)
        await client.login()

        # 登录成功后可以保持浏览器打开
        print("登录完成，浏览器将保持打开状态...")

    finally:
        # 关闭浏览器
        await browser_manager.close()

#
# if __name__ == "__main__":
#     asyncio.run(main())