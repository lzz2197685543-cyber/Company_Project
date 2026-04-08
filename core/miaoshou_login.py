import asyncio
import json
import ddddocr
from pathlib import Path
from datetime import datetime
from core.browser import BrowserManager
from utils.logger import get_logger
from utils.config_loader import get_shop_config

IMG_DIR = Path(__file__).resolve().parent.parent / "data"
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

    async def captcha(self):
        # 直接截取验证码元素
        captcha_element = self.page.locator('#J_loginBox > div.recovery-form-item.J_imgCaptcha').locator('img').first
        await captcha_element.screenshot(path=f'{IMG_DIR}/captcha.png')

        # 初始化OCR对象
        ocr = ddddocr.DdddOcr()

        # 读取图片
        with open(f'{IMG_DIR}/captcha.png', "rb") as f:
            image = f.read()

        # 识别图片
        result = ocr.classification(image)
        print(result)  # 输出识别结果

        await asyncio.sleep(1)

        # 输入验证码
        captcha_input = self.page.locator(
            '#J_loginBox > div.recovery-form-item.J_imgCaptcha > input.captcha-text.J_inputField')
        await captcha_input.click()
        await captcha_input.fill(result)

    async def login(self):
        """主登录流程"""
        try:
            # 访问网站
            await self.page.goto("https://erp.91miaoshou.com/?redirect=%2Fwelcome")
            await self.page.wait_for_load_state("domcontentloaded")

            # ==============输入账号密码=============
            await self.page.fill('input[placeholder*="手机号/子账号/邮箱"]', self.phone)
            await self.page.fill('input[placeholder*="密码"]', self.password)

            # ==============验证码的处理（循环重试）==============
            max_retries = 6  # 最大重试次数
            retry_count = 0
            login_success = False

            while retry_count < max_retries:
                # 输入验证码
                await self.captcha()

                # 点击登录按钮
                await self.page.click('#J_loginBtn')

                # 等待页面响应，让错误提示出现
                await self.page.wait_for_timeout(2000)

                # 检查是否有错误提示
                error_element = self.page.locator('.error-msg').first
                if await error_element.count() > 0:
                    error_text = await error_element.text_content()
                    print(f"错误提示: {error_text}")

                    if "图形验证码不正确" in error_text:
                        retry_count += 1
                        print(f"验证码错误，第{retry_count}次重试...")

                        # 刷新验证码（如果需要的话，可以点击验证码图片刷新）
                        captcha_img = self.page.locator('#J_loginBox > div.recovery-form-item.J_imgCaptcha').locator(
                            'img').first
                        if await captcha_img.count() > 0:
                            await captcha_img.click()  # 点击刷新验证码

                        await self.page.wait_for_timeout(1000)
                        continue
                    else:
                        # 其他错误提示
                        print(f"登录失败: {error_text}")
                        break
                else:
                    # 没有错误提示，可能登录成功
                    # 检查是否跳转到目标页面
                    current_url = self.page.url
                    if "welcome" in current_url or "dashboard" in current_url:
                        print("登录成功！")
                        login_success = True
                        break
                    else:
                        print("等待页面跳转...")
                        await self.page.wait_for_timeout(2000)

                        # 再次检查是否登录成功
                        current_url = self.page.url
                        if "welcome" in current_url or "dashboard" in current_url:
                            print("登录成功！")
                            login_success = True
                            break
                        else:
                            print("登录状态未知，继续等待...")
                            break

            if not login_success and retry_count >= max_retries:
                print("验证码重试次数已达上限，请检查")
            elif login_success:
                print("验证码验证成功，已登录")

            # ============== 保存 Cookie ==============
            await self._save_cookies()

            return True


        except Exception as e:
            self.logger.error(f"登录过程异常: {e}")
            import traceback
            traceback.print_exc()
            raise

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