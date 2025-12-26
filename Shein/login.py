import json
import asyncio
import requests
from playwright.async_api import async_playwright
import os
import time


class TikTokLogin:
    def __init__(self, name, account_data):
        self.name = name
        self.hub_id = str(account_data['hubId'])
        self.username = account_data['credentials']['username']
        self.password = account_data['credentials']['password']
        self.proxy_config = account_data.get('proxy', {})

        self.api_url = "http://127.0.0.1:6873/api/v1/browser/start"
        self.stop_api_url = "http://127.0.0.1:6873/api/v1/browser/stop"  # 新增：停止API
        self.playwright = None
        self.browser = None
        self.page = None
        self.retry_count = 0
        self.max_retries = 2

    async def start_browser(self):
        """启动浏览器"""
        data = {"containerCode": self.hub_id}
        try:
            res = requests.post(self.api_url, json=data, timeout=10).json()

            if res['code'] != 0:
                error_msg = res.get('msg', '未知错误')
                print(f"{self.name} - 启动失败: {error_msg}")

                # 如果是资源不足或环境被占用，增加重试延迟
                if "系统资源不足" in error_msg or "被使用" in error_msg:
                    if self.retry_count < self.max_retries:
                        print(f"{self.name} - 等待10秒后重试 ({self.retry_count + 1}/{self.max_retries})")
                        await asyncio.sleep(10)
                        self.retry_count += 1
                        return await self.start_browser()

                return False

            self.debug_port = res['data']['debuggingPort']
            print(f"{self.name} - 启动成功，端口: {self.debug_port}")
            return True

        except Exception as e:
            print(f"{self.name} - API请求失败: {e}")
            return False

    async def stop_browser(self):
        """停止浏览器实例"""
        try:
            data = {"containerCode": self.hub_id}
            response = requests.post(self.stop_api_url, json=data, timeout=10)
            if response.status_code == 200:
                print(f"{self.name} - 浏览器实例已停止")
                return True
            else:
                print(f"{self.name} - 停止浏览器失败: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"{self.name} - 停止浏览器API请求失败: {e}")
            return False

    async def connect(self):
        """连接浏览器"""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.connect_over_cdp(
                f"http://127.0.0.1:{self.debug_port}",
                timeout=15000
            )

            # 获取页面
            contexts = self.browser.contexts
            if contexts and contexts[0].pages:
                self.page = contexts[0].pages[0]
            else:
                self.page = await self.browser.new_page()

            print(f"{self.name} - 连接成功")
            return True

        except Exception as e:
            print(f"{self.name} - 连接失败: {e}")
            return False

    async def check_network(self):
        """检查网络，增加重试"""
        for attempt in range(3):
            try:
                await self.page.goto(
                    "https://sso.geiwohuo.com/#/idms/stockup?auth_login_token=12d1e16f81bc459484190fe71ba6ec05",
                    timeout=15000,
                    wait_until="domcontentloaded"
                )

                # 检查页面是否正常
                await self.page.wait_for_selector("body", timeout=5000)

                if "chrome-error://" not in self.page.url:
                    print(f"{self.name} - 网络正常")
                    return True

            except Exception as e:
                if attempt < 2:  # 如果不是最后一次尝试
                    print(f"{self.name} - 网络检查失败，重试 {attempt + 1}/3: {e}")
                    await asyncio.sleep(3)
                else:
                    print(f"{self.name} - 网络检查最终失败: {e}")

        return False

    async def login(self):
        """登录TikTok"""
        # 检查是否已登录
        for i in range(3):
            if "idms/stockup" in self.page.url:
                print(f"{self.name} - 已登录")
                return True
            await asyncio.sleep(3)

        # 执行登录
        try:
            print(f"{self.name} - 开始登录...")

            # 输入用户名 - 增加多种选择器
            selectors = [
                ".csrmlzp.soui-input-input",
                "input[name='username']",
                "input[type='text']",
                "input[placeholder*='手机']",
                "input[placeholder*='账号']"
            ]

            user_input = None
            for selector in selectors:
                try:
                    user_input = await self.page.wait_for_selector(selector, timeout=3000)
                    if user_input:
                        break
                except:
                    continue

            if not user_input:
                print(f"{self.name} - 找不到用户名输入框")
                return False

            await user_input.fill(self.username)
            print(f"{self.name} - 已输入用户名")

            # 输入密码
            pwd_selectors = [
                "input[type='password']",
                "input[name='password']",
                "input[placeholder*='密码']"
            ]

            pwd_input = None
            for selector in pwd_selectors:
                try:
                    pwd_input = await self.page.wait_for_selector(selector, timeout=3000)
                    if pwd_input:
                        break
                except:
                    continue

            if not pwd_input:
                print(f"{self.name} - 找不到密码输入框")
                return False

            await pwd_input.fill(self.password)
            print(f"{self.name} - 已输入密码")

            # 点击登录
            btn_selectors = [
                "button[type='submit']",
                "button:has-text('登录')",
                "button:has-text('Sign in')"
            ]

            login_btn = None
            for selector in btn_selectors:
                try:
                    login_btn = await self.page.wait_for_selector(selector, timeout=3000)
                    if login_btn:
                        break
                except:
                    continue

            if not login_btn:
                print(f"{self.name} - 找不到登录按钮")
                return False

            await login_btn.click()
            print(f"{self.name} - 已点击登录")

            # 等待登录完成
            for i in range(15):
                current_url = self.page.url
                if "homepage" in current_url:
                    print(f"{self.name} - 登录成功")
                    return True

                # 检查是否有错误提示
                try:
                    error_selectors = [".error", ".ant-message-error", ".error-message", ".alert"]
                    for selector in error_selectors:
                        elements = await self.page.query_selector_all(selector)
                        for element in elements:
                            text = await element.text_content()
                            if text and len(text.strip()) > 10:
                                print(f"{self.name} - 登录错误: {text[:50]}...")
                                return False
                except:
                    pass

                if i % 3 == 0:  # 每3次打印一次状态
                    print(f"{self.name} - 等待登录 {i + 1}/15")
                await asyncio.sleep(2)

        except Exception as e:
            print(f"{self.name} - 登录错误: {e}")

        print(f"{self.name} - 登录失败")
        return False

    async def get_cookies(self):
        """获取并保存Cookie"""
        try:
            await asyncio.sleep(5)  # 等待页面完全加载
            cookies = await self.page.context.cookies()

            if not cookies:
                print(f"{self.name} - 没有获取到Cookie")
                return False

            cookie_dict = {c['name']: c['value'] for c in cookies}

            # 确保data目录存在
            os.makedirs('./data', exist_ok=True)

            # 保存到文件
            save_data = {
                "name": self.name,
                "hub_id": self.hub_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "cookies": cookie_dict,
                "cookie_count": len(cookies)
            }

            with open(f'./data/{self.name}_cookies.json', 'w', encoding='utf-8') as f:
                json.dump(save_data, f, indent=2, ensure_ascii=False)

            print(f"{self.name} - Cookie已保存 ({len(cookies)}个)")
            return True

        except Exception as e:
            print(f"{self.name} - 获取Cookie失败: {e}")
            return False

    async def run(self):
        """执行完整流程"""
        print(f"\n{'=' * 50}")
        print(f"开始处理: {self.name}")
        print(f"{'=' * 50}")

        try:
            # 启动浏览器
            if not await self.start_browser():
                return False
            await asyncio.sleep(2)

            # 连接浏览器
            if not await self.connect():
                return False

            # 检查网络
            if not await self.check_network():
                print(f"{self.name} - 网络异常，跳过")
                await self.close()
                return False

            # 登录
            if not await self.login():
                print(f"{self.name} - 登录失败")
                await self.close()
                return False

            # 获取Cookie
            if not await self.get_cookies():
                print(f"{self.name} - 获取Cookie失败")
                await self.close()
                return False

            # 关闭
            await self.close()
            print(f"{self.name} - 处理成功 ✓")
            return True

        except Exception as e:
            print(f"{self.name} - 运行过程中出错: {e}")
            await self.close()
            return False

    async def close(self):
        """关闭浏览器连接并停止浏览器实例"""
        try:
            # 先关闭Playwright连接
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            print(f"{self.name} - 关闭Playwright连接时出错: {e}")
        finally:
            # 无论是否成功关闭连接，都尝试停止浏览器实例
            await self.stop_browser()


async def main():
    """主函数"""
    # 读取配置
    config_file = './data/shein_accounts.json'

    if not os.path.exists(config_file):
        print(f"配置文件不存在: {config_file}")
        return

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            accounts = json.load(f)
        print(f"读取到 {len(accounts)} 个账户")
    except Exception as e:
        print(f"读取配置文件失败: {e}")
        return

    # 分批处理账户（先处理一部分）
    all_accounts = list(accounts.items())

    # 可以分批处理，避免资源不足
    batch_size = 5  # 每批处理5个账户
    success_count = 0
    failed_accounts = []

    for i in range(0, len(all_accounts), batch_size):
        batch = all_accounts[i:i + batch_size]
        print(batch)
        print(f"\n{'=' * 60}")
        print(f"处理第 {i // batch_size + 1} 批，共 {len(batch)} 个账户")
        print(f"{'=' * 60}")

        for name, data in batch:
            print(f"{name}: {data}")
            login = TikTokLogin(name, data)
            success = await login.run()

            status = "✓" if success else "✗"
            print(f"{name}: {status}")

            if success:
                success_count += 1
            else:
                failed_accounts.append(name)

            # 账户间延时（失败账户延长等待时间）
            wait_time = 2 if success else 10
            if name != batch[-1][0]:
                print(f"等待{wait_time}秒后继续...")
                await asyncio.sleep(wait_time)

        # 批次间延时
        if i + batch_size < len(all_accounts):
            print(f"\n批次间等待3秒...")
            await asyncio.sleep(3)

    # 输出统计结果
    print(f"\n{'=' * 60}")
    print(f"处理完成！")
    print(f"成功: {success_count}/{len(all_accounts)}")
    if failed_accounts:
        print(f"失败账户: {', '.join(failed_accounts)}")
        print(f"\n可以单独重新运行失败的账户:")
        for account in failed_accounts:
            print(f"  账户: {account}")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    asyncio.run(main())