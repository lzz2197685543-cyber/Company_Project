from DrissionPage import ChromiumPage
from DrissionPage.common import By
import json
import time
import os


class SimpleLogin:
    def __init__(self, shop_name, account, password,channelId):
        self.shop_name = shop_name
        self.account = account
        self.password = password
        self.page = ChromiumPage()
        self.channelId=channelId
        # self.url = f'https://csp.aliexpress.com/m_apps/merchandise-csp/goodsManagement?channelId={channelId}'

        self.timeout = 10
        self.max_retries = 3
        self.cookies = None
        self.full_cookies_info = None

    def _get_cookies_dict(self):
        """获取cookies字典"""
        try:
            self.full_cookies_info = self.page.cookies()
            self.cookies = {cookie['name']: cookie['value'] for cookie in self.full_cookies_info}
            return self.cookies
        except Exception as e:
            print(f"获取cookie失败: {e}")
            return None

    def save_cookies(self, filename=None):
        """保存cookies到文件"""
        if not self.cookies:
            print("没有可保存的cookies")
            return False

        filename = filename or f"./data/{self.shop_name}_cookies.json"
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            save_data = {
                'shop_name': self.shop_name,
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'cookies_dict': self.cookies,
                'full_cookies_info': self.full_cookies_info
            }

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            print(f"Cookies已保存到: {filename}")
            return True
        except Exception as e:
            print(f"保存cookies失败: {e}")
            return False

    def _perform_login(self):
        """执行登录操作"""
        url=f'https://login.aliexpress.com/user/seller/login?bizSegment=CSP&return_url=http%3A%2F%2Fcsp.aliexpress.com%2Fm_apps%2Fnewhome%2Fpop%3FchannelId%3D{self.channelId}'
        self.page.get(url)

        if '商品管理' in self.page.html:
            return True
        else:

            # 输入账户信息
            try:
                # 等待输入框的出现
                self.page.wait.eles_loaded('#loginName', timeout=3)
                self.page.ele((By.ID, 'loginName')).input(self.account)
                time.sleep(1)

                self.page.ele((By.ID, 'password')).input(self.password)
                time.sleep(1)

                # 点击登录
                login_btn = '//*[@id="root"]/div/main/section[2]/section/section/section/form/button[1]'
                self.page.ele((By.XPATH, login_btn)).click()
                time.sleep(3)

                # 验证登录成功 - 等待商品元素出现
                self.page.wait.eles_loaded('#Root-Home-cspaeproducts', timeout=3)
                product_element = self.page.ele((By.XPATH, '//*[@id="Root-Home-cspaeproducts"]/div/span[2]/a'))
                if product_element and product_element.text == '商品':
                    print("✅ 登录成功验证通过")
                    return True
                else:
                    print("⚠️ 登录成功但验证元素未找到")
                    return True

            except Exception as e:
                print(f"登录过程中出错: {e}")
                # 检查是否有验证码或其他页面
                try:
                    # 截屏以便调试
                    self.page.screenshot(f"login_error_{int(time.time())}.png")
                    print("已保存错误截图")
                except:
                    pass
                return False

    def login(self):
        """登录主函数"""
        for retry in range(self.max_retries):
            try:
                print(f"登录尝试 {retry + 1}/{self.max_retries} - {self.shop_name}")

                if self._perform_login():
                    print("✅ 登录成功")
                    self._get_cookies_dict()
                    return True

            except Exception as e:
                print(f"登录失败: {e}")
                if retry < self.max_retries - 1:
                    print("3秒后重试...")
                    time.sleep(3)
                    self.page = ChromiumPage()  # 重新创建浏览器

        print("❌ 达到最大重试次数")
        return False

    def get_cookies_string(self):
        """获取cookies字符串"""
        return "; ".join([f"{k}={v}" for k, v in self.cookies.items()]) if self.cookies else ""

    def close(self):
        """关闭浏览器"""
        try:
            self.page.quit()
        except:
            pass


# if __name__ == '__main__':
#     # 加载账号
#     with open('data/smt_accounts.json', 'r', encoding='utf-8') as f:
#         accounts = json.load(f)
#
#     shop_name = "SMT014"
#
#     if shop_name not in accounts:
#         print(f"未找到门店: {shop_name}")
#         exit()
#
#     account_data = accounts[shop_name]
#     login_client = SimpleLogin(
#         shop_name=shop_name,
#         account=account_data['account'],
#         password=account_data['password'],
#         channelId =account_data['channelId']
#     )
#
#     try:
#         if login_client.login():
#             login_client.save_cookies()
#
#             cookies_str = login_client.get_cookies_string()
#             print("\n" + "=" * 80)
#             print("Cookie字符串:")
#             print("=" * 80)
#             print(cookies_str)
#             print(f"\n长度: {len(cookies_str)} 字符")
#
#             time.sleep(2)
#     finally:
#         login_client.close()
#         print("\n浏览器已关闭")