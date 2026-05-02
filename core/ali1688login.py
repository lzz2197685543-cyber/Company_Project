# core/ali1688login.py
import json
from pathlib import Path
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.common import Actions
from utils.dingtalk_bot import ding_bot_send
from utils.config_loader import get_shop_config
from utils.logger import get_logger

COOKIE_DIR = Path(__file__).resolve().parent.parent / "data" / "cookies"
COOKIE_DIR.mkdir(parents=True, exist_ok=True)

# 用户数据目录（保存登录状态，比cookie更持久）
USER_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "browser_profile"
USER_DATA_DIR.mkdir(parents=True, exist_ok=True)


class Ali1688Login:
    def __init__(self, headless=False, use_user_data=True, job=None, account_name="ali1688"):
        """
        初始化登录器
        :param headless: 是否无头模式
        :param use_user_data: 是否使用用户数据目录
        :param job: 任务名称
        :param account_name: 配置中的账号名称，如 "ali1688" 或 "ali16880"
        """
        self.account_name = account_name
        # 从配置中加载指定账号的配置
        cfg = get_shop_config(account_name)
        self.phone = cfg.get('account')
        self.password = cfg.get('password')

        if not self.phone or not self.password:
            raise ValueError(f"账号 {account_name} 的配置不完整")

        self.headless = headless
        self.use_user_data = use_user_data
        self.page = None
        self.actions = None
        self.cookie_file = COOKIE_DIR / f"ali1688_cookie_{self.account_name}.json"
        self.logger = get_logger(job)

        # 为不同账号使用独立的用户数据目录
        if use_user_data:
            self.user_data_dir = USER_DATA_DIR / self.account_name
            self.user_data_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.user_data_dir = None

        self.logger.info(f"初始化登录器: 账号={self.account_name}, 手机号={self.phone}")

    def init_browser(self):
        """初始化浏览器（DrissionPage版本）"""
        try:
            co = ChromiumOptions()

            # 使用独立的数据目录
            if self.use_user_data and self.user_data_dir:
                co.set_user_data_path(str(self.user_data_dir))

            # 设置无头模式
            if self.headless:
                co.headless()

            # 设置User-Agent（可选）
            co.set_user_agent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            # 添加反检测参数
            co.set_argument('--disable-blink-features=AutomationControlled')
            co.set_argument('--disable-features=IsolateOrigins,site-per-process')
            co.set_argument('--disable-web-security')

            # 初始化页面
            self.page = ChromiumPage(addr_or_opts=co)
            self.actions = Actions(self.page)

            print(f"浏览器初始化成功（账号: {self.account_name}）")
            return True
        except Exception as e:
            print(f"浏览器初始化失败: {e}")
            raise

    def close(self):
        """关闭浏览器"""
        if self.page:
            self.page.quit()
            print(f"浏览器已关闭（账号: {self.account_name}）")

    def save_cookie_as_dict(self):
        """将当前浏览器 cookie 保存为 dict 到 json 文件"""
        self.page.get('https://detail.1688.com/offer/990163610799.html')
        ding_bot_send('me', f'账号 {self.account_name} 需要过验证码')
        input('请回车')

        cookies = self.page.cookies()

        cookie_dict = {
            c['name']: c['value']
            for c in cookies
        }

        with open(self.cookie_file, 'w', encoding='utf-8') as f:
            json.dump(cookie_dict, f, ensure_ascii=False, indent=2)

        print(f'✅ Cookie 已保存为 dict：{self.cookie_file}')

    def check_login_status(self):
        """检查是否已登录"""
        try:
            # 访问1688主页
            self.page.get('https://login.1688.com/')

            # 1688工作台页“交易”
            user_elem = self.page.ele('xpath://*[@id="topbar-box"]/div[3]/ul/li[2]/a', timeout=6)

            if user_elem:
                print(f"✅ 检测到已登录状态（账号: {self.account_name}）")
                self.save_cookie_as_dict()
                return True
            else:
                print(f"❌ 未检测到登录状态（账号: {self.account_name}）")
                return False

        except Exception as e:
            print(f"检查登录状态失败: {e}")
            return False

    def login(self):
        """执行登录操作"""
        try:
            print(f"开始执行登录流程（账号: {self.account_name}）...")

            # 访问登录页面
            self.page.get('https://login.1688.com/')
            self.page.wait(3)

            # 切换到密码登录模式（如果需要）
            try:
                pwd_login_tab = self.page.ele('text=密码登录')
                if pwd_login_tab:
                    pwd_login_tab.click()
                    self.page.wait(1)
            except:
                pass

            # 输入账号
            username_input = self.page.ele('#fm-login-id')
            if username_input:
                username_input.clear()
                username_input.input(self.phone)
                print("已输入账号")
            else:
                print("未找到账号输入框")
                return False

            # 输入密码
            password_input = self.page.ele('#fm-login-password')
            if password_input:
                password_input.clear()
                password_input.input(self.password)
                print("已输入密码")

            # 点击登录按钮
            login_btn = self.page.ele('xpath://*[@id="login-form"]/div[6]/button')
            if login_btn:
                login_btn.click()
                print("已点击登录按钮")

            ding_bot_send('me', f'1688账号 {self.account_name} 需要登录')
            input('登录之后请回车:')

            # 验证登录结果
            if self.check_login_status():
                print(f"✅ 登录成功（账号: {self.account_name}）！")
                return True
            else:
                print(f"❌ 登录失败（账号: {self.account_name}），可能需要手动处理验证码")
                return False

        except Exception as e:
            print(f"登录过程出错: {e}")
            return False

    def ensure_login(self):
        """确保已登录，如果未登录则执行登录"""
        if not self.page:
            self.init_browser()

        if not self.check_login_status():
            print(f"未检测到登录状态（账号: {self.account_name}），开始登录...")
            return self.login()
        else:
            print(f"已处于登录状态（账号: {self.account_name}）")
            return True


def main():
    """测试主函数"""
    # 测试 ali1688 账号
    login = Ali1688Login(
        headless=False,
        use_user_data=True,
        account_name="ali1688"
    )

    try:
        login.init_browser()
        if login.ensure_login():
            print("登录流程完成")
            input("按回车键关闭浏览器...")
    finally:
        login.close()


if __name__ == '__main__':
    main()