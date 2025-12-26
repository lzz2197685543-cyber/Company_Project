import sys
import requests
from playwright import sync_api
import json
import time


class BrowserAutomation:
    def __init__(self, container_code, name):
        """
        初始化浏览器自动化类
        Args:
            container_code (str): 环境ID
            name (str): 账户名称
            account_dict (dict): 账户信息字典
        """
        self.container_code = container_code
        self.api_url = "http://127.0.0.1:6873/api/v1/browser/start"
        self.debugging_port = None
        self.playwright = None
        self.browser_context = None
        self.browser = None
        self.name = name

    def start_browser(self):
        """启动浏览器环境"""
        open_data = {"containerCode": self.container_code}
        open_res = requests.post(self.api_url, json=open_data).json()

        if open_res['code'] != 0:
            print(f'环境打开失败: {open_res}')
            sys.exit()

        self.debugging_port = open_res['data']['debuggingPort']
        print(f"浏览器已启动，调试端口: {self.debugging_port}")
        return self.debugging_port

    def connect_browser(self):
        """连接到浏览器"""
        if not self.debugging_port:
            print("请先启动浏览器")
            return False

        try:
            self.playwright = sync_api.sync_playwright().start()
            self.browser = self.playwright.chromium.connect_over_cdp(
                f"http://127.0.0.1:{self.debugging_port}"
            )
            self.browser_context = self.browser.contexts[0]
            print("浏览器连接成功")
            return True
        except Exception as e:
            print(f"连接浏览器失败: {e}")
            return False

    def open_shopee(self):
        """打开sso.geiwohuo.com网站并获取Cookie"""
        if not self.browser_context:
            print("浏览器未连接")
            return False
        try:
            # 使用第一个标签页
            page = self.browser_context.pages[0]
            page.goto("https://seller.scs.shopee.cn/login")

            # 点击登录
            page.locator('//*[@id="app"]/div/div[2]/div[1]/form/div[4]/div/button').click()


            # 等待页面加载
            page.wait_for_timeout(3000)
            print(f"页面标题: {page.title()}")

            # 获取当前页面的所有Cookie
            cookies = self.browser_context.cookies()

            print(f"获取到的Cookie数量: {len(cookies)}")
            cookie_dict = {}
            for cookie in cookies:
                cookie_dict[cookie['name']] = cookie['value']

            with open(f'./data/{self.name}_cookies.json', 'w', encoding='utf-8') as f:
                f.write(json.dumps(cookie_dict))
            print('cookie已经保存成功')

            return True

        except Exception as e:
            print(f"执行操作失败: {e}")
            return False


    def close(self):
        """关闭连接"""
        try:
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            print("浏览器连接已关闭")
        except Exception as e:
            print(f"关闭连接时出错: {e}")

    def run(self):
        """执行完整流程"""
        try:
            print(f"开始执行 {self.name} 的浏览器自动化流程...")

            # 启动浏览器
            self.start_browser()
            time.sleep(2)  # 等待浏览器启动

            # 连接浏览器
            if not self.connect_browser():
                return False
            time.sleep(1)

            # 执行shein网站操作
            if self.open_shopee():
                # 可以在这里添加更多操作，比如登录、搜索等
                print(f"{self.name} 流程执行完成")

            # 关闭浏览器
            self.close()
            return True

        except Exception as e:
            print(f"执行流程时出错: {e}")
            return False


# 使用示例
if __name__ == '__main__':
    # 读取账户信息
    with open('./data/shopee_accounts.json', 'r', encoding='utf-8') as f:
        account = json.loads(f.read())

    name_list = ["虾皮全托1501店", "虾皮全托507-lxz","虾皮全托506-kedi", "虾皮全托505-qipei","虾皮全托504-huanchuang","虾皮全托503-juyule","虾皮全托502-xiyue","虾皮全托501-quzhi"]
    # name_list=["虾皮全托506-kedi",'虾皮全托503-juyule','虾皮全托502-xiyue']


    for name in name_list:
        print(f"\n{'=' * 50}")
        print(f"开始处理账户: {name}")
        print(f"{'=' * 50}")

        if name in account:
            id = account[name]['hubId']

            # 创建自动化实例，传入账户字典以便更新Cookie
            automation = BrowserAutomation(id, name)

            # 执行完整流程
            success = automation.run()

            if success:
                print(f"{name} 处理成功")
            else:
                print(f"{name} 处理失败")

            # 每个账户处理完后等待几秒
            time.sleep(3)
        else:
            print(f"账户 {name} 不存在于数据文件中")

# https://seller.scs.shopee.cn/login