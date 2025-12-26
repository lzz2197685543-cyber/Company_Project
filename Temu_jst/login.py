from DrissionPage import ChromiumPage
import json
import time
import os


class JSTlogin:
    def __init__(self, username, password):
        self.page = ChromiumPage()
        self.username = username
        self.password = password
        self.cookie_file = './data/cookies.json'

    def save_cookie_as_dict(self):
        """
        将当前浏览器 cookie 保存为 dict 到 json 文件
        """
        cookies = self.page.cookies()

        cookie_dict = {
            c['name']: c['value']
            for c in cookies
        }

        os.makedirs(os.path.dirname(self.cookie_file), exist_ok=True)

        with open(self.cookie_file, 'w', encoding='utf-8') as f:
            json.dump(cookie_dict, f, ensure_ascii=False, indent=2)

        print(f'✅ Cookie 已保存为 dict：{self.cookie_file}')

    def login(self):
        # 已登录状态直接访问
        self.page.get('https://ww.erp321.com/epaas')

        """登录需要验证码"""
        # self.page.get(' https://www.erp321.com/login.aspx')
        # self.page.ele((By.ID,'login_id')).input(self.username)
        # self.page.ele((By.ID,'password')).input(self.password)
        # # 点击勾选
        # self.page.ele('xpath://*[@id="root"]/div/div[2]/div/div[2]/div/div/div[1]/div[2]/label/span[1]/input').click()
        # # 点击登录
        # self.page.ele('xpath://*[@id="root"]/div/div[2]/div/div[2]/div/div/div[1]/button/span').click()
        # time.sleep(70)

        # 等页面稳定（确保 cookie 已写入）
        time.sleep(3)

        # 保存 cookie
        self.save_cookie_as_dict()

    def main(self):
        self.login()

def refresh():
    jst = JSTlogin(username='18165643805', password='Aa14789+')
    jst.main()

# if __name__ == '__main__':
#     refresh()


