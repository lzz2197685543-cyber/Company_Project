import random
from fake_useragent import UserAgent
import requests
from typing import Dict, Optional, List


class HeaderManager:
    """请求头管理器"""

    def __init__(self, use_fake_ua: bool = True):
        self.use_fake_ua = use_fake_ua
        # self.logger = get_logger('amazon_goods_monitor')
        if use_fake_ua:
            try:
                self.ua = UserAgent()
            except:
                print("fake-useragent初始化失败，使用内置UA池")
                self.use_fake_ua = False

        # 内置的常用User-Agent池
        self.builtin_uas = [
            # Chrome Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',

            # Chrome macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',

            # Firefox Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0',

            # Firefox macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/120.0',

            # Safari macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',

            # Edge Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        ]

        # 常见的Accept值
        self.accept_values = [
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        ]

        # 常见的Accept-Language
        self.accept_languages = [
            # 'zh-CN,zh;q=0.9,en;q=0.8',
            # 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'en-US,en;q=0.9',
            'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            # 'zh-CN,zh;q=0.9',
            'en-GB,en-US;q=0.9,en;q=0.8',
        ]

        # 常见的Accept-Encoding
        self.accept_encodings = [
            'gzip, deflate, br',
            'gzip, deflate',
            'gzip, deflate, sdch',
        ]

        # 常见的Connection值
        self.connection_values = [
            'keep-alive',
            'close',
        ]

        # 常见的Sec-Ch-UA值
        self.sec_ch_uas = [
            '"Google Chrome";v="120", "Chromium";v="120", "Not?A_Brand";v="99"',
            '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="99"',
            '"Microsoft Edge";v="120", "Chromium";v="120", "Not?A_Brand";v="99"',
            '"Firefox";v="121", "Gecko";v="121", "Not?A_Brand";v="99"',
        ]

        # 常见的平台
        self.sec_ch_ua_platforms = [
            '"Windows"',
            '"macOS"',
            '"Linux"',
            '"Android"',
            '"iOS"',
        ]

    def get_random_user_agent(self) -> str:
        """获取随机User-Agent"""
        if self.use_fake_ua:
            try:
                return self.ua.random
            except:
                return random.choice(self.builtin_uas)
        else:
            return random.choice(self.builtin_uas)

    def generate_headers(self,
                         referer: Optional[str] = None,
                         add_common_headers: bool = True,
                         mobile: bool = False,
                         custom_headers: Optional[Dict] = None) -> Dict[str, str]:
        """
        生成随机请求头

        Args:
            referer: 来源页面
            add_common_headers: 是否添加常见请求头
            mobile: 是否模拟移动设备
            custom_headers: 自定义请求头

        Returns:
            请求头字典
        """
        headers = {}

        # 基础请求头
        headers['User-Agent'] = self.get_random_user_agent()
        headers['Accept'] = random.choice(self.accept_values)
        headers['Accept-Language'] = random.choice(self.accept_languages)
        headers['Accept-Encoding'] = random.choice(self.accept_encodings)
        headers['Connection'] = random.choice(self.connection_values)

        # 添加缓存控制
        if random.random() > 0.5:
            headers['Cache-Control'] = 'max-age=0'

        # 添加来源
        if referer:
            headers['Referer'] = referer
        else:
            # 随机生成常见来源
            if random.random() > 0.7:
                headers['Referer'] = 'https://www.google.com/'
            elif random.random() > 0.5:
                headers['Referer'] = 'https://www.amazon.com/'

        # 添加安全相关的请求头
        if add_common_headers:
            if 'Chrome' in headers['User-Agent'] or 'Edg' in headers['User-Agent']:
                headers['Sec-Ch-Ua'] = random.choice(self.sec_ch_uas)
                headers['Sec-Ch-Ua-Mobile'] = '?1' if mobile else '?0'
                headers['Sec-Ch-Ua-Platform'] = random.choice(self.sec_ch_ua_platforms)
                headers['Sec-Fetch-Dest'] = 'document'
                headers['Sec-Fetch-Mode'] = 'navigate'
                headers['Sec-Fetch-Site'] = random.choice(['none', 'same-origin', 'same-site', 'cross-site'])
                headers['Sec-Fetch-User'] = '?1'
                headers['Upgrade-Insecure-Requests'] = '1'

        # 添加DNT（Do Not Track）
        if random.random() > 0.8:
            headers['DNT'] = '1'

        # 添加自定义请求头
        if custom_headers:
            headers.update(custom_headers)

        return headers
    def generate_amazon_headers(self) -> Dict[str, str]:
        """生成针对亚马逊的请求头"""
        headers = self.generate_headers(referer='https://www.amazon.com/')

        # 亚马逊特定的请求头
        amazon_specific = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Upgrade-Insecure-Requests': '1',
            'TE': 'Trailers',
        }

        headers.update(amazon_specific)

        # 随机添加亚马逊Cookie相关头
        if random.random() > 0.5:
            headers['X-Requested-With'] = 'XMLHttpRequest'

        return headers

    def rotate_headers(self, session: requests.Session) -> requests.Session:
        """
        为session轮换请求头

        Args:
            session: requests Session对象

        Returns:
            更新后的session
        """
        session.headers.update(self.generate_headers())
        return session


def main():
    # 创建请求头管理器
    header_manager = HeaderManager()

    # 生成亚马逊特定请求头
    amazon_headers = header_manager.generate_amazon_headers()
    print("亚马逊特定请求头:")
    for key, value in amazon_headers.items():
        print(f"  {key}: {value}")

    print("\n" + "=" * 50 + "\n")


if __name__ == '__main__':
    main()